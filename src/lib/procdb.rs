use std::fmt::Debug;
use std::ops::DerefMut;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;

use duckdb::{params, Connection};
use psutil::process::os::linux::ProcessExt;
use psutil::process::os::unix::{Gids, Uids};
use psutil::process::{processes, ProcessError};
use psutil::{Bytes, Pid};
use timer::Guard;
use users::{Groups, Users, UsersCache};

macro_rules! ok_or_skip {
    ($res:expr) => {
        match $res {
            Ok(val) => val,
            Err(_) => continue,
        }
    };
}

type Callback = Arc<Mutex<dyn 'static + FnMut() + Send>>;

pub struct ProcDB {
    db: Arc<Mutex<Connection>>,
    users_cache: Arc<Mutex<UsersCache>>,

    did_start: bool,
    refresh_thread: Option<thread::JoinHandle<()>>,
    timer: Option<timer::Timer>,
    periodic_timer: Option<Guard>,
    on_refresh: Callback,
}

#[derive(Debug)]
pub enum ProcDBError {
    DuckDBError(duckdb::Error),
    PSUtilError(psutil::Error),
    ProcessError(ProcessError),
}

impl From<ProcessError> for ProcDBError {
    fn from(e: ProcessError) -> Self {
        ProcDBError::ProcessError(e)
    }
}

impl From<duckdb::Error> for ProcDBError {
    fn from(e: duckdb::Error) -> Self {
        ProcDBError::DuckDBError(e)
    }
}

impl ProcDB {
    pub(crate) fn new(on_refresh: Option<impl 'static + FnMut() + Send>) -> Self {
        //XXX///////////////////////////////////////////////////////////////////////////////////////////
        let db = Connection::open_in_memory().unwrap();
        // let db = Connection::open("procs.db").unwrap();

        db.pragma_update(None, "threads", &"4").unwrap();
        db.pragma_update(None, "memory_limit", &"128MB").unwrap();

        // language=sql
        db.execute_batch(
            "-- noinspection SqlResolveForFile @ object-type/UBIGINT
            CREATE TABLE IF NOT EXISTS procs (
                pid UBIGINT PRIMARY KEY,
                ppid UBIGINT,
                pgid UBIGINT,

                status TEXT NOT NULL,
                nice INT NOT NULL,
                priority INT NOT NULL,

                real_uid INT NOT NULL,
                real_username TEXT,
                eff_uid INT NOT NULL,
                eff_username TEXT,

                real_gid INT NOT NULL,
                real_groupname TEXT,
                eff_gid INT NOT NULL,
                eff_groupname TEXT,

                name TEXT NOT NULL,
                exe TEXT NOT NULL,
                cmdline TEXT,
                cwd TEXT NOT NULL,

                cpu_time_user_ms UBIGINT NOT NULL,
                cpu_time_system_ms UBIGINT NOT NULL,
                cpu_time_children_user_ms UBIGINT NOT NULL,
                cpu_time_children_system_ms UBIGINT NOT NULL,
                
                memory_rss UBIGINT NOT NULL,
                memory_vms UBIGINT NOT NULL
            );
            ",
        )
        .unwrap();

        let on_refresh_cb: Callback = match on_refresh {
            Some(cb) => Arc::new(Mutex::new(cb)),
            None => Arc::new(Mutex::new(|| {})),
        };

        let mut slf = Self {
            db: Arc::new(Mutex::new(db)),
            users_cache: Arc::new(Mutex::new(unsafe { UsersCache::with_all_users() })),
            on_refresh: on_refresh_cb,
            did_start: false,
            refresh_thread: None,
            timer: None,
            periodic_timer: None,
        };

        slf.start();

        slf
    }

    pub(crate) fn start(&mut self) {
        if self.did_start {
            return;
        }

        println!("Starting procdb");

        let (tx, rx) = channel();

        let timer = timer::Timer::new();
        let periodic_timer = timer.schedule_repeating(chrono::Duration::seconds(1), move || {
            tx.send(()).unwrap();
        });

        let db_mutex = Arc::clone(&self.db);
        let users_cache = Arc::clone(&self.users_cache);
        let on_refresh = Arc::clone(&self.on_refresh);
        let refresh_thread = thread::spawn(move || loop {
            match rx.recv() {
                Ok(_) => {
                    Self::_refresh_procs(
                        db_mutex.lock().unwrap().deref_mut(),
                        users_cache.lock().unwrap().deref_mut(),
                    )
                    .expect("Failed to refresh procs");

                    let mut cb = on_refresh.lock().unwrap();
                    (cb)();
                },
                Err(_) => break,
            };
        });

        self.refresh_thread = Some(refresh_thread);
        self.timer = Some(timer);
        self.periodic_timer = Some(periodic_timer);
        self.did_start = true;

        println!("Started procdb");
    }

    pub(crate) fn stop(&mut self) {
        if !self.did_start {
            return;
        }

        self.refresh_thread = None;
        self.timer = None;
        self.periodic_timer = None;
        self.did_start = false;

        self.db.lock().unwrap().execute_batch("CHECKPOINT").unwrap();

        println!("Stopped procdb");
    }

    pub(crate) fn cursor(&self) -> Result<Connection, ProcDBError> {
        self.db
            .lock()
            .unwrap()
            .try_clone()
            .map_err(|e| ProcDBError::DuckDBError(e))
    }

    pub(crate) fn refresh_procs(&self) -> Result<(), ProcDBError> {
        Self::_refresh_procs(
            self.db.lock().unwrap().deref_mut(),
            &*self.users_cache.lock().unwrap(),
        )
    }

    fn _refresh_procs(db: &mut Connection, users_cache: &UsersCache) -> Result<(), ProcDBError> {
        let procs = match processes() {
            Ok(procs) => procs,
            Err(e) => {
                eprintln!("Error refreshing processes");
                return Err(ProcDBError::PSUtilError(e));
            },
        };

        db.execute_batch("TRUNCATE TABLE procs;")
            .map_err(|e| ProcDBError::DuckDBError(e))?;

        {
            let mut appender = db
                .appender("procs")
                .map_err(|e| ProcDBError::DuckDBError(e))?;

            for res in procs {
                let proc = match res {
                    Ok(proc) => proc,
                    Err(_) => continue,
                };

                let _proc_stat = ok_or_skip!(proc.procfs_stat());
                let _proc_statm = ok_or_skip!(proc.procfs_statm());
                let _proc_status = ok_or_skip!(proc.procfs_status());

                let _uid = Uids::from(_proc_status.clone());
                let _gid = Gids::from(_proc_status);

                let _cpu_times = ok_or_skip!(proc.cpu_times());
                let _mem = ok_or_skip!(proc.memory_info());

                let _exe = proc.exe().ok();
                let _cwd = proc.cwd().ok();

                let params = params![
                    //
                    // Process IDs/owners
                    proc.pid(),
                    _proc_stat.ppid,
                    _proc_stat.pgrp,
                    //
                    // Status/nice
                    ok_or_skip!(proc.status()).to_string(),
                    _proc_stat.nice,
                    _proc_stat.priority,
                    //
                    // User
                    _uid.real,
                    users_cache
                        .get_user_by_uid(_uid.real)
                        .map(|u| u.name().to_string_lossy().to_string()),
                    _uid.effective,
                    users_cache
                        .get_user_by_uid(_uid.effective)
                        .map(|u| u.name().to_string_lossy().to_string()),
                    //
                    // Group
                    _gid.real,
                    users_cache
                        .get_group_by_gid(_gid.real)
                        .map(|g| g.name().to_string_lossy().to_string()),
                    _gid.effective,
                    users_cache
                        .get_group_by_gid(_gid.effective)
                        .map(|g| g.name().to_string_lossy().to_string()),
                    //
                    // Executable/paths
                    _proc_stat.comm,
                    _exe.map(|p| p.to_string_lossy().to_string())
                        .unwrap_or("".to_string()),
                    proc.cmdline().unwrap_or(None),
                    _cwd.map(|p| p.to_string_lossy().to_string())
                        .unwrap_or("".to_string()),
                    //
                    // CPU
                    _proc_stat.utime.as_millis() as u64,
                    _proc_stat.stime.as_millis() as u64,
                    _proc_stat.cutime.as_millis() as u64,
                    _proc_stat.cstime.as_millis() as u64,
                    //
                    // Memory
                    _mem.rss(),
                    _mem.vms(),
                ];

                let res = appender.append_row(params);
                if let Err(e) = res {
                    eprintln!("Error appending row for {}: {:?}", proc.pid(), e);
                }
            }
        }

        Ok(())
    }
}

impl Drop for ProcDB {
    fn drop(&mut self) {
        self.stop();
    }
}
