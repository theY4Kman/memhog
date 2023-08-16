use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::thread;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use crossbeam::channel::{Receiver, Sender};
use crossbeam::select;
use duckdb::arrow::array::Array;
use duckdb::types::{TimeUnit, ValueRef};
use duckdb::{params, Connection, Rows, Statement};
use egui_extras::{Column, TableBuilder};
use ouroboros::self_referencing;

use crate::lib::Widget;

pub struct QueryTableWindow {
    query: String,
    did_restore_query_state: bool,
    conn: Connection,

    executor: Arc<Mutex<Option<QueryExecutor>>>,
    query_change_tx: Sender<String>,
    query_change_rx: Receiver<String>,
    refresh_thread: Option<thread::JoinHandle<()>>,

    // We store stringified versions of each row, for display in the table
    results: Arc<Mutex<Vec<Vec<String>>>>,
    column_names: Arc<Mutex<Vec<String>>>,
    error: Arc<Mutex<Option<String>>>,
}

#[self_referencing]
struct QueryExecutor {
    cursor: Connection,
    #[borrows(cursor)]
    #[covariant]
    stmt: Statement<'this>,
    query: String,
}

impl QueryExecutor {
    pub fn from_query(conn: Connection, query: &str) -> Result<Self, duckdb::Error> {
        QueryExecutor::try_new(
            conn,
            |cursor| match cursor.prepare(query) {
                Ok(stmt) => Ok(stmt),
                Err(err) => Err(err),
            },
            query.to_string(),
        )
    }

    pub fn query<F, V>(&mut self, transform: F) -> Result<V, duckdb::Error>
    where
        F: FnOnce(&mut Rows) -> V,
    {
        self.with_stmt_mut(|stmt| match stmt.query(params![]) {
            Ok(mut rows) => Ok(transform(&mut rows)),
            Err(err) => Err(err),
        })
    }
}

impl Clone for QueryExecutor {
    fn clone(&self) -> Self {
        self.with(|fields| {
            let cursor = fields.cursor.try_clone().unwrap();
            QueryExecutor::from_query(cursor, fields.query.as_str()).unwrap()
        })
    }
}

impl QueryTableWindow {
    pub fn new(conn: Connection, query: &str) -> Self {
        let (query_change_tx, query_change_rx): (Sender<String>, Receiver<String>) =
            crossbeam::channel::unbounded();

        let mut slf = Self {
            query: query.to_string(),
            did_restore_query_state: false,
            conn,

            executor: Arc::new(Mutex::new(None)),
            query_change_tx,
            query_change_rx,
            refresh_thread: None,

            results: Arc::new(Mutex::new(Vec::new())),
            column_names: Arc::new(Mutex::new(Vec::new())),
            error: Arc::new(Mutex::new(None)),
        };

        slf.parse_query();

        slf
    }

    pub fn parse_query(&self) {
        match QueryExecutor::from_query(self.conn.try_clone().unwrap(), self.query.as_str()) {
            Ok(executor) => {
                *self.executor.lock().unwrap() = Some(executor);
                *self.error.lock().unwrap() = None;
                self.query_change_tx.send(self.query.clone()).unwrap();
            },
            Err(e) => {
                *self.executor.lock().unwrap() = None;
                *self.error.lock().unwrap() = Some(e.to_string());
            },
        }

        self.refresh();
    }

    pub fn refresh(&self) {
        Self::_refresh(
            Arc::clone(&self.executor),
            Arc::clone(&self.results),
            Arc::clone(&self.column_names),
            Arc::clone(&self.error),
        );
    }

    fn _refresh(
        mut executor: Arc<Mutex<Option<QueryExecutor>>>,
        results: Arc<Mutex<Vec<Vec<String>>>>,
        column_names: Arc<Mutex<Vec<String>>>,
        error: Arc<Mutex<Option<String>>>,
    ) {
        let mut executor_binding = executor.lock().unwrap();
        let maybe_executor = executor_binding.deref_mut();
        if maybe_executor.is_none() {
            return;
        }

        let executor = maybe_executor.as_mut().unwrap();
        executor
            .query(|rows| {
                *error.lock().unwrap() = None;

                *column_names.lock().unwrap() = rows
                    .as_ref()
                    .unwrap()
                    .column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();

                let mut results = results.lock().unwrap();
                results.clear();

                while let Some(row) = rows.next().expect("Error reading row") {
                    let mut row_strs = Vec::new();
                    let mut i = 0;
                    loop {
                        let col = match row.get_ref(i) {
                            Ok(col) => col,
                            Err(_) => break,
                        };
                        row_strs.push(match col {
                            ValueRef::Null => "<NULL>".to_string(),
                            ValueRef::Boolean(b) => b.to_string(),
                            ValueRef::TinyInt(i) => i.to_string(),
                            ValueRef::SmallInt(i) => i.to_string(),
                            ValueRef::Int(i) => i.to_string(),
                            ValueRef::BigInt(i) => i.to_string(),
                            ValueRef::HugeInt(i) => i.to_string(),
                            ValueRef::UTinyInt(i) => i.to_string(),
                            ValueRef::USmallInt(i) => i.to_string(),
                            ValueRef::UInt(i) => i.to_string(),
                            ValueRef::UBigInt(i) => i.to_string(),
                            ValueRef::Float(f) => f.to_string(),
                            ValueRef::Double(f) => f.to_string(),
                            ValueRef::Decimal(d) => d.to_string(),
                            ValueRef::Timestamp(unit, i) => {
                                let naive = match unit {
                                    TimeUnit::Second => NaiveDateTime::from_timestamp_opt(i, 0),
                                    TimeUnit::Millisecond => {
                                        NaiveDateTime::from_timestamp_millis(i)
                                    },
                                    TimeUnit::Microsecond => {
                                        NaiveDateTime::from_timestamp_micros(i)
                                    },
                                    TimeUnit::Nanosecond => NaiveDateTime::from_timestamp_opt(
                                        i / 1_000_000_000,
                                        (i % 1_000_000_000) as u32,
                                    ),
                                };
                                if let Some(naive) = naive {
                                    DateTime::<Utc>::from_utc(naive, Utc).to_string()
                                } else {
                                    "<INVALID TIMESTAMP>".to_string()
                                }
                            },
                            ValueRef::Text(_) => col.as_str().unwrap().to_string(),
                            ValueRef::Blob(_) => "<BLOB>".to_string(),
                            ValueRef::Date32(i) => NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .checked_add_signed(chrono::Duration::days(i as i64))
                                .unwrap()
                                .to_string(),
                            ValueRef::Time64(unit, i) => {
                                let naive = match unit {
                                    TimeUnit::Second => {
                                        NaiveTime::from_num_seconds_from_midnight_opt(i as u32, 0)
                                    },
                                    TimeUnit::Millisecond => {
                                        NaiveTime::from_num_seconds_from_midnight_opt(
                                            (i / 1000) as u32,
                                            (i % 1000) as u32 * 1_000_000,
                                        )
                                    },
                                    TimeUnit::Microsecond => {
                                        NaiveTime::from_num_seconds_from_midnight_opt(
                                            (i / 1_000_000) as u32,
                                            (i % 1_000_000) as u32 * 1_000,
                                        )
                                    },
                                    TimeUnit::Nanosecond => {
                                        NaiveTime::from_num_seconds_from_midnight_opt(
                                            (i / 1_000_000_000) as u32,
                                            (i % 1_000_000_000) as u32,
                                        )
                                    },
                                };
                                if let Some(naive) = naive {
                                    naive.to_string()
                                } else {
                                    "<INVALID TIME>".to_string()
                                }
                            },
                        });
                        i += 1;
                    }
                    results.push(row_strs);
                }
            })
            .ok();
    }
}

impl Widget for QueryTableWindow {
    fn view(&mut self, ctx: &egui::Context, ui: &mut egui::Ui) {
        egui::Window::new("Query").resizable(true).show(ctx, |ui| {
            ui.horizontal(|ui| {
                let state_id = ui.make_persistent_id("query");
                let mut query =
                    ui.data_mut(|storage| match storage.get_persisted::<String>(state_id) {
                        None => self.query.clone(),
                        Some(s) => {
                            if !self.did_restore_query_state {
                                self.did_restore_query_state = true;
                                self.query = s.clone();
                                self.parse_query();
                            }
                            s
                        },
                    });

                ui.label("Query:");
                ui.text_edit_multiline(&mut query);
                if ui.button("Run").clicked() {
                    self.query = query.clone();
                    self.parse_query();
                }

                ui.data_mut(|storage| {
                    storage.insert_persisted(state_id, query.clone());
                });
            });

            if let Some(error) = self.error.lock().unwrap().deref() {
                ui.label(error);
            }

            let results = self.results.lock().unwrap();
            let column_names = self.column_names.lock().unwrap();

            if column_names.is_empty() {
                return;
            }

            ui.label("Result:");
            ui.separator();

            ui.push_id("table", |ui| {
                TableBuilder::new(ui)
                    .auto_shrink([false, true])
                    .striped(true)
                    .resizable(true)
                    .selectable(true)
                    .frame(true)
                    .columns(Column::auto(), column_names.len())
                    .header(20.0, |mut header| {
                        for name in column_names.iter() {
                            header.col(|ui| {
                                ui.label(name);
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(20.0, results.len(), |idx, mut row| {
                            let data = &results[idx];
                            for col_data in data {
                                row.col(|ui| {
                                    ui.label(col_data);
                                });
                            }
                        });
                    });
            });
        });
    }

    fn setup_refresh_channel(&mut self, on_refresh: Receiver<()>) {
        let on_query_change = self.query_change_rx.clone();

        let query = self.query.clone();
        let conn = self.conn.try_clone().unwrap();
        let results = Arc::clone(&self.results);
        let column_names = Arc::clone(&self.column_names);
        let error = Arc::clone(&self.error);

        self.refresh_thread = Some(thread::spawn(move || {
            let executor = Arc::new(Mutex::new(
                QueryExecutor::from_query(conn.try_clone().unwrap(), query.as_str()).ok(),
            ));

            loop {
                select! {
                    recv(on_refresh) -> res => {
                        if res.is_err() {
                            break;
                        }
                    },
                    recv(on_query_change) -> res => {
                        if let Ok(query) = res {
                            *executor.lock().unwrap().deref_mut() =
                                QueryExecutor::from_query(conn.try_clone().unwrap(), query.as_str()).ok();
                            continue;
                        } else {
                            break;
                        }
                    }
                }

                let executor = Arc::clone(&executor);
                let results = Arc::clone(&results);
                let column_names = Arc::clone(&column_names);
                let error = Arc::clone(&error);

                Self::_refresh(executor, results, column_names, error);
            }
        }));
    }
}
