use std::ops::DerefMut;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use eframe::{CreationContext, Frame};
use egui::{Context, Ui};

use procdb::ProcDB;

use crate::lib::query_table::QueryTableWindow;

pub mod procdb;
pub mod query_table;

pub trait Widget {
    fn view(&mut self, ctx: &Context, ui: &mut Ui);
    fn setup_refresh_channel(&mut self, on_refresh: crossbeam::channel::Receiver<()>) {}
}

enum WidgetMessage<'a> {
    View(&'a Context, &'a mut Ui),
    Refresh,
    Quit,
}

pub struct MemHogApp {
    proc_db: Arc<Mutex<ProcDB>>,
    query_thread: Option<std::thread::JoinHandle<()>>,
    context: Arc<Mutex<Context>>,
    widget_refresh_tx: crossbeam::channel::Sender<()>,
    widget_refresh_rx: crossbeam::channel::Receiver<()>,
    widgets: Arc<Mutex<Vec<Box<dyn Widget>>>>,
}

impl MemHogApp {
    pub(crate) fn new(ctx: &CreationContext) -> Self {
        let (refresh_tx, refresh_rx): (Sender<()>, Receiver<()>) = channel();
        let on_refresh_tx = refresh_tx.clone();

        let (widget_refresh_tx, widget_refresh_rx) = crossbeam::channel::unbounded();

        let mut slf = Self {
            proc_db: Arc::new(Mutex::new(ProcDB::new(Some(move || {
                on_refresh_tx.clone().send(()).unwrap();
            })))),
            query_thread: None,
            context: Arc::new(Mutex::new(ctx.egui_ctx.clone())),
            widget_refresh_tx,
            widget_refresh_rx,
            widgets: Arc::new(Mutex::new(Vec::new())),
        };

        let egui_ctx = ctx.egui_ctx.clone();
        let on_widget_refresh = slf.widget_refresh_tx.clone();

        slf.query_thread = Some(std::thread::spawn(move || loop {
            match refresh_rx.recv() {
                Ok(_) => {
                    if on_widget_refresh.send(()).is_err() {
                        break;
                    }
                    egui_ctx.request_repaint();
                },
                Err(_) => break,
            }
        }));

        let query_win = Box::new(QueryTableWindow::new(
            slf.proc_db.lock().unwrap().cursor().unwrap(),
            // language=SQL
            "SELECT \
                        pid, \
                        name, \
                        format_bytes(memory_rss::bigint) AS rss \
                    FROM procs \
                    ORDER BY memory_rss DESC;",
        ));
        slf.add_widget(query_win);

        slf.proc_db.lock().unwrap().start();

        slf
    }

    fn add_widget(&mut self, mut widget: Box<dyn Widget>) {
        widget.setup_refresh_channel(self.widget_refresh_rx.clone());
        self.widgets.lock().unwrap().push(widget);
    }

    fn view(&mut self, ctx: &Context, ui: &mut Ui, _frame: &mut Frame) {
        self.widgets.lock().unwrap().iter_mut().for_each(|w| {
            w.view(ctx, ui);
        });
    }
}

impl eframe::App for MemHogApp {
    fn update(&mut self, ctx: &Context, frame: &mut Frame) {
        egui::SidePanel::left("side_panel").show(ctx, |ui| {
            ui.heading("MemHog");
            ui.separator();
            ui.label("Memory usage process explorer");
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            self.view(ctx, ui, frame);
        });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.proc_db.lock().unwrap().stop();
    }
}
