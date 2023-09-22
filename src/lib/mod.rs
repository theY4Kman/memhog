use std::any::Any;
use std::collections::HashMap;
use std::fs;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

use crossbeam::channel;
use crossbeam::channel::{Receiver, Sender};
use eframe::{CreationContext, Frame};
use egui::{Context, Modifiers, Ui};

use procdb::ProcDB;

use crate::lib::query_table::QueryTableWindow;

pub mod procdb;
pub mod query_table;
mod summary;
pub mod syntax_highlighting;

#[typetag::serde(tag = "type", content = "state")]
pub trait WidgetPersistentState {
    fn as_any(&self) -> &dyn Any;
}

pub trait Widget {
    fn get_id(&self) -> String;
    fn get_state(&self) -> Option<Box<dyn WidgetPersistentState>> {
        None
    }
    fn load_state(&mut self, loaded_state: Box<dyn WidgetPersistentState>) -> Result<(), String> {
        Ok(())
    }
    fn view(&mut self, ctx: &Context, ui: &mut Ui);
    fn setup_refresh_channel(&mut self, on_refresh: Receiver<()>) {}
}

enum WidgetMessage<'a> {
    View(&'a Context, &'a mut Ui),
    Refresh,
    Quit,
}

pub struct MemHogApp {
    state_path: String,
    is_initialized: bool,

    proc_db: Arc<Mutex<ProcDB>>,
    query_thread: Option<std::thread::JoinHandle<()>>,
    context: Arc<Mutex<Context>>,
    widget_refresh_tx: Sender<()>,
    widget_refresh_rx: Receiver<()>,

    summary_panel: summary::SummaryPanel,
    widgets: Arc<Mutex<HashMap<String, Box<dyn Widget>>>>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MemHogAppPersistentState {
    windows: HashMap<String, Option<Box<dyn WidgetPersistentState>>>,
}

impl Default for MemHogAppPersistentState {
    fn default() -> Self {
        Self {
            windows: HashMap::from([(String::from("Query Table"), None)]),
        }
    }
}

#[derive(clap::Parser, Debug)]
pub struct MemHogAppConfig {
    #[arg(short, long = "db", default_value_t = String::from(":memory:"))]
    db_path: String,

    #[arg(short, long = "state", value_hint = clap::ValueHint::FilePath, default_value_t = String::from("state.toml"))]
    state_path: String,
}

impl MemHogApp {
    pub(crate) fn new(ctx: &CreationContext, config: MemHogAppConfig) -> Self {
        let (refresh_tx, refresh_rx): (Sender<()>, Receiver<()>) = channel::bounded(1);
        let on_refresh_tx = refresh_tx.clone();

        let (widget_refresh_tx, widget_refresh_rx) = channel::unbounded();

        let mut slf = Self {
            state_path: config.state_path,
            is_initialized: false,
            proc_db: Arc::new(Mutex::new(ProcDB::new(
                config.db_path,
                Some(move || {
                    on_refresh_tx.clone().send(()).unwrap();
                }),
            ))),
            query_thread: None,
            context: Arc::new(Mutex::new(ctx.egui_ctx.clone())),
            widget_refresh_tx,
            widget_refresh_rx,

            summary_panel: summary::SummaryPanel::default(),
            widgets: Arc::new(Mutex::new(HashMap::new())),
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

        slf.summary_panel
            .setup_refresh_channel(slf.widget_refresh_rx.clone());

        slf.load_state().unwrap();
        slf.is_initialized = true;

        slf.proc_db.lock().unwrap().start();

        slf
    }

    fn read_state_file(&self) -> Result<MemHogAppPersistentState, std::io::Error> {
        let contents = match fs::read_to_string(&self.state_path) {
            Ok(contents) => contents,
            Err(e) => {
                return if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(MemHogAppPersistentState::default())
                } else {
                    Err(e)
                }
            },
        };

        let state: MemHogAppPersistentState = match toml::from_str(&contents) {
            Ok(state) => state,
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse state file: {}", e),
                ))
            },
        };

        Ok(state)
    }

    fn load_state(&mut self) -> Result<(), std::io::Error> {
        let state = self.read_state_file()?;

        for (id, state) in state.windows {
            self.new_query_window(Some(id), state);
        }

        Ok(())
    }

    fn save_state(&self) -> Result<(), std::io::Error> {
        let mut state = self.read_state_file()?;

        state.windows = self
            .widgets
            .lock()
            .unwrap()
            .values_mut()
            .into_iter()
            .map(|w| (w.get_id().clone(), w.get_state()))
            .collect();

        let state_toml = toml::to_string(&state).unwrap();

        fs::write(&self.state_path, state_toml)?;

        Ok(())
    }

    fn add_widget(&mut self, mut widget: Box<dyn Widget>) {
        widget.setup_refresh_channel(self.widget_refresh_rx.clone());
        self.widgets.lock().unwrap().insert(widget.get_id(), widget);

        if self.is_initialized {
            self.save_state().unwrap();
        }
    }

    fn view(&mut self, ctx: &Context, ui: &mut Ui, _frame: &mut Frame) {
        self.widgets
            .lock()
            .unwrap()
            .values_mut()
            .into_iter()
            .for_each(|w| {
                w.view(ctx, ui);
            });
    }

    fn new_query_window(
        &mut self,
        id: Option<String>,
        state: Option<Box<dyn WidgetPersistentState>>,
    ) {
        let mut query_win = Box::new(QueryTableWindow::new(
            self.proc_db.lock().unwrap().cursor().unwrap(),
            // language=SQL
            "SELECT \
                        pid, \
                        name, \
                        format_bytes(memory_rss::bigint) AS rss \
                    FROM procs \
                    ORDER BY memory_rss DESC;",
            id,
        ));
        if let Some(state) = state {
            query_win.load_state(state).unwrap();
        }
        self.add_widget(query_win);
    }
}

impl eframe::App for MemHogApp {
    fn update(&mut self, ctx: &Context, frame: &mut Frame) {
        egui::TopBottomPanel::top("menu_bar").show(ctx, |ui| {
            let new_window_shortcut = egui::KeyboardShortcut::new(Modifiers::CTRL, egui::Key::N);

            if ui.input_mut(|i| i.consume_shortcut(&new_window_shortcut)) {
                self.new_query_window(None, None);
            }

            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui
                        .add(
                            egui::Button::new("New Window")
                                .shortcut_text(ui.ctx().format_shortcut(&new_window_shortcut)),
                        )
                        .clicked()
                    {
                        self.new_query_window(None, None);
                        ui.close_menu();
                    }

                    ui.separator();
                    if ui.button("Quit").clicked() {
                        ui.close_menu();
                        frame.close();
                    }
                });
            });
        });

        egui::SidePanel::left("summary_panel").show(ctx, |ui| {
            self.summary_panel.view(ctx, ui);
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            self.view(ctx, ui, frame);
        });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.save_state().unwrap();
        self.proc_db.lock().unwrap().stop();
    }
}
