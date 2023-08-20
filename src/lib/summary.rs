use std::sync::{Arc, Mutex};
use std::thread;

use bytesize::ByteSize;
use crossbeam::channel::Receiver;
use egui::{Context, Ui};
use psutil::memory::VirtualMemory;

use crate::lib::Widget;

pub struct SummaryPanel {
    virt_mem: Arc<Mutex<Option<VirtualMemory>>>,
    refresh_thread: Option<thread::JoinHandle<()>>,
}

impl Default for SummaryPanel {
    fn default() -> Self {
        Self {
            virt_mem: Arc::new(Mutex::new(None)),
            refresh_thread: None,
        }
    }
}

impl Widget for SummaryPanel {
    fn view(&mut self, ctx: &Context, ui: &mut Ui) {
        ui.heading("MemHog");
        ui.separator();
        ui.label("Memory usage process explorer");

        let mut virt_mem_binding = self.virt_mem.lock().unwrap();
        if let Some(virt_mem) = virt_mem_binding.as_ref() {
            ui.label(format!("Total: {}", ByteSize(virt_mem.total())));
            ui.label(format!("Available: {}", ByteSize(virt_mem.available())));
            ui.label(format!("Used: {}", ByteSize(virt_mem.used())));
            ui.label(format!("Free: {}", ByteSize(virt_mem.free())));
            ui.add(egui::ProgressBar::new(virt_mem.percent() as f32 / 100.0));
        }
    }

    fn setup_refresh_channel(&mut self, on_refresh: Receiver<()>) {
        let mut virt_mem = Arc::clone(&self.virt_mem);

        self.refresh_thread = Some(thread::spawn(move || loop {
            match on_refresh.recv() {
                Ok(_) => {
                    *virt_mem.lock().unwrap() = Some(
                        psutil::memory::virtual_memory().expect("Failed to get virtual memory"),
                    );
                },
                Err(_) => break,
            }
        }));
    }
}
