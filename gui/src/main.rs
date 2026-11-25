//! storeboard's window: every store, peer and byte on this machine, and the
//! verbs on them (scan, send, versions and restore, take over, clean up).

mod api;

use adw::prelude::*;
use api::Snapshot;
use gtk::{gio, glib};
use serde_json::json;
use std::cell::RefCell;
use std::rc::Rc;

const APP_ID: &str = "dev.grid.Storeboard";

fn main() -> glib::ExitCode {
    let app = adw::Application::builder().application_id(APP_ID).build();
    app.connect_activate(|app| {
        let ui = Ui::new(app);
        ui.window.present();
        ui.clone().start();
    });
    app.run()
}

#[derive(Clone, Copy, PartialEq)]
enum Page {
    Overview,
    Stores,
    Peers,
    Space,
}

const PAGES: [(Page, &str, &str); 4] = [
    (Page::Overview, "Overview", "emblem-synchronizing-symbolic"),
    (Page::Stores, "Stores", "folder-symbolic"),
    (Page::Peers, "Peers", "network-workgroup-symbolic"),
    (Page::Space, "Space", "drive-harddisk-symbolic"),
];

struct Inner {
    window: adw::ApplicationWindow,
    toasts: adw::ToastOverlay,
    split: adw::NavigationSplitView,
    sidebar: gtk::ListBox,
    content: gtk::Box,
    title: adw::WindowTitle,
    page: RefCell<Page>,
    snap: RefCell<Option<Result<Snapshot, String>>>,
    /// The page on screen, to tell a redraw from a page change.
    shown: RefCell<Option<Page>>,
    /// Whether the test-only expand option has opened its row once.
    opened: std::cell::Cell<bool>,
    /// Space and cleanup, counted separately (they are slow), and when.
    heavy: RefCell<Option<Result<api::Heavy, String>>>,
    heavy_at: std::cell::Cell<Option<std::time::Instant>>,
}

#[derive(Clone)]
struct Ui(Rc<Inner>);

impl std::ops::Deref for Ui {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.0
    }
}

impl Ui {
    fn new(app: &adw::Application) -> Ui {
        let window = adw::ApplicationWindow::builder().application(app).title("Storeboard").default_width(1000).default_height(700).build();
        let sidebar = gtk::ListBox::new();
        sidebar.add_css_class("navigation-sidebar");
        for (_, name, icon) in PAGES {
            let row = gtk::Box::new(gtk::Orientation::Horizontal, 12);
            row.set_margin_top(6);
            row.set_margin_bottom(6);
            row.set_margin_start(6);
            row.append(&gtk::Image::from_icon_name(icon));
            let label = gtk::Label::new(Some(name));
            label.set_xalign(0.0);
            label.set_hexpand(true);
            row.append(&label);
            let badge = gtk::Label::new(None);
            badge.add_css_class("dim-label");
            row.append(&badge);
            sidebar.append(&row);
        }
        let side_view = adw::ToolbarView::new();
        let side_header = adw::HeaderBar::new();
        side_header.set_title_widget(Some(&adw::WindowTitle::new("Storeboard", "")));
        side_view.add_top_bar(&side_header);
        let side_scroll = gtk::ScrolledWindow::new();
        side_scroll.set_child(Some(&sidebar));
        side_view.set_content(Some(&side_scroll));
        let side_page = adw::NavigationPage::new(&side_view, "Storeboard");

        let title = adw::WindowTitle::new("Overview", "");
        let header = adw::HeaderBar::new();
        header.set_title_widget(Some(&title));
        let refresh = gtk::Button::from_icon_name("view-refresh-symbolic");
        refresh.set_tooltip_text(Some("Refresh"));
        header.pack_end(&refresh);
        let content = gtk::Box::new(gtk::Orientation::Vertical, 0);
        content.set_vexpand(true);
        let main_view = adw::ToolbarView::new();
        main_view.add_top_bar(&header);
        main_view.set_content(Some(&content));
        let toasts = adw::ToastOverlay::new();
        toasts.set_child(Some(&main_view));
        let main_page = adw::NavigationPage::new(&toasts, "Overview");

        let split = adw::NavigationSplitView::new();
        split.set_sidebar(Some(&side_page));
        split.set_content(Some(&main_page));
        window.set_content(Some(&split));
        let bp = adw::Breakpoint::new(adw::BreakpointCondition::new_length(adw::BreakpointConditionLengthType::MaxWidth, 640.0, adw::LengthUnit::Sp));
        bp.add_setter(&split, "collapsed", Some(&true.to_value()));
        window.add_breakpoint(bp);

        let ui = Ui(Rc::new(Inner { window, toasts, split, sidebar, content, title, page: RefCell::new(Page::Overview), snap: RefCell::new(None), shown: RefCell::new(None), opened: std::cell::Cell::new(false), heavy: RefCell::new(None), heavy_at: std::cell::Cell::new(None) }));
        let u = ui.clone();
        ui.sidebar.connect_row_selected(move |_, row| {
            if let Some(row) = row {
                *u.page.borrow_mut() = PAGES[row.index() as usize].0;
                u.split.set_show_content(true);
                if PAGES[row.index() as usize].0 == Page::Space {
                    u.count_space(false);
                }
                u.render();
            }
        });
        let u = ui.clone();
        refresh.connect_clicked(move |_| {
            if *u.page.borrow() == Page::Space {
                u.count_space(true);
            }
            u.refresh();
        });
        // STOREBOARD_GUI_PAGE opens on another page (screenshots, shortcuts).
        let start = match std::env::var("STOREBOARD_GUI_PAGE").as_deref() {
            Ok("stores") => 1,
            Ok("peers") => 2,
            Ok("space") => 3,
            _ => 0,
        };
        ui.sidebar.select_row(ui.sidebar.row_at_index(start).as_ref());
        ui
    }

    fn start(self) {
        self.refresh();
        let u = self.clone();
        glib::timeout_add_seconds_local(3, move || {
            u.refresh();
            glib::ControlFlow::Continue
        });
    }

    fn refresh(&self) {
        if *self.page.borrow() == Page::Space {
            self.count_space(false);
        }
        let u = self.clone();
        glib::spawn_future_local(async move {
            let snap = gio::spawn_blocking(api::snapshot).await.unwrap_or_else(|_| Err("crashed".into())).map(|mut s| {
                // The uptime moves every second; on its own it is no reason
                // to redraw (the next real change shows it).
                if let Some(Ok(old)) = u.snap.borrow().as_ref() {
                    if old.status.version == s.status.version && s.status.uptime >= old.status.uptime {
                        s.status.uptime = old.status.uptime;
                    }
                }
                s
            });
            if u.snap.borrow().as_ref() == Some(&snap) {
                return;
            }
            *u.snap.borrow_mut() = Some(snap);
            u.render();
        });
    }

    /// Counts space and cleanup: when the Space page asks, at most once a
    /// minute unless `now`.
    fn count_space(&self, now: bool) {
        let fresh = self.heavy_at.get().is_some_and(|t| t.elapsed() < std::time::Duration::from_secs(60));
        if fresh && !now {
            return;
        }
        self.heavy_at.set(Some(std::time::Instant::now()));
        let u = self.clone();
        glib::spawn_future_local(async move {
            let h = gio::spawn_blocking(api::heavy).await.unwrap_or_else(|_| Err("crashed".into()));
            if u.heavy.borrow().as_ref() != Some(&h) {
                *u.heavy.borrow_mut() = Some(h);
                if *u.page.borrow() == Page::Space {
                    u.render();
                }
            }
        });
    }

    fn toast(&self, text: &str) {
        self.toasts.add_toast(adw::Toast::new(text));
    }

    /// Runs a request off the main loop, then toasts and refreshes.
    fn act(&self, work: impl FnOnce() -> Result<serde_json::Value, String> + Send + 'static, ok: impl FnOnce(&Ui, serde_json::Value) + 'static) {
        let u = self.clone();
        glib::spawn_future_local(async move {
            match gio::spawn_blocking(work).await.unwrap_or_else(|_| Err("crashed".into())) {
                Ok(v) => ok(&u, v),
                Err(e) => u.toast(&e),
            }
            u.refresh();
        });
    }

    fn render(&self) {
        let page = *self.page.borrow();
        let (_, name, _) = PAGES.iter().find(|p| p.0 == page).copied().unwrap();
        self.title.set_title(name);
        let snap = self.snap.borrow().clone();
        self.badges(snap.as_ref().and_then(|s| s.as_ref().ok()));
        let view: gtk::Widget = match snap {
            None => adw::Spinner::new().upcast(),
            Some(Err(e)) => adw::StatusPage::builder()
                .icon_name("network-offline-symbolic")
                .title("storeboard is not reachable")
                .description(format!("{e}\n\nIt runs as storeboard.service."))
                .build()
                .upcast(),
            Some(Ok(s)) => match page {
                Page::Overview => self.overview(&s),
                Page::Stores => self.stores(&s),
                Page::Peers => self.peers(&s),
                Page::Space => self.space(),
            },
        };
        // Same page redrawn: keep where the reader was.
        let same = *self.shown.borrow() == Some(page);
        let keep = self.content.first_child().filter(|_| same).map(|c| kept(&c));
        while let Some(c) = self.content.first_child() {
            self.content.remove(&c);
        }
        view.set_vexpand(true);
        self.content.append(&view);
        if let Some(k) = keep {
            restore(&view, k);
        }
        *self.shown.borrow_mut() = Some(page);
    }

    fn badges(&self, s: Option<&Snapshot>) {
        let counts = [
            None,
            s.map(|s| s.status.stores.len().to_string()),
            s.map(|s| {
                let up = s.status.connections.iter().filter(|c| c.state == "connected").count();
                format!("{up}/{}", s.status.connections.len())
            }),
            s.and(self.heavy.borrow().as_ref().and_then(|h| h.as_ref().ok()).map(|h| api::bytes(h.space.total))),
        ];
        for (i, n) in counts.iter().enumerate() {
            if let Some(row) = self.sidebar.row_at_index(i as i32) {
                let b = row.child().and_then(|c| c.last_child()).and_downcast::<gtk::Label>().unwrap();
                b.set_label(n.as_deref().unwrap_or(""));
            }
        }
    }

    // {{{ Pages

    fn overview(&self, s: &Snapshot) -> gtk::Widget {
        let st = &s.status;
        let page = adw::PreferencesPage::new();
        let me = adw::PreferencesGroup::builder().title("This machine").build();
        me.add(&info("Peer", &st.peer));
        me.add(&info("Version", &format!("{} · up {}", st.version, api::duration(st.uptime))));
        let root = info("Root", &st.root);
        root.add_suffix(&open_button(&st.root, self));
        me.add(&root);
        me.add(&info("Daemon state", &st.data_dir));
        page.add(&me);

        let up = st.connections.iter().filter(|c| c.state == "connected").count();
        let conflicts: i64 = st.sync.iter().map(|x| x.conflicts).sum();
        // Pending is "known here but not on this machine": for a streamed
        // store that is every file, by design. In flight is what is moving.
        let pending: i64 = st.sync.iter().map(|x| x.receiving + if streamed(st, &x.store) { 0 } else { x.pending }).sum();
        let grid = adw::PreferencesGroup::builder().title("At a glance").build();
        grid.add(&info("Peers connected", &format!("{up} of {}", st.connections.len())));
        grid.add(&info("Stores", &format!("{} ({} requested by programs)", st.stores.len(), st.stores.iter().filter(|x| !x.requested_by.is_empty()).count())));
        grid.add(&info("In flight", &if pending == 0 { "nothing: every store is in step".to_string() } else { format!("{pending} files") }));
        grid.add(&info("Conflicts", &conflicts.to_string()));
        page.add(&grid);

        let health = adw::PreferencesGroup::builder()
            .title("Health")
            .description(format!("storeboard's own checks: {}", if s.doctor.status.is_empty() { "unknown" } else { &s.doctor.status }))
            .build();
        for c in &s.doctor.checks {
            let r = row(&c.name, &if c.hint.is_empty() { c.detail.clone() } else { format!("{} — {}", c.detail, c.hint) });
            r.add_prefix(&gtk::Image::from_icon_name(match c.status.as_str() {
                "ok" => "object-select-symbolic",
                "warn" | "warning" => "dialog-warning-symbolic",
                _ => "dialog-error-symbolic",
            }));
            health.add(&r);
        }
        page.add(&health);
        page.upcast()
    }

    fn stores(&self, s: &Snapshot) -> gtk::Widget {
        let page = adw::PreferencesPage::new();
        for g in &s.status.groups {
            let group = adw::PreferencesGroup::builder().title(format!("Group {}", g.name)).description(format!("Shared with {}", g.peers.join(", "))).build();
            let mut any = false;
            for st in s.status.stores.iter().filter(|x| x.group == g.name) {
                let r = self.store_row(s, st);
                // STOREBOARD_GUI_EXPAND opens the first store (screenshots).
                if !any && !self.opened.replace(true) && std::env::var("STOREBOARD_GUI_EXPAND").is_ok() {
                    r.set_expanded(true);
                }
                group.add(&r);
                any = true;
            }
            if any {
                page.add(&group);
            }
        }
        page.upcast()
    }

    fn store_row(&self, s: &Snapshot, st: &api::Store) -> adw::ExpanderRow {
        let sync = s.status.sync.iter().find(|x| x.store == st.name).cloned().unwrap_or_default();
        let stream = st.sync == "stream";
        let subtitle = if stream {
            format!("streamed · {} · {} files, fetched when opened", sync.state, sync.entries)
        } else {
            format!("local · {} · {} files", sync.state, sync.entries)
        };
        let r = adw::ExpanderRow::builder().title(glib::markup_escape_text(&st.name)).subtitle(subtitle).build();
        r.add_prefix(&gtk::Image::from_icon_name(if st.sync == "stream" { "folder-remote-symbolic" } else { "folder-symbolic" }));
        if !st.requested_by.is_empty() {
            let l = gtk::Label::new(Some(&format!("requested by {}", st.requested_by)));
            l.add_css_class("caption");
            l.add_css_class("dim-label");
            r.add_suffix(&l);
        }
        if sync.conflicts > 0 {
            let w = gtk::Image::from_icon_name("dialog-warning-symbolic");
            w.set_tooltip_text(Some(&format!("{} conflicts", sync.conflicts)));
            r.add_suffix(&w);
        }
        // Only real work spins: a streamed store keeps its files elsewhere.
        if sync.scanning || sync.receiving > 0 || (!stream && sync.pending > 0) {
            r.add_suffix(&adw::Spinner::new());
        }
        let root = info("Folder", &st.root);
        root.add_suffix(&open_button(&st.root, self));
        r.add_row(&root);
        r.add_row(&info(
            "Files",
            &if stream {
                format!("{} known · {} kept on this machine · {} arriving", sync.entries, sync.local, sync.receiving)
            } else {
                format!("{} here of {} · {} still to fetch · {} arriving", sync.local, sync.entries, sync.pending, sync.receiving)
            },
        ));
        r.add_row(&info("Last scan", &sync.last_scan.map(|t| api::ago(&t)).unwrap_or("not yet".into())));
        r.add_row(&info("Ownership", &format!("{}:{} {}", sync.permissions.user, sync.permissions.group, sync.permissions.mode)));
        for l in &sync.symlinks {
            let lr = row(&l.path, &if l.busy.is_empty() { l.state.clone() } else { format!("{}: {}", l.state, l.busy) });
            lr.add_prefix(&gtk::Image::from_icon_name("emblem-symbolic-link-symbolic"));
            if l.state == "busy" {
                let b = flat("Take over");
                b.set_tooltip_text(Some("Move what is in the way into the store and link the folder"));
                let (u, n) = (self.clone(), st.name.clone());
                b.connect_clicked(move |_| {
                    let n2 = n.clone();
                    u.act(move || api::post(&format!("/v1/stores/{}/takeover", api::esc(&n2)), json!({})), |u, _| u.toast("Taken over"));
                });
                lr.add_suffix(&b);
            }
            r.add_row(&lr);
        }
        let actions = row("Actions", "");
        let scan = flat("Scan now");
        let (u, n) = (self.clone(), st.name.clone());
        scan.connect_clicked(move |_| {
            let n2 = n.clone();
            u.act(move || api::post(&format!("/v1/stores/{}/scan", api::esc(&n2)), json!({})), |u, _| u.toast("Scanning"));
        });
        actions.add_suffix(&scan);
        let send = flat("Send a file…");
        let (u, n) = (self.clone(), st.name.clone());
        send.connect_clicked(move |_| u.send_file(&n));
        actions.add_suffix(&send);
        let vers = flat("Versions…");
        let (u, n, root) = (self.clone(), st.name.clone(), st.root.clone());
        vers.connect_clicked(move |_| u.pick_versions(&n, &root));
        actions.add_suffix(&vers);
        r.add_row(&actions);
        r
    }

    fn peers(&self, s: &Snapshot) -> gtk::Widget {
        let st = &s.status;
        let page = adw::PreferencesPage::new();
        let g = adw::PreferencesGroup::builder().title("Peers").description("Every machine this one syncs with.").build();
        for p in st.peers.iter().filter(|p| p.name != st.peer) {
            let c = st.connections.iter().find(|c| c.peer == p.name).cloned().unwrap_or_default();
            let mut sub = format!("{} · {}", c.state, p.endpoints.join(", "));
            if let Some(t) = c.since {
                sub.push_str(&format!(" · since {}", api::ago(&t)));
            }
            let r = adw::ExpanderRow::builder().title(glib::markup_escape_text(&p.name)).subtitle(glib::markup_escape_text(&sub)).build();
            r.add_prefix(&gtk::Image::from_icon_name(if c.state == "connected" { "network-transmit-receive-symbolic" } else { "network-offline-symbolic" }));
            if !c.endpoint.is_empty() {
                r.add_row(&info("Connected through", &c.endpoint));
            }
            if !c.version.is_empty() {
                r.add_row(&info("Version", &c.version));
            }
            if c.clock_skew_ms != 0 {
                r.add_row(&info("Clock skew", &format!("{} ms", c.clock_skew_ms)));
            }
            if !c.error.is_empty() {
                r.add_row(&info("Last error", &c.error));
            }
            g.add(&r);
        }
        page.add(&g);
        let groups = adw::PreferencesGroup::builder().title("Groups").build();
        for gr in &st.groups {
            groups.add(&info(&gr.name, &format!("{} · stores: {}", gr.peers.join(", "), gr.stores.join(", "))));
        }
        page.add(&groups);
        page.upcast()
    }

    fn space(&self) -> gtk::Widget {
        let heavy = self.heavy.borrow().clone();
        let s = match heavy {
            None => {
                self.count_space(false);
                return adw::StatusPage::builder()
                    .icon_name("drive-harddisk-symbolic")
                    .title("Counting…")
                    .description("storeboard walks its data directory for this: a few seconds on a big mesh.")
                    .child(&adw::Spinner::new())
                    .build()
                    .upcast();
            }
            Some(Err(e)) => return adw::StatusPage::builder().icon_name("dialog-error-symbolic").title("Could not count").description(e).build().upcast(),
            Some(Ok(h)) => h,
        };
        let s = &s;
        let page = adw::PreferencesPage::new();
        let g = adw::PreferencesGroup::builder().title(format!("Daemon state · {}", api::bytes(s.space.total))).description("Blobs, journals, history and trash, by directory.").build();
        let mut entries = s.space.entries.clone();
        entries.sort_by(|a, b| b.bytes.cmp(&a.bytes));
        for e in entries.iter().take(12) {
            g.add(&info(&e.path, &format!("{} · {} files", api::bytes(e.bytes), e.files)));
        }
        page.add(&g);
        let c = adw::PreferencesGroup::builder().title("Trash and history").description("What deletes and edits left behind, per store.").build();
        for st in &s.cleanup.stores {
            let r = row(
                &st.store,
                &format!(
                    "trash: {} files, {} (kept {}) · old versions: {}, {}",
                    st.trash_files,
                    api::bytes(st.trash_bytes),
                    if st.trash_keep.is_empty() { "-".to_string() } else { api::go_duration(&st.trash_keep) },
                    st.old_versions,
                    api::bytes(st.old_version_bytes)
                ),
            );
            if st.trash_files > 0 {
                let b = flat("Empty trash…");
                let (u, n, files) = (self.clone(), st.store.clone(), st.trash_files);
                b.connect_clicked(move |_| {
                    let n2 = n.clone();
                    u.confirm(&format!("Empty {n}'s trash?"), &format!("{files} files go for good."), "Empty", move |u| {
                        let n3 = n2.clone();
                        u.act(move || api::post("/v1/cleanup", json!({"what": "trash", "stores": [n3]})), |u, _| u.toast("Trash emptied"));
                    });
                });
                r.add_suffix(&b);
            }
            c.add(&r);
        }
        page.add(&c);
        if !s.cleanup.orphans.is_empty() || s.cleanup.unreferenced_bytes > 0 {
            let o = adw::PreferencesGroup::builder().title("Left behind").build();
            for or in &s.cleanup.orphans {
                o.add(&row(&or.store, &format!("no longer configured · {} of data{}", api::bytes(or.data_bytes), if or.folder.is_empty() { String::new() } else { format!(" · folder {}", or.folder) })));
            }
            if s.cleanup.unreferenced_bytes > 0 {
                o.add(&row("Unreferenced content", &format!("{} the next collection removes", api::bytes(s.cleanup.unreferenced_bytes))));
            }
            page.add(&o);
        }
        page.upcast()
    }

    // }}}

    // {{{ Dialogs

    fn send_file(&self, store: &str) {
        let d = gtk::FileDialog::builder().title(format!("Send a file into {store}")).build();
        let (u, n) = (self.clone(), store.to_string());
        d.open(Some(&self.window), gio::Cancellable::NONE, move |res| {
            let Some(path) = res.ok().and_then(|f| f.path()) else { return };
            let (n2, src) = (n.clone(), path.to_string_lossy().to_string());
            u.act(move || api::post(&format!("/v1/stores/{}/send", api::esc(&n2)), json!({"source": src})), |u, v| {
                u.toast(&format!("Sent to {}", v.get("path").and_then(|p| p.as_str()).unwrap_or("the store")));
            });
        });
    }

    fn pick_versions(&self, store: &str, root: &str) {
        let d = gtk::FileDialog::builder().title(format!("A file in {store}")).initial_folder(&gio::File::for_path(root)).build();
        let (u, n, root) = (self.clone(), store.to_string(), root.to_string());
        d.open(Some(&self.window), gio::Cancellable::NONE, move |res| {
            let Some(path) = res.ok().and_then(|f| f.path()) else { return };
            let Ok(rel) = path.strip_prefix(&root) else {
                u.toast("That file is not in this store");
                return;
            };
            u.show_versions(&n, &rel.to_string_lossy());
        });
    }

    fn show_versions(&self, store: &str, rel: &str) {
        let (u, n, p) = (self.clone(), store.to_string(), rel.to_string());
        glib::spawn_future_local(async move {
            let (n2, p2) = (n.clone(), p.clone());
            let res = gio::spawn_blocking(move || api::get::<api::Versions>(&format!("/v1/stores/{}/versions?path={}", api::esc(&n2), api::esc(&p2))))
                .await
                .unwrap_or_else(|_| Err("crashed".into()));
            let vs = match res {
                Ok(v) => v,
                Err(e) => return u.toast(&e),
            };
            let d = adw::Dialog::builder().title(format!("{p} — versions")).content_width(560).content_height(480).build();
            let view = adw::ToolbarView::new();
            view.add_top_bar(&adw::HeaderBar::new());
            let pp = adw::PreferencesPage::new();
            let g = adw::PreferencesGroup::builder().description(format!("Kept for {}", if vs.keep.is_empty() { "1 year".to_string() } else { api::go_duration(&vs.keep) })).build();
            for v in vs.versions.iter().rev() {
                let r = row(
                    &v.time.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S").to_string(),
                    &if v.deleted { "deleted".into() } else { api::bytes(v.size) },
                );
                if !v.deleted && !v.blob.is_empty() {
                    let b = flat("Restore");
                    let (u, n, p, blob) = (u.clone(), n.clone(), p.clone(), v.blob.clone());
                    b.connect_clicked(move |_| {
                        let (n2, p2, b2) = (n.clone(), p.clone(), blob.clone());
                        u.act(move || api::post(&format!("/v1/stores/{}/restore", api::esc(&n2)), json!({"path": p2, "blob": b2})), |u, v| {
                            let beside = v.get("copied").and_then(|c| c.as_bool()).unwrap_or(false);
                            u.toast(if beside { "Restored beside the file" } else { "Restored" });
                        });
                    });
                    r.add_suffix(&b);
                }
                g.add(&r);
            }
            if vs.versions.is_empty() {
                g.add(&row("No versions recorded", "History may be off for this store"));
            }
            pp.add(&g);
            view.set_content(Some(&pp));
            d.set_child(Some(&view));
            d.present(Some(&u.window));
        });
    }

    fn confirm(&self, heading: &str, body: &str, verb: &str, then: impl Fn(&Ui) + 'static) {
        let d = adw::AlertDialog::new(Some(heading), Some(body));
        d.add_response("cancel", "Cancel");
        d.add_response("ok", verb);
        d.set_response_appearance("ok", adw::ResponseAppearance::Destructive);
        let u = self.clone();
        d.connect_response(Some("ok"), move |_, _| then(&u));
        d.present(Some(&self.window));
    }

    // }}}
}

// {{{ Small widgets

fn streamed(st: &api::Status, store: &str) -> bool {
    st.stores.iter().any(|x| x.name == store && x.sync == "stream")
}

fn row(title: &str, subtitle: &str) -> adw::ActionRow {
    adw::ActionRow::builder().title(glib::markup_escape_text(title)).subtitle(glib::markup_escape_text(subtitle)).build()
}

fn info(title: &str, value: &str) -> adw::ActionRow {
    let r = row(title, value);
    r.add_css_class("property");
    r.set_subtitle_selectable(true);
    r
}

fn flat(label: &str) -> gtk::Button {
    let b = gtk::Button::with_label(label);
    b.set_valign(gtk::Align::Center);
    b
}

fn open_button(path: &str, ui: &Ui) -> gtk::Button {
    let b = gtk::Button::from_icon_name("folder-open-symbolic");
    b.set_valign(gtk::Align::Center);
    b.add_css_class("flat");
    b.set_tooltip_text(Some("Open"));
    let (p, u) = (path.to_string(), ui.clone());
    b.connect_clicked(move |_| {
        let launcher = gtk::FileLauncher::new(Some(&gio::File::for_path(&p)));
        let u = u.clone();
        launcher.launch(Some(&u.window.clone()), gio::Cancellable::NONE, move |r| {
            if let Err(e) = r {
                u.toast(&e.to_string());
            }
        });
    });
    b
}

// }}}

// {{{ Keeping the view across a redraw
//
// A page is rebuilt when its data changes; without this, every redraw would
// throw the reader back to the top and close what they opened.

/// Where the reader is: the scroll position and which rows are open.
struct Kept {
    scroll: f64,
    open: Vec<String>,
}

fn kept(w: &gtk::Widget) -> Kept {
    let mut k = Kept { scroll: 0.0, open: vec![] };
    walk(w, &mut |c| {
        if let Some(s) = c.downcast_ref::<gtk::ScrolledWindow>() {
            if k.scroll == 0.0 {
                k.scroll = s.vadjustment().value();
            }
        }
        if let Some(r) = c.downcast_ref::<adw::ExpanderRow>() {
            if r.is_expanded() {
                k.open.push(r.title().to_string());
            }
        }
    });
    k
}

fn restore(w: &gtk::Widget, k: Kept) {
    let mut scroller = None;
    walk(w, &mut |c| {
        if let Some(r) = c.downcast_ref::<adw::ExpanderRow>() {
            if k.open.iter().any(|t| t.as_str() == r.title().as_str()) {
                r.set_expanded(true);
            }
        }
        if scroller.is_none() {
            scroller = c.downcast_ref::<gtk::ScrolledWindow>().cloned();
        }
    });
    let (Some(s), true) = (scroller, k.scroll > 0.0) else { return };
    // The new page has no height until it is laid out: wait for it.
    let adj = s.vadjustment();
    let done = std::rc::Rc::new(std::cell::Cell::new(false));
    let d = done.clone();
    let target = k.scroll;
    adj.connect_changed(move |a| {
        if !d.get() && a.upper() - a.page_size() >= target {
            a.set_value(target);
            d.set(true);
        }
    });
    glib::timeout_add_local_once(std::time::Duration::from_millis(400), move || done.set(true));
}

fn walk(w: &gtk::Widget, f: &mut impl FnMut(&gtk::Widget)) {
    f(w);
    let mut c = w.first_child();
    while let Some(ch) = c {
        walk(&ch, f);
        c = ch.next_sibling();
    }
}

// }}}
