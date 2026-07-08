from __future__ import annotations

import sys
from pathlib import Path

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QComboBox,
    QProgressBar,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
    QMessageBox,
)

from app.db.collection_repository import CollectionRepository
from app.db.imdb_repository import ImdbRepository
from app.services.settings import SettingsStore
from app.ui.settings_dialog import SettingsDialog
from app.ui.title_details import TitleDetailsDialog
from app.workers.description_worker import DescriptionFetchWorker
from app.workers.filter_worker import FilterWorker
from app.workers.poster_worker import PosterDownloadWorker
from app.workers.search_worker import SearchWorker


class BrowserWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings_store = SettingsStore()
        self.app_settings = self.settings_store.load()
        self.collection_repo = CollectionRepository(self.app_settings.collection_db_path)
        self.imdb_repo = ImdbRepository(self.app_settings.imdb_db_path)
        self.search_worker = None
        self.filter_worker = None
        self.poster_workers = {}
        self.description_workers = {}
        self.detail_dialogs = {}
        self.search_results = []
        self.collection_loaded_once = False

        self.setWindowTitle("IMDb Browser")
        self.resize(1200, 800)
        self.create_menus()
        self.tabs = QTabWidget()
        self.search_tab = self.create_search_tab()
        self.collection_tab = self.create_collection_tab()
        self.tabs.addTab(self.search_tab, "Search")
        self.tabs.addTab(self.collection_tab, "Collection")
        self.tabs.currentChanged.connect(self.handle_tab_changed)
        self.setCentralWidget(self.tabs)
        self.start_filter_load()

    def create_menus(self):
        file_menu = self.menuBar().addMenu("&File")
        exit_action = file_menu.addAction("E&xit")
        exit_action.triggered.connect(self.close)

        settings_menu = self.menuBar().addMenu("&Settings")
        description_action = settings_menu.addAction("&Description Sources...")
        description_action.triggered.connect(self.open_settings_dialog)

    def open_settings_dialog(self):
        dialog = SettingsDialog(self.app_settings, self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        self.app_settings.description = dialog.description_settings()
        self.settings_store.save(self.app_settings)
        self.search_status.setText("Settings saved")

    def create_search_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        controls = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search titles")
        self.search_input.returnPressed.connect(self.search_titles)
        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self.search_titles)
        self.search_button.setEnabled(False)
        self.add_to_collection_button = QPushButton("Add Selected")
        self.add_to_collection_button.clicked.connect(self.add_selected_to_collection)
        self.add_to_collection_button.setEnabled(False)
        controls.addWidget(self.search_input, 1)
        controls.addWidget(self.search_button)
        controls.addWidget(self.add_to_collection_button)

        filter_controls = QHBoxLayout()
        self.type_filter = QComboBox()
        self.type_filter.addItem("All Types", None)
        self.type_filter.setMinimumWidth(110)
        self.genre_filter = QComboBox()
        self.genre_filter.addItem("All Genres", None)
        self.genre_filter.setMinimumWidth(140)
        self.year_filter = QLineEdit()
        self.year_filter.setPlaceholderText("Year")
        self.year_filter.setFixedWidth(72)
        self.year_filter.setMaxLength(4)
        self.min_rating_filter = QComboBox()
        self.min_rating_filter.addItem("Any Rating", None)
        self.min_rating_filter.setMinimumWidth(110)
        for rating in range(1, 10):
            self.min_rating_filter.addItem(f"{rating}+", float(rating))
        self.refresh_filters_button = QPushButton("Refresh Filters")
        self.refresh_filters_button.clicked.connect(self.start_filter_load)

        filter_controls.addWidget(QLabel("Type:"))
        filter_controls.addWidget(self.type_filter, 0)
        filter_controls.addWidget(QLabel("Genre:"))
        filter_controls.addWidget(self.genre_filter, 0)
        filter_controls.addWidget(QLabel("Year:"))
        filter_controls.addWidget(self.year_filter, 0)
        filter_controls.addWidget(QLabel("Rating:"))
        filter_controls.addWidget(self.min_rating_filter, 0)
        filter_controls.addWidget(self.refresh_filters_button)
        filter_controls.addStretch(1)

        self.search_table = self.create_title_table(include_collection_flags=False)
        self.search_table.cellDoubleClicked.connect(lambda row, column: self.show_title_details(self.search_table, row))
        layout.addLayout(controls)
        layout.addLayout(filter_controls)
        layout.addWidget(self.search_table)
        self.search_status = QLabel("Ready")
        self.loading_progress = QProgressBar()
        self.loading_progress.setRange(0, 0)
        self.loading_progress.setVisible(False)
        layout.addWidget(self.search_status)
        layout.addWidget(self.loading_progress)
        return tab

    def create_collection_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        controls = QHBoxLayout()
        self.collection_search_input = QLineEdit()
        self.collection_search_input.setPlaceholderText("Search collection")
        self.collection_refresh_button = QPushButton("Refresh")
        self.collection_refresh_button.clicked.connect(self.load_collection)
        self.collection_remove_button = QPushButton("Remove Selected")
        self.collection_remove_button.clicked.connect(self.remove_selected_from_collection)
        self.watched_filter = QCheckBox("Watched")
        self.owned_filter = QCheckBox("Owned")
        controls.addWidget(self.collection_search_input, 1)
        controls.addWidget(self.watched_filter)
        controls.addWidget(self.owned_filter)
        controls.addWidget(self.collection_refresh_button)
        controls.addWidget(self.collection_remove_button)

        self.collection_table = self.create_title_table(include_collection_flags=True)
        self.collection_table.cellDoubleClicked.connect(lambda row, column: self.show_title_details(self.collection_table, row))
        layout.addLayout(controls)
        layout.addWidget(self.collection_table)
        self.collection_status = QLabel("Collection not loaded")
        layout.addWidget(self.collection_status)
        return tab

    def create_title_table(self, include_collection_flags: bool):
        table = QTableWidget()
        headers = ["Poster", "Title", "Type", "Year", "Rating", "Genres"]
        if include_collection_flags:
            headers.extend(["Watched", "Owned"])
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.verticalHeader().setDefaultSectionSize(96)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        return table

    def search_titles(self):
        if self.search_worker and self.search_worker.isRunning():
            self.search_status.setText("Cancelling search...")
            self.search_worker.terminate()
            self.search_worker.wait(1000)
            self.search_worker = None
            self.search_button.setText("Search")
            return

        term = self.search_input.text().strip()
        year_text = self.year_filter.text().strip()
        if year_text and not year_text.isdigit():
            QMessageBox.warning(self, "Invalid Year", "Enter a single numeric year, such as 2020.")
            return

        self.search_table.setRowCount(0)
        self.search_results = []
        self.search_status.setText("Searching...")
        self.search_button.setText("Cancel")
        self.search_button.setEnabled(True)

        self.search_worker = SearchWorker(
            db_path=self.app_settings.imdb_db_path,
            term=term,
            title_type=self.type_filter.currentData(),
            genre=self.genre_filter.currentData(),
            year=int(year_text) if year_text else None,
            min_rating=self.min_rating_filter.currentData(),
        )
        self.search_worker.results_ready.connect(self.handle_search_results)
        self.search_worker.failed.connect(self.handle_search_error)
        self.search_worker.finished.connect(self.handle_search_finished)
        self.search_worker.start()

    def handle_search_results(self, rows: list[dict]):
        self.search_results = rows
        self.search_table.setRowCount(0)
        for row_index, row in enumerate(rows):
            self.search_table.insertRow(row_index)
            self.populate_title_row(self.search_table, row_index, row)
        self.search_status.setText(f"{len(rows)} title(s) found")

    def handle_search_error(self, message: str):
        self.search_status.setText("Search failed")
        QMessageBox.warning(self, "Search Error", message)

    def handle_search_finished(self):
        self.search_button.setText("Search")
        self.search_worker = None

    def restore_combo_value(self, combo: QComboBox, value):
        if value is None:
            combo.setCurrentIndex(0)
            return
        index = combo.findData(value)
        if index >= 0:
            combo.setCurrentIndex(index)

    def populate_title_row(self, table: QTableWidget, row_index: int, row: dict, watched: bool = False, owned: bool = False):
        tconst = row.get("tconst") or ""
        title = row.get("primaryTitle") or row.get("title") or ""
        title_type = row.get("titleType") or row.get("title_type") or ""
        year = row.get("startYear") if "startYear" in row else row.get("year")
        genres = row.get("genres") or ""
        rating = row.get("averageRating") if "averageRating" in row else row.get("rating")
        votes = row.get("numVotes") if "numVotes" in row else row.get("votes")

        id_item = QTableWidgetItem("")
        id_item.setData(Qt.ItemDataRole.UserRole, tconst)
        table.setItem(row_index, 0, id_item)
        self.set_poster_cell(table, row_index, tconst)

        title_item = QTableWidgetItem(title)
        title_item.setData(Qt.ItemDataRole.UserRole, tconst)
        table.setItem(row_index, 1, title_item)
        table.setItem(row_index, 2, QTableWidgetItem(title_type))
        table.setItem(row_index, 3, QTableWidgetItem(str(year or "")))
        rating_text = f"{float(rating):.1f} ({int(votes):,})" if rating is not None and votes is not None else ""
        table.setItem(row_index, 4, QTableWidgetItem(rating_text))
        table.setItem(row_index, 5, QTableWidgetItem(genres))
        if table.columnCount() > 6:
            self.set_collection_status_cells(table, row_index, tconst, watched, owned)
        table.resizeColumnsToContents()

    def set_collection_status_cells(
        self,
        table: QTableWidget,
        row_index: int,
        tconst: str,
        watched: bool,
        owned: bool,
    ):
        watched_checkbox = QCheckBox()
        watched_checkbox.setChecked(watched)
        watched_checkbox.setProperty("tconst", tconst)
        watched_checkbox.stateChanged.connect(self.collection_status_changed)
        owned_checkbox = QCheckBox()
        owned_checkbox.setChecked(owned)
        owned_checkbox.setProperty("tconst", tconst)
        owned_checkbox.stateChanged.connect(self.collection_status_changed)
        table.setCellWidget(row_index, 6, self.centered_widget(watched_checkbox))
        table.setCellWidget(row_index, 7, self.centered_widget(owned_checkbox))

    def centered_widget(self, widget: QWidget) -> QWidget:
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addStretch(1)
        layout.addWidget(widget)
        layout.addStretch(1)
        return container

    def collection_status_changed(self):
        checkbox = self.sender()
        if not checkbox:
            return
        tconst = checkbox.property("tconst")
        if not tconst:
            return

        for row_index in range(self.collection_table.rowCount()):
            item = self.collection_table.item(row_index, 1) or self.collection_table.item(row_index, 0)
            if item and item.data(Qt.ItemDataRole.UserRole) == tconst:
                watched = self.checkbox_in_cell(self.collection_table, row_index, 6).isChecked()
                owned = self.checkbox_in_cell(self.collection_table, row_index, 7).isChecked()
                self.collection_repo.update_item_status(tconst, watched, owned)
                self.collection_status.setText(f"Saved status for {item.text()}")
                return

    def checkbox_in_cell(self, table: QTableWidget, row_index: int, column_index: int) -> QCheckBox:
        container = table.cellWidget(row_index, column_index)
        return container.findChild(QCheckBox) if container else QCheckBox()

    def set_poster_cell(self, table: QTableWidget, row_index: int, tconst: str, downloading: bool = False):
        poster_path = self.local_poster_path(tconst)
        if poster_path:
            label = QLabel()
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            pixmap = QPixmap(str(poster_path))
            if not pixmap.isNull():
                label.setPixmap(
                    pixmap.scaled(64, 90, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                )
                table.setCellWidget(row_index, 0, label)
                return

        button = QPushButton("Downloading..." if downloading else "Get Poster")
        button.setEnabled(not downloading)
        button.setProperty("tconst", tconst)
        button.clicked.connect(self.download_poster_from_button)
        table.setCellWidget(row_index, 0, button)

    def image_dir(self) -> Path:
        path = Path(self.app_settings.image_dir)
        return path if path.is_absolute() else Path.cwd() / path

    def local_poster_path(self, tconst: str) -> Path | None:
        for suffix in (".jpg", ".jpeg", ".png", ".webp"):
            path = self.image_dir() / f"{tconst}{suffix}"
            if path.exists() and path.stat().st_size > 0:
                return path
        return None

    def selected_search_tconst(self) -> str | None:
        selected = self.search_table.selectedRanges()
        if not selected:
            return None
        row = selected[0].topRow()
        item = self.search_table.item(row, 1) or self.search_table.item(row, 0)
        return item.data(Qt.ItemDataRole.UserRole) if item else None

    def selected_collection_tconst(self) -> str | None:
        selected = self.collection_table.selectedRanges()
        if not selected:
            return None
        row = selected[0].topRow()
        item = self.collection_table.item(row, 1) or self.collection_table.item(row, 0)
        return item.data(Qt.ItemDataRole.UserRole) if item else None

    def add_selected_to_collection(self):
        tconst = self.selected_search_tconst()
        if not tconst:
            QMessageBox.information(self, "Collection", "Select a title first.")
            return

        title = self.imdb_repo.get_title_for_collection(tconst)
        if not title:
            QMessageBox.warning(self, "Collection", f"Could not load title {tconst}.")
            return

        self.collection_repo.upsert_item(
            {
                "tconst": title["tconst"],
                "title": title["primaryTitle"],
                "title_type": title["titleType"],
                "year": title["startYear"],
                "genres": title["genres"],
                "rating": title["averageRating"],
                "votes": title["numVotes"],
                "description": "",
                "description_source": "",
                "poster_path": "",
                "watched": 0,
                "owned": 0,
                "notes": "",
            }
        )
        self.collection_loaded_once = True
        self.load_collection()
        QMessageBox.information(self, "Collection", "Added to collection")

    def remove_selected_from_collection(self):
        tconst = self.selected_collection_tconst()
        if not tconst:
            QMessageBox.information(self, "Collection", "Select a collection item first.")
            return

        title_item = self.collection_table.item(self.collection_table.selectedRanges()[0].topRow(), 1)
        title = title_item.text() if title_item else tconst
        choice = QMessageBox.question(
            self,
            "Remove from Collection",
            f"Remove \"{title}\" from your collection?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if choice != QMessageBox.StandardButton.Yes:
            return

        self.collection_repo.remove_item(tconst)
        self.load_collection()
        QMessageBox.information(self, "Collection", "Removed from collection")

    def show_title_details(self, table: QTableWidget, row: int):
        item = table.item(row, 1) or table.item(row, 0)
        if not item:
            return
        tconst = item.data(Qt.ItemDataRole.UserRole)
        collection_row = self.collection_repo.get_by_tconst(tconst)
        collection_match = dict(collection_row) if collection_row else None
        metadata_row = self.collection_repo.get_title_metadata(tconst)
        metadata_match = dict(metadata_row) if metadata_row else None
        details = self.imdb_repo.get_title_details(tconst)
        if not details:
            if not collection_match:
                QMessageBox.warning(self, "Title", f"Could not load title details for {tconst}.")
                return
            details = {
                "tconst": tconst,
                "primaryTitle": collection_match.get("title"),
                "titleType": collection_match.get("title_type"),
                "startYear": collection_match.get("year"),
                "genres": collection_match.get("genres"),
                "averageRating": collection_match.get("rating"),
                "numVotes": collection_match.get("votes"),
                "cast": [],
                "akas": [],
                "episodes": [],
            }
        if collection_match:
            details.update(
                {
                    "description": collection_match.get("description"),
                    "poster_path": collection_match.get("poster_path"),
                    "watched": collection_match.get("watched"),
                    "owned": collection_match.get("owned"),
                    "notes": collection_match.get("notes"),
                }
            )
        if metadata_match:
            if not self.has_text(details.get("description")):
                details["description"] = metadata_match.get("description")
                details["description_source"] = metadata_match.get("description_source")
            if not self.local_poster_path(tconst) and metadata_match.get("poster_path"):
                details["poster_path"] = metadata_match.get("poster_path")

        dialog = TitleDetailsDialog(details, self.local_poster_path(tconst), self.download_poster, self)
        self.detail_dialogs.setdefault(tconst, []).append(dialog)
        dialog.finished.connect(lambda _result, title_id=tconst, win=dialog: self.forget_detail_dialog(title_id, win))
        dialog.show()
        if not self.has_text(details.get("description")):
            self.fetch_missing_description(tconst, details)

    def forget_detail_dialog(self, tconst: str, dialog: TitleDetailsDialog):
        dialogs = self.detail_dialogs.get(tconst, [])
        if dialog in dialogs:
            dialogs.remove(dialog)
        if not dialogs:
            self.detail_dialogs.pop(tconst, None)

    def download_poster_from_button(self):
        sender = self.sender()
        if sender:
            self.download_poster(sender.property("tconst"))

    def fetch_missing_description(self, tconst: str, details: dict):
        if tconst in self.description_workers:
            return
        if self.has_text(details.get("description")) or self.collection_repo.has_description(tconst):
            return
        if not self.app_settings.description.tmdb_enabled and not self.app_settings.description.imdb_scrape_enabled:
            return

        for dialog in self.detail_dialogs.get(tconst, []):
            dialog.set_description_loading()
        self.search_status.setText(f"Fetching description for {tconst}...")

        year = details.get("startYear") or details.get("year")
        try:
            year = int(year) if year not in (None, "", "\\N") else None
        except ValueError:
            year = None

        worker = DescriptionFetchWorker(
            self.app_settings.description,
            tconst,
            details.get("primaryTitle") or details.get("title") or "",
            year,
            details.get("titleType") or details.get("title_type"),
        )
        self.description_workers[tconst] = worker
        worker.fetched.connect(self.handle_description_fetched)
        worker.failed.connect(self.handle_description_failed)
        worker.finished.connect(lambda title_id=tconst: self.description_workers.pop(title_id, None))
        worker.start()

    def handle_description_fetched(self, tconst: str, description: str, source: str):
        self.collection_repo.update_description(tconst, description, source)
        for dialog in self.detail_dialogs.get(tconst, []):
            dialog.set_description(description, source)
        self.search_status.setText(f"Description fetched for {tconst}")

    def handle_description_failed(self, tconst: str, message: str):
        for dialog in self.detail_dialogs.get(tconst, []):
            dialog.set_description("", "")
        self.search_status.setText(f"Description not found for {tconst}: {message}")

    def has_text(self, value) -> bool:
        return bool(str(value or "").strip())

    def download_poster(self, tconst: str):
        if not tconst:
            return
        existing = self.local_poster_path(tconst)
        if existing:
            self.apply_downloaded_poster(tconst, str(existing))
            return
        if tconst in self.poster_workers:
            self.search_status.setText(f"Poster download already running for {tconst}")
            return

        self.show_poster_downloading(tconst)
        self.search_status.setText(f"Downloading poster for {tconst}...")
        worker = PosterDownloadWorker(str(self.image_dir()), tconst)
        self.poster_workers[tconst] = worker
        worker.downloaded.connect(self.handle_poster_downloaded)
        worker.failed.connect(self.handle_poster_failed)
        worker.finished.connect(lambda title_id=tconst: self.poster_workers.pop(title_id, None))
        worker.start()

    def handle_poster_downloaded(self, tconst: str, path: str):
        self.apply_downloaded_poster(tconst, path)
        self.search_status.setText(f"Poster downloaded for {tconst}")

    def handle_poster_failed(self, tconst: str, message: str):
        self.search_status.setText(f"Poster failed for {tconst}")
        self.show_poster_idle(tconst)
        for dialog in self.detail_dialogs.get(tconst, []):
            dialog.set_poster_failed()
        QMessageBox.warning(self, "Poster Download", f"Failed to download poster for {tconst}: {message}")

    def apply_downloaded_poster(self, tconst: str, path: str):
        self.collection_repo.update_poster_path(tconst, path)
        self.update_poster_cells(self.search_table, tconst)
        self.update_poster_cells(self.collection_table, tconst)
        for dialog in self.detail_dialogs.get(tconst, []):
            dialog.set_poster(path)

    def show_poster_downloading(self, tconst: str):
        self.update_poster_cells(self.search_table, tconst, downloading=True)
        self.update_poster_cells(self.collection_table, tconst, downloading=True)
        for dialog in self.detail_dialogs.get(tconst, []):
            dialog.set_poster_downloading()

    def show_poster_idle(self, tconst: str):
        self.update_poster_cells(self.search_table, tconst)
        self.update_poster_cells(self.collection_table, tconst)

    def update_poster_cells(self, table: QTableWidget, tconst: str, downloading: bool = False):
        for row_index in range(table.rowCount()):
            item = table.item(row_index, 0) or table.item(row_index, 1)
            if item and item.data(Qt.ItemDataRole.UserRole) == tconst:
                self.set_poster_cell(table, row_index, tconst, downloading=downloading)

    def load_collection(self):
        rows = self.collection_repo.search(
            self.collection_search_input.text().strip(),
            watched=True if self.watched_filter.isChecked() else None,
            owned=True if self.owned_filter.isChecked() else None,
        )
        self.collection_table.setRowCount(0)
        for row_index, row in enumerate(rows):
            self.collection_table.insertRow(row_index)
            self.populate_title_row(
                self.collection_table,
                row_index,
                dict(row),
                watched=bool(row["watched"]),
                owned=bool(row["owned"]),
            )
        self.collection_status.setText(f"{len(rows)} collection item(s)")

    def set_search_controls_enabled(self, enabled: bool):
        self.search_input.setEnabled(enabled)
        self.search_button.setEnabled(enabled)
        self.add_to_collection_button.setEnabled(enabled)
        self.type_filter.setEnabled(enabled)
        self.genre_filter.setEnabled(enabled)
        self.year_filter.setEnabled(enabled)
        self.min_rating_filter.setEnabled(enabled)
        self.refresh_filters_button.setEnabled(enabled)

    def start_filter_load(self):
        if self.filter_worker and self.filter_worker.isRunning():
            return

        self.set_search_controls_enabled(False)
        self.search_status.setText("Loading title types and genres...")
        self.loading_progress.setVisible(True)
        self.filter_worker = FilterWorker(self.app_settings.imdb_db_path)
        self.filter_worker.filters_ready.connect(self.handle_filters_ready)
        self.filter_worker.failed.connect(self.handle_filters_failed)
        self.filter_worker.finished.connect(self.handle_filters_finished)
        self.filter_worker.start()

    def handle_filters_ready(self, title_types: list[str], genres: list[str]):
        current_type = self.type_filter.currentData()
        current_genre = self.genre_filter.currentData()

        self.type_filter.clear()
        self.genre_filter.clear()
        self.type_filter.addItem("All Types", None)
        self.genre_filter.addItem("All Genres", None)

        for title_type in title_types:
            self.type_filter.addItem(title_type, title_type)
        for genre in genres:
            self.genre_filter.addItem(genre, genre)

        self.restore_combo_value(self.type_filter, current_type)
        self.restore_combo_value(self.genre_filter, current_genre)
        self.search_status.setText("Ready")

    def handle_filters_failed(self, message: str):
        self.search_status.setText(f"Filter loading failed: {message}")

    def handle_filters_finished(self):
        self.loading_progress.setVisible(False)
        self.set_search_controls_enabled(True)
        self.filter_worker = None

    def handle_tab_changed(self, index: int):
        if self.tabs.widget(index) is self.collection_tab and not self.collection_loaded_once:
            self.load_collection()
            self.collection_loaded_once = True


def run_browser():
    app = QApplication(sys.argv)
    window = BrowserWindow()
    window.show()
    sys.exit(app.exec())
