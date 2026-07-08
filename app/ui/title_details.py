from __future__ import annotations

from pathlib import Path
from typing import Callable

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class TitleDetailsDialog(QDialog):
    def __init__(
        self,
        details: dict,
        poster_path: str | Path | None,
        download_poster: Callable[[str], None],
        parent=None,
    ):
        super().__init__(parent)
        self.details = details
        self.tconst = details.get("tconst", "")
        self.download_poster = download_poster

        title = details.get("primaryTitle") or details.get("title") or self.tconst
        self.setWindowTitle(title)
        self.resize(900, 620)

        layout = QHBoxLayout(self)
        poster_layout = QVBoxLayout()
        self.poster_label = QLabel("No poster")
        self.poster_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.poster_label.setFixedSize(190, 285)
        self.poster_label.setStyleSheet("border: 1px solid #999; background: #f5f5f5;")
        self.poster_button = QPushButton("Download Poster")
        self.poster_button.clicked.connect(self.start_poster_download)
        poster_layout.addWidget(self.poster_label)
        poster_layout.addWidget(self.poster_button)
        poster_layout.addStretch(1)

        content_layout = QVBoxLayout()
        heading = QLabel(title)
        heading.setStyleSheet("font-size: 18px; font-weight: bold;")
        content_layout.addWidget(heading)
        content_layout.addWidget(self.summary_text())

        tabs = QTabWidget()
        self.details_text = self.text_tab(self.description_text())
        tabs.addTab(self.details_text, "Details")
        tabs.addTab(self.table_tab(details.get("cast", []), ["Name", "Category", "Job", "Characters"]), "Cast")
        tabs.addTab(self.table_tab(details.get("akas", []), ["Title", "Region", "Language", "Types", "Original"]), "AKA")
        tabs.addTab(self.table_tab(details.get("episodes", []), ["Season", "Episode", "Title", "Year"]), "Episodes")
        content_layout.addWidget(tabs, 1)

        layout.addLayout(poster_layout)
        layout.addLayout(content_layout, 1)
        self.set_poster(poster_path)

    def start_poster_download(self):
        self.set_poster_downloading()
        self.download_poster(self.tconst)

    def summary_text(self) -> QLabel:
        parts = [
            self.details.get("titleType") or self.details.get("title_type"),
            str(self.details.get("startYear") or self.details.get("year") or ""),
            self.details.get("genres"),
        ]
        rating = self.details.get("averageRating") or self.details.get("rating")
        votes = self.details.get("numVotes") or self.details.get("votes")
        if rating is not None:
            vote_text = f" ({int(votes):,} votes)" if votes is not None else ""
            parts.append(f"{float(rating):.1f}{vote_text}")
        runtime = self.details.get("runtimeMinutes")
        if runtime not in (None, "", "\\N"):
            parts.append(f"{runtime} min")
        label = QLabel(" | ".join(str(part) for part in parts if part and part != "\\N"))
        label.setWordWrap(True)
        return label

    def description_text(self) -> str:
        description = self.details.get("description") or ""
        rows = [
            ("IMDb ID", self.tconst),
            ("Original Title", self.details.get("originalTitle")),
            ("End Year", self.details.get("endYear")),
        ]
        text = description.strip()
        if text:
            text += "\n\n"
        text += "\n".join(f"{label}: {value}" for label, value in rows if value not in (None, "", "\\N"))
        return text

    def text_tab(self, text: str) -> QTextEdit:
        widget = QTextEdit()
        widget.setReadOnly(True)
        widget.setPlainText(text)
        return widget

    def table_tab(self, rows: list[dict], headers: list[str]) -> QWidget:
        table = QTableWidget()
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(rows))
        key_map = {
            "Name": "primaryName",
            "Category": "category",
            "Job": "job",
            "Characters": "characters",
            "Title": "title",
            "Region": "region",
            "Language": "language",
            "Types": "types",
            "Original": "isOriginalTitle",
            "Season": "seasonNumber",
            "Episode": "episodeNumber",
            "Year": "startYear",
        }
        for row_index, row in enumerate(rows):
            for col_index, header in enumerate(headers):
                table.setItem(row_index, col_index, QTableWidgetItem(str(row.get(key_map[header]) or "")))
        table.resizeColumnsToContents()
        return table

    def set_poster(self, poster_path: str | Path | None):
        if not poster_path:
            self.poster_label.setText("No poster")
            self.poster_button.setText("Download Poster")
            self.poster_button.setEnabled(True)
            return
        pixmap = QPixmap(str(poster_path))
        if pixmap.isNull():
            self.poster_label.setText("No poster")
            self.poster_button.setText("Download Poster")
            self.poster_button.setEnabled(True)
            return
        self.poster_label.setPixmap(
            pixmap.scaled(
                self.poster_label.size(),
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
        )
        self.poster_button.setText("Poster Downloaded")
        self.poster_button.setEnabled(False)

    def set_poster_downloading(self):
        self.poster_label.setText("Downloading poster...")
        self.poster_button.setText("Downloading...")
        self.poster_button.setEnabled(False)

    def set_poster_failed(self):
        self.poster_label.setText("No poster")
        self.poster_button.setText("Try Again")
        self.poster_button.setEnabled(True)

    def set_description_loading(self):
        self.details["description"] = "Loading description..."
        self.details_text.setPlainText(self.description_text())

    def set_description(self, description: str, source: str):
        self.details["description"] = description
        self.details["description_source"] = source
        self.details_text.setPlainText(self.description_text())
