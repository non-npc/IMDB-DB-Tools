from __future__ import annotations

from PyQt6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
    QVBoxLayout,
)

from app.services.settings import AppSettings, DescriptionSettings


class SettingsDialog(QDialog):
    def __init__(self, app_settings: AppSettings, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.resize(520, 220)

        self.tmdb_enabled = QCheckBox("Use TMDB for missing descriptions")
        self.tmdb_enabled.setChecked(app_settings.description.tmdb_enabled)

        self.tmdb_api_key = QLineEdit(app_settings.description.tmdb_api_key)
        self.tmdb_api_key.setPlaceholderText("TMDB API key")

        self.tmdb_read_token = QLineEdit(app_settings.description.tmdb_read_token)
        self.tmdb_read_token.setPlaceholderText("TMDB read access token")
        self.tmdb_read_token.setEchoMode(QLineEdit.EchoMode.Password)

        self.imdb_scrape_enabled = QCheckBox("Use IMDb scraping for missing descriptions")
        self.imdb_scrape_enabled.setChecked(app_settings.description.imdb_scrape_enabled)

        form = QFormLayout()
        form.addRow("", self.tmdb_enabled)
        form.addRow("TMDB API key:", self.tmdb_api_key)
        form.addRow("TMDB read token:", self.tmdb_read_token)
        form.addRow("", self.imdb_scrape_enabled)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(buttons)

    def description_settings(self) -> DescriptionSettings:
        return DescriptionSettings(
            tmdb_enabled=self.tmdb_enabled.isChecked(),
            tmdb_api_key=self.tmdb_api_key.text().strip(),
            tmdb_read_token=self.tmdb_read_token.text().strip(),
            imdb_scrape_enabled=self.imdb_scrape_enabled.isChecked(),
        )
