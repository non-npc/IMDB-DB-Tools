from __future__ import annotations

from PyQt6.QtCore import QThread, pyqtSignal

from app.db.imdb_repository import ImdbRepository


class FilterWorker(QThread):
    filters_ready = pyqtSignal(list, list)
    failed = pyqtSignal(str)

    def __init__(self, db_path: str):
        super().__init__()
        self.db_path = db_path

    def run(self):
        try:
            repo = ImdbRepository(self.db_path)
            self.filters_ready.emit(repo.title_types(), repo.genres())
        except Exception as exc:
            self.failed.emit(str(exc))

