from __future__ import annotations

from PyQt6.QtCore import QThread, pyqtSignal

from app.db.imdb_repository import ImdbRepository


class SearchWorker(QThread):
    results_ready = pyqtSignal(list)
    failed = pyqtSignal(str)

    def __init__(
        self,
        db_path: str,
        term: str,
        title_type: str | None = None,
        genre: str | None = None,
        year: int | None = None,
        min_rating: float | None = None,
        limit: int = 250,
    ):
        super().__init__()
        self.db_path = db_path
        self.term = term
        self.title_type = title_type
        self.genre = genre
        self.year = year
        self.min_rating = min_rating
        self.limit = limit

    def run(self):
        try:
            repo = ImdbRepository(self.db_path)
            rows = repo.search_titles(
                term=self.term,
                title_type=self.title_type,
                genre=self.genre,
                year=self.year,
                min_rating=self.min_rating,
                limit=self.limit,
            )
            self.results_ready.emit([dict(row) for row in rows])
        except Exception as exc:
            self.failed.emit(str(exc))
