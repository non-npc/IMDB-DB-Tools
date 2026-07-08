from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from app.db.schema import collection_schema_sql


class CollectionRepository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        self.ensure_schema(conn)
        return conn

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        for sql in collection_schema_sql():
            conn.execute(sql)
        conn.commit()

    def search(self, term: str = "", watched: bool | None = None, owned: bool | None = None) -> list[sqlite3.Row]:
        conn = self.connect()
        try:
            where = []
            params: list[object] = []
            if term:
                like = f"%{term}%"
                where.append("(ci.title LIKE ? OR ci.genres LIKE ? OR ci.description LIKE ? OR ci.notes LIKE ?)")
                params.extend([like, like, like, like])
            if watched is not None:
                where.append("ci.watched = ?")
                params.append(1 if watched else 0)
            if owned is not None:
                where.append("ci.owned = ?")
                params.append(1 if owned else 0)

            where_sql = f"WHERE {' AND '.join(where)}" if where else ""
            cursor = conn.execute(
                f"""
                SELECT ci.*
                FROM collection_items ci
                {where_sql}
                ORDER BY ci.date_added DESC, ci.title
                """,
                params,
            )
            return cursor.fetchall()
        finally:
            conn.close()

    def get_by_tconst(self, tconst: str) -> sqlite3.Row | None:
        conn = self.connect()
        try:
            return conn.execute("SELECT * FROM collection_items WHERE tconst = ?", (tconst,)).fetchone()
        finally:
            conn.close()

    def get_title_metadata(self, tconst: str) -> sqlite3.Row | None:
        conn = self.connect()
        try:
            return conn.execute("SELECT * FROM title_metadata WHERE tconst = ?", (tconst,)).fetchone()
        finally:
            conn.close()

    def has_description(self, tconst: str) -> bool:
        conn = self.connect()
        try:
            row = conn.execute(
                """
                SELECT 1
                FROM (
                    SELECT description FROM collection_items WHERE tconst = ?
                    UNION ALL
                    SELECT description FROM title_metadata WHERE tconst = ?
                )
                WHERE NULLIF(TRIM(description), '') IS NOT NULL
                LIMIT 1
                """,
                (tconst, tconst),
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def update_poster_path(self, tconst: str, poster_path: str) -> None:
        conn = self.connect()
        try:
            conn.execute(
                """
                INSERT INTO title_metadata (tconst, poster_path, date_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(tconst) DO UPDATE SET
                    poster_path = excluded.poster_path,
                    date_updated = CURRENT_TIMESTAMP
                """,
                (tconst, poster_path),
            )
            conn.execute(
                """
                UPDATE collection_items
                SET poster_path = ?, date_updated = CURRENT_TIMESTAMP
                WHERE tconst = ?
                """,
                (poster_path, tconst),
            )
            conn.commit()
        finally:
            conn.close()

    def update_description(self, tconst: str, description: str, source: str) -> None:
        conn = self.connect()
        try:
            conn.execute(
                """
                INSERT INTO title_metadata (tconst, description, description_source, date_updated)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(tconst) DO UPDATE SET
                    description = CASE
                        WHEN NULLIF(TRIM(title_metadata.description), '') IS NULL
                        THEN excluded.description
                        ELSE title_metadata.description
                    END,
                    description_source = CASE
                        WHEN NULLIF(TRIM(title_metadata.description_source), '') IS NULL
                        THEN excluded.description_source
                        ELSE title_metadata.description_source
                    END,
                    date_updated = CURRENT_TIMESTAMP
                """,
                (tconst, description, source),
            )
            conn.execute(
                """
                UPDATE collection_items
                SET description = ?, description_source = ?, date_updated = CURRENT_TIMESTAMP
                WHERE tconst = ?
                  AND NULLIF(TRIM(description), '') IS NULL
                """,
                (description, source, tconst),
            )
            conn.commit()
        finally:
            conn.close()

    def update_item_status(self, tconst: str, watched: bool, owned: bool) -> None:
        conn = self.connect()
        try:
            conn.execute(
                """
                UPDATE collection_items
                SET watched = ?, owned = ?, date_updated = CURRENT_TIMESTAMP
                WHERE tconst = ?
                """,
                (1 if watched else 0, 1 if owned else 0, tconst),
            )
            conn.commit()
        finally:
            conn.close()

    def remove_item(self, tconst: str) -> None:
        conn = self.connect()
        try:
            conn.execute("DELETE FROM collection_items WHERE tconst = ?", (tconst,))
            conn.commit()
        finally:
            conn.close()

    def upsert_item(self, item: dict, tags: Iterable[str] = ()) -> None:
        conn = self.connect()
        try:
            conn.execute(
                """
                INSERT INTO collection_items (
                    tconst, title, title_type, year, genres, rating, votes,
                    description, description_source, poster_path,
                    watched, owned, notes, date_updated
                )
                VALUES (
                    :tconst, :title, :title_type, :year, :genres, :rating, :votes,
                    :description, :description_source, :poster_path,
                    :watched, :owned, :notes, CURRENT_TIMESTAMP
                )
                ON CONFLICT(tconst) DO UPDATE SET
                    title = excluded.title,
                    title_type = excluded.title_type,
                    year = excluded.year,
                    genres = excluded.genres,
                    rating = excluded.rating,
                    votes = excluded.votes,
                    description = COALESCE(NULLIF(excluded.description, ''), collection_items.description),
                    description_source = COALESCE(NULLIF(excluded.description_source, ''), collection_items.description_source),
                    poster_path = COALESCE(NULLIF(excluded.poster_path, ''), collection_items.poster_path),
                    watched = excluded.watched,
                    owned = excluded.owned,
                    notes = excluded.notes,
                    date_updated = CURRENT_TIMESTAMP
                """,
                item,
            )
            item_id = conn.execute("SELECT id FROM collection_items WHERE tconst = ?", (item["tconst"],)).fetchone()[0]
            self.replace_tags(conn, item_id, tags)
            conn.commit()
        finally:
            conn.close()

    def replace_tags(self, conn: sqlite3.Connection, item_id: int, tags: Iterable[str]) -> None:
        conn.execute("DELETE FROM collection_item_tags WHERE item_id = ?", (item_id,))
        for tag in sorted({tag.strip() for tag in tags if tag.strip()}):
            conn.execute("INSERT OR IGNORE INTO collection_tags (name) VALUES (?)", (tag,))
            tag_id = conn.execute("SELECT id FROM collection_tags WHERE name = ?", (tag,)).fetchone()[0]
            conn.execute(
                "INSERT OR IGNORE INTO collection_item_tags (item_id, tag_id) VALUES (?, ?)",
                (item_id, tag_id),
            )
