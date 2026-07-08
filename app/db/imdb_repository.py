from __future__ import annotations

import sqlite3
from pathlib import Path


class ImdbRepository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def exists(self) -> bool:
        return self.db_path.exists()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def available_tables(self) -> set[str]:
        if not self.exists():
            return set()
        conn = self.connect()
        try:
            rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            return {row["name"] for row in rows}
        finally:
            conn.close()

    def title_types(self) -> list[str]:
        if "title_basics" not in self.available_tables():
            return []
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT DISTINCT titleType FROM title_basics WHERE titleType IS NOT NULL ORDER BY titleType"
            ).fetchall()
            return [row["titleType"] for row in rows]
        finally:
            conn.close()

    def genres(self) -> list[str]:
        if "title_basics" not in self.available_tables():
            return []
        conn = self.connect()
        try:
            rows = conn.execute("SELECT DISTINCT genres FROM title_basics WHERE genres IS NOT NULL").fetchall()
            values = set()
            for row in rows:
                for genre in (row["genres"] or "").split(","):
                    if genre and genre != "\\N":
                        values.add(genre)
            return sorted(values)
        finally:
            conn.close()

    def search_titles(
        self,
        term: str,
        title_type: str | None = None,
        genre: str | None = None,
        year: int | None = None,
        min_rating: float | None = None,
        limit: int = 250,
    ) -> list[sqlite3.Row]:
        tables = self.available_tables()
        if "title_basics" not in tables:
            raise RuntimeError(f"IMDb database not found or missing title_basics: {self.db_path}")

        has_ratings = "title_ratings" in tables
        rating_select = "tr.averageRating, tr.numVotes" if has_ratings else "NULL AS averageRating, NULL AS numVotes"
        rating_join = "LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst" if has_ratings else ""

        where = []
        params: list[object] = []
        if term:
            where.append("(tb.primaryTitle LIKE ? OR tb.originalTitle LIKE ?)")
            like_term = f"%{term}%"
            params.extend([like_term, like_term])
        if title_type:
            where.append("tb.titleType = ?")
            params.append(title_type)
        if genre:
            where.append("tb.genres LIKE ?")
            params.append(f"%{genre}%")
        if year is not None:
            where.append("tb.startYear = ?")
            params.append(year)
        if min_rating is not None:
            if not has_ratings:
                raise RuntimeError("Rating filter requires title.ratings.tsv.gz to be imported.")
            where.append("tr.averageRating >= ?")
            params.append(min_rating)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        params.append(limit)

        conn = self.connect()
        try:
            return conn.execute(
                f"""
                SELECT tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle,
                       tb.startYear, tb.endYear, tb.runtimeMinutes, tb.genres,
                       {rating_select}
                FROM title_basics tb
                {rating_join}
                {where_sql}
                ORDER BY
                    CASE WHEN tb.startYear IS NULL THEN 0 ELSE tb.startYear END DESC,
                    tb.primaryTitle
                LIMIT ?
                """,
                params,
            ).fetchall()
        finally:
            conn.close()

    def get_title_for_collection(self, tconst: str) -> dict | None:
        tables = self.available_tables()
        if "title_basics" not in tables:
            return None

        has_ratings = "title_ratings" in tables
        rating_select = "tr.averageRating, tr.numVotes" if has_ratings else "NULL AS averageRating, NULL AS numVotes"
        rating_join = "LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst" if has_ratings else ""

        conn = self.connect()
        try:
            row = conn.execute(
                f"""
                SELECT tb.tconst, tb.titleType, tb.primaryTitle, tb.startYear,
                       tb.genres, {rating_select}
                FROM title_basics tb
                {rating_join}
                WHERE tb.tconst = ?
                """,
                (tconst,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_title_details(self, tconst: str) -> dict | None:
        tables = self.available_tables()
        if "title_basics" not in tables:
            return None

        has_ratings = "title_ratings" in tables
        rating_select = "tr.averageRating, tr.numVotes" if has_ratings else "NULL AS averageRating, NULL AS numVotes"
        rating_join = "LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst" if has_ratings else ""

        conn = self.connect()
        try:
            row = conn.execute(
                f"""
                SELECT tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle,
                       tb.isAdult, tb.startYear, tb.endYear, tb.runtimeMinutes,
                       tb.genres, {rating_select}
                FROM title_basics tb
                {rating_join}
                WHERE tb.tconst = ?
                """,
                (tconst,),
            ).fetchone()
            if not row:
                return None

            details = dict(row)
            details["cast"] = []
            details["akas"] = []
            details["episodes"] = []

            if "title_principals" in tables:
                name_join = "LEFT JOIN name_basics nb ON tp.nconst = nb.nconst" if "name_basics" in tables else ""
                name_select = "nb.primaryName" if "name_basics" in tables else "tp.nconst AS primaryName"
                details["cast"] = [
                    dict(cast_row)
                    for cast_row in conn.execute(
                        f"""
                        SELECT {name_select}, tp.category, tp.job, tp.characters
                        FROM title_principals tp
                        {name_join}
                        WHERE tp.tconst = ?
                        ORDER BY tp.ordering
                        LIMIT 50
                        """,
                        (tconst,),
                    ).fetchall()
                ]

            if "title_akas" in tables:
                details["akas"] = [
                    dict(aka_row)
                    for aka_row in conn.execute(
                        """
                        SELECT title, region, language, types, isOriginalTitle
                        FROM title_akas
                        WHERE titleId = ?
                        ORDER BY ordering
                        LIMIT 100
                        """,
                        (tconst,),
                    ).fetchall()
                ]

            if "title_episode" in tables:
                details["episodes"] = [
                    dict(episode_row)
                    for episode_row in conn.execute(
                        """
                        SELECT te.seasonNumber, te.episodeNumber, tb.primaryTitle AS title, tb.startYear
                        FROM title_episode te
                        JOIN title_basics tb ON tb.tconst = te.tconst
                        WHERE te.parentTconst = ?
                        ORDER BY
                            CASE WHEN te.seasonNumber IS NULL THEN 999999 ELSE te.seasonNumber END,
                            CASE WHEN te.episodeNumber IS NULL THEN 999999 ELSE te.episodeNumber END
                        LIMIT 250
                        """,
                        (tconst,),
                    ).fetchall()
                ]

            return details
        finally:
            conn.close()
