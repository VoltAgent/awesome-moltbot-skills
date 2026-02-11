"""
SQLite database manager for the Marketing Research Agent.
Handles all CRUD operations for papers, analyses, patterns, ideas, and datasets.
"""

import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import config

logger = logging.getLogger(__name__)


class Database:
    """SQLite-backed knowledge database for marketing research papers."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or config.DB_PATH
        self.conn: Optional[sqlite3.Connection] = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        logger.info("Connected to database: %s", self.db_path)

    def _create_tables(self):
        """Create all tables if they don't exist."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS papers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doi TEXT UNIQUE,
                title TEXT NOT NULL,
                authors TEXT,              -- JSON list of author names
                journal_code TEXT NOT NULL, -- JM, JMR, MKSC, JCR, JAMS, IJRM
                journal_name TEXT,
                year INTEGER,
                month INTEGER,
                volume TEXT,
                issue TEXT,
                pages TEXT,
                abstract TEXT,
                keywords TEXT,             -- JSON list
                citation_count INTEGER DEFAULT 0,
                reference_count INTEGER DEFAULT 0,
                pdf_path TEXT,
                full_text TEXT,
                url TEXT,
                openalex_id TEXT,
                crossref_fetched INTEGER DEFAULT 0,
                openalex_fetched INTEGER DEFAULT 0,
                pdf_downloaded INTEGER DEFAULT 0,
                text_extracted INTEGER DEFAULT 0,
                analyzed INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                paper_id INTEGER NOT NULL UNIQUE,
                methodology_types TEXT,        -- JSON list (experiment, survey, etc.)
                data_type TEXT,                 -- primary, secondary, both
                data_sources TEXT,              -- JSON list of data source descriptions
                sample_description TEXT,
                sample_size TEXT,
                analytical_techniques TEXT,     -- JSON list
                theoretical_frameworks TEXT,    -- JSON list
                key_contributions TEXT,         -- JSON list
                key_findings TEXT,
                research_domain TEXT,           -- e.g., consumer behavior, pricing, etc.
                publishability_factors TEXT,    -- JSON dict of factor -> score
                novelty_assessment TEXT,
                practical_implications TEXT,
                limitations TEXT,
                future_directions TEXT,         -- JSON list
                raw_llm_analysis TEXT,          -- Full LLM response if used
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (paper_id) REFERENCES papers(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_type TEXT NOT NULL,     -- methodology, data, technique, topic, etc.
                description TEXT NOT NULL,
                frequency INTEGER DEFAULT 1,
                journals TEXT,                  -- JSON list of journal codes
                year_range TEXT,                -- e.g., "2020-2025"
                example_paper_ids TEXT,         -- JSON list of paper IDs
                strength REAL DEFAULT 0.0,      -- 0-1 score
                details TEXT,                   -- JSON with additional details
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS ideas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                research_questions TEXT,        -- JSON list
                rationale TEXT,
                based_on_pattern_ids TEXT,      -- JSON list of pattern IDs
                suggested_methodology TEXT,     -- JSON list
                suggested_techniques TEXT,      -- JSON list
                target_journals TEXT,           -- JSON list of journal codes
                feasibility_score REAL DEFAULT 0.0,  -- 0-1
                novelty_score REAL DEFAULT 0.0,      -- 0-1
                impact_score REAL DEFAULT 0.0,       -- 0-1
                status TEXT DEFAULT 'proposed', -- proposed, exploring, validated, archived
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS datasets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idea_id INTEGER,
                name TEXT NOT NULL,
                source TEXT NOT NULL,           -- Repository name
                source_url TEXT,
                description TEXT,
                variables TEXT,                 -- JSON list of key variables
                size_description TEXT,
                access_type TEXT,               -- open, restricted, commercial
                relevance_score REAL DEFAULT 0.0,
                notes TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (idea_id) REFERENCES ideas(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_papers_journal ON papers(journal_code);
            CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year);
            CREATE INDEX IF NOT EXISTS idx_papers_doi ON papers(doi);
            CREATE INDEX IF NOT EXISTS idx_analyses_paper ON analyses(paper_id);
            CREATE INDEX IF NOT EXISTS idx_patterns_type ON patterns(pattern_type);
            CREATE INDEX IF NOT EXISTS idx_datasets_idea ON datasets(idea_id);
        """)
        self.conn.commit()
        logger.info("Database tables initialized.")

    # ─── Paper Operations ─────────────────────────────────────────────────────

    def upsert_paper(self, paper_data: Dict[str, Any]) -> int:
        """Insert or update a paper. Returns the paper ID."""
        doi = paper_data.get("doi")
        if doi:
            existing = self.conn.execute(
                "SELECT id FROM papers WHERE doi = ?", (doi,)
            ).fetchone()
            if existing:
                paper_id = existing["id"]
                self._update_paper(paper_id, paper_data)
                return paper_id

        # Serialize list/dict fields to JSON
        for field in ("authors", "keywords"):
            if field in paper_data and isinstance(paper_data[field], (list, dict)):
                paper_data[field] = json.dumps(paper_data[field])

        columns = ", ".join(paper_data.keys())
        placeholders = ", ".join(["?"] * len(paper_data))
        values = list(paper_data.values())

        cursor = self.conn.execute(
            f"INSERT INTO papers ({columns}) VALUES ({placeholders})", values
        )
        self.conn.commit()
        return cursor.lastrowid

    def _update_paper(self, paper_id: int, paper_data: Dict[str, Any]):
        """Update existing paper fields (only non-None values)."""
        for field in ("authors", "keywords"):
            if field in paper_data and isinstance(paper_data[field], (list, dict)):
                paper_data[field] = json.dumps(paper_data[field])

        updates = {k: v for k, v in paper_data.items() if v is not None and k != "doi"}
        if not updates:
            return
        updates["updated_at"] = datetime.now().isoformat()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [paper_id]
        self.conn.execute(
            f"UPDATE papers SET {set_clause} WHERE id = ?", values
        )
        self.conn.commit()

    def get_paper(self, paper_id: int) -> Optional[Dict]:
        """Get a single paper by ID."""
        row = self.conn.execute(
            "SELECT * FROM papers WHERE id = ?", (paper_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_paper_by_doi(self, doi: str) -> Optional[Dict]:
        """Get a single paper by DOI."""
        row = self.conn.execute(
            "SELECT * FROM papers WHERE doi = ?", (doi,)
        ).fetchone()
        return dict(row) if row else None

    def get_papers(
        self,
        journal_code: Optional[str] = None,
        year: Optional[int] = None,
        analyzed: Optional[bool] = None,
        limit: int = 1000,
    ) -> List[Dict]:
        """Get papers with optional filters."""
        query = "SELECT * FROM papers WHERE 1=1"
        params = []
        if journal_code:
            query += " AND journal_code = ?"
            params.append(journal_code)
        if year:
            query += " AND year = ?"
            params.append(year)
        if analyzed is not None:
            query += " AND analyzed = ?"
            params.append(1 if analyzed else 0)
        query += " ORDER BY citation_count DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_unanalyzed_papers(self, limit: int = 100) -> List[Dict]:
        """Get papers that have text but haven't been analyzed yet."""
        rows = self.conn.execute(
            """SELECT * FROM papers
               WHERE (full_text IS NOT NULL OR abstract IS NOT NULL)
               AND analyzed = 0
               ORDER BY citation_count DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def count_papers(self, journal_code: Optional[str] = None) -> int:
        """Count papers, optionally filtered by journal."""
        if journal_code:
            row = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM papers WHERE journal_code = ?",
                (journal_code,),
            ).fetchone()
        else:
            row = self.conn.execute("SELECT COUNT(*) as cnt FROM papers").fetchone()
        return row["cnt"]

    # ─── Analysis Operations ──────────────────────────────────────────────────

    def save_analysis(self, analysis_data: Dict[str, Any]) -> int:
        """Save a paper analysis. Returns the analysis ID."""
        for field in (
            "methodology_types", "data_sources", "analytical_techniques",
            "theoretical_frameworks", "key_contributions", "future_directions",
            "publishability_factors",
        ):
            if field in analysis_data and isinstance(analysis_data[field], (list, dict)):
                analysis_data[field] = json.dumps(analysis_data[field])

        columns = ", ".join(analysis_data.keys())
        placeholders = ", ".join(["?"] * len(analysis_data))
        values = list(analysis_data.values())

        cursor = self.conn.execute(
            f"INSERT OR REPLACE INTO analyses ({columns}) VALUES ({placeholders})",
            values,
        )
        # Mark paper as analyzed
        paper_id = analysis_data.get("paper_id")
        if paper_id:
            self.conn.execute(
                "UPDATE papers SET analyzed = 1, updated_at = ? WHERE id = ?",
                (datetime.now().isoformat(), paper_id),
            )
        self.conn.commit()
        return cursor.lastrowid

    def get_analysis(self, paper_id: int) -> Optional[Dict]:
        """Get analysis for a paper."""
        row = self.conn.execute(
            "SELECT * FROM analyses WHERE paper_id = ?", (paper_id,)
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        # Deserialize JSON fields
        for field in (
            "methodology_types", "data_sources", "analytical_techniques",
            "theoretical_frameworks", "key_contributions", "future_directions",
            "publishability_factors",
        ):
            if result.get(field):
                try:
                    result[field] = json.loads(result[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        return result

    def get_all_analyses(self) -> List[Dict]:
        """Get all analyses with paper info."""
        rows = self.conn.execute(
            """SELECT a.*, p.title, p.journal_code, p.year, p.citation_count
               FROM analyses a
               JOIN papers p ON a.paper_id = p.id
               ORDER BY p.citation_count DESC"""
        ).fetchall()
        results = []
        for row in rows:
            d = dict(row)
            for field in (
                "methodology_types", "data_sources", "analytical_techniques",
                "theoretical_frameworks", "key_contributions", "future_directions",
                "publishability_factors",
            ):
                if d.get(field):
                    try:
                        d[field] = json.loads(d[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            results.append(d)
        return results

    # ─── Pattern Operations ───────────────────────────────────────────────────

    def save_pattern(self, pattern_data: Dict[str, Any]) -> int:
        """Save a discovered pattern."""
        for field in ("journals", "example_paper_ids", "details"):
            if field in pattern_data and isinstance(pattern_data[field], (list, dict)):
                pattern_data[field] = json.dumps(pattern_data[field])

        columns = ", ".join(pattern_data.keys())
        placeholders = ", ".join(["?"] * len(pattern_data))
        values = list(pattern_data.values())

        cursor = self.conn.execute(
            f"INSERT INTO patterns ({columns}) VALUES ({placeholders})", values
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_patterns(self, pattern_type: Optional[str] = None) -> List[Dict]:
        """Get patterns, optionally filtered by type."""
        if pattern_type:
            rows = self.conn.execute(
                "SELECT * FROM patterns WHERE pattern_type = ? ORDER BY strength DESC",
                (pattern_type,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM patterns ORDER BY strength DESC"
            ).fetchall()
        results = []
        for row in rows:
            d = dict(row)
            for field in ("journals", "example_paper_ids", "details"):
                if d.get(field):
                    try:
                        d[field] = json.loads(d[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            results.append(d)
        return results

    def clear_patterns(self):
        """Remove all patterns (before re-computing)."""
        self.conn.execute("DELETE FROM patterns")
        self.conn.commit()

    # ─── Idea Operations ──────────────────────────────────────────────────────

    def save_idea(self, idea_data: Dict[str, Any]) -> int:
        """Save a research idea."""
        for field in (
            "research_questions", "based_on_pattern_ids",
            "suggested_methodology", "suggested_techniques", "target_journals",
        ):
            if field in idea_data and isinstance(idea_data[field], (list, dict)):
                idea_data[field] = json.dumps(idea_data[field])

        columns = ", ".join(idea_data.keys())
        placeholders = ", ".join(["?"] * len(idea_data))
        values = list(idea_data.values())

        cursor = self.conn.execute(
            f"INSERT INTO ideas ({columns}) VALUES ({placeholders})", values
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_ideas(self, status: Optional[str] = None) -> List[Dict]:
        """Get research ideas."""
        if status:
            rows = self.conn.execute(
                "SELECT * FROM ideas WHERE status = ? ORDER BY feasibility_score DESC",
                (status,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM ideas ORDER BY feasibility_score DESC"
            ).fetchall()
        results = []
        for row in rows:
            d = dict(row)
            for field in (
                "research_questions", "based_on_pattern_ids",
                "suggested_methodology", "suggested_techniques", "target_journals",
            ):
                if d.get(field):
                    try:
                        d[field] = json.loads(d[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            results.append(d)
        return results

    def clear_ideas(self):
        """Remove all ideas (before re-generating)."""
        self.conn.execute("DELETE FROM ideas")
        self.conn.commit()

    # ─── Dataset Operations ───────────────────────────────────────────────────

    def save_dataset(self, dataset_data: Dict[str, Any]) -> int:
        """Save a dataset reference."""
        if "variables" in dataset_data and isinstance(dataset_data["variables"], list):
            dataset_data["variables"] = json.dumps(dataset_data["variables"])

        columns = ", ".join(dataset_data.keys())
        placeholders = ", ".join(["?"] * len(dataset_data))
        values = list(dataset_data.values())

        cursor = self.conn.execute(
            f"INSERT INTO datasets ({columns}) VALUES ({placeholders})", values
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_datasets(self, idea_id: Optional[int] = None) -> List[Dict]:
        """Get datasets, optionally filtered by idea."""
        if idea_id:
            rows = self.conn.execute(
                "SELECT * FROM datasets WHERE idea_id = ? ORDER BY relevance_score DESC",
                (idea_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM datasets ORDER BY relevance_score DESC"
            ).fetchall()
        results = []
        for row in rows:
            d = dict(row)
            if d.get("variables"):
                try:
                    d["variables"] = json.loads(d["variables"])
                except (json.JSONDecodeError, TypeError):
                    pass
            results.append(d)
        return results

    def clear_datasets(self):
        """Remove all datasets (before re-searching)."""
        self.conn.execute("DELETE FROM datasets")
        self.conn.commit()

    # ─── Statistics ───────────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        stats = {}
        stats["total_papers"] = self.conn.execute(
            "SELECT COUNT(*) FROM papers"
        ).fetchone()[0]
        stats["papers_with_text"] = self.conn.execute(
            "SELECT COUNT(*) FROM papers WHERE full_text IS NOT NULL"
        ).fetchone()[0]
        stats["papers_analyzed"] = self.conn.execute(
            "SELECT COUNT(*) FROM papers WHERE analyzed = 1"
        ).fetchone()[0]
        stats["total_analyses"] = self.conn.execute(
            "SELECT COUNT(*) FROM analyses"
        ).fetchone()[0]
        stats["total_patterns"] = self.conn.execute(
            "SELECT COUNT(*) FROM patterns"
        ).fetchone()[0]
        stats["total_ideas"] = self.conn.execute(
            "SELECT COUNT(*) FROM ideas"
        ).fetchone()[0]
        stats["total_datasets"] = self.conn.execute(
            "SELECT COUNT(*) FROM datasets"
        ).fetchone()[0]

        # Per-journal counts
        stats["papers_by_journal"] = {}
        for row in self.conn.execute(
            "SELECT journal_code, COUNT(*) as cnt FROM papers GROUP BY journal_code"
        ).fetchall():
            stats["papers_by_journal"][row["journal_code"]] = row["cnt"]

        return stats

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
