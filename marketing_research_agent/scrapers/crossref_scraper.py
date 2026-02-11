"""
CrossRef API scraper for fetching paper metadata by journal ISSN.
Free API, no authentication required. Provides DOIs, titles, authors, abstracts.
"""

import logging
from typing import Any, Dict, List, Optional

from .. import config
from ..database import Database
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class CrossRefScraper(BaseScraper):
    """Fetch paper metadata from the CrossRef API."""

    def __init__(self, db: Database):
        super().__init__()
        self.db = db
        self.base_url = config.CROSSREF_API_URL

    def fetch_journal_papers(
        self,
        journal_code: str,
        from_year: int = config.DEFAULT_YEARS[0],
        to_year: int = config.DEFAULT_YEARS[1],
        max_results: int = config.MAX_PAPERS_PER_JOURNAL,
    ) -> int:
        """
        Fetch papers from a journal via CrossRef.
        Returns the number of papers fetched.
        """
        journal_info = config.JOURNALS.get(journal_code)
        if not journal_info:
            logger.error("Unknown journal code: %s", journal_code)
            return 0

        issn = journal_info["issn"]
        logger.info(
            "Fetching papers from %s (%s) for %d-%d via CrossRef...",
            journal_info["name"], issn, from_year, to_year,
        )

        total_fetched = 0
        cursor = "*"  # CrossRef deep paging cursor

        while total_fetched < max_results:
            batch_size = min(100, max_results - total_fetched)
            params = {
                "filter": f"issn:{issn},from-pub-date:{from_year},until-pub-date:{to_year}",
                "rows": batch_size,
                "cursor": cursor,
                "sort": "published",
                "order": "desc",
                "select": (
                    "DOI,title,author,published-print,published-online,"
                    "volume,issue,page,abstract,subject,is-referenced-by-count,"
                    "references-count,URL,type"
                ),
                "mailto": config.CROSSREF_MAILTO,
            }

            response = self._rate_limited_get(self.base_url, params=params)
            if not response:
                logger.warning("Failed to fetch batch from CrossRef for %s", journal_code)
                break

            data = response.json()
            items = data.get("message", {}).get("items", [])
            if not items:
                break

            for item in items:
                paper_data = self._parse_crossref_item(item, journal_code, journal_info)
                if paper_data:
                    self.db.upsert_paper(paper_data)
                    total_fetched += 1

            # Get next cursor
            cursor = data.get("message", {}).get("next-cursor")
            if not cursor:
                break

            logger.info(
                "  Fetched %d/%d papers from %s",
                total_fetched, max_results, journal_code,
            )

        logger.info(
            "Completed CrossRef fetch for %s: %d papers",
            journal_code, total_fetched,
        )
        return total_fetched

    def _parse_crossref_item(
        self, item: Dict[str, Any], journal_code: str, journal_info: Dict
    ) -> Optional[Dict[str, Any]]:
        """Parse a CrossRef work item into our paper schema."""
        doi = item.get("DOI")
        if not doi:
            return None

        # Only process journal articles
        item_type = item.get("type", "")
        if item_type not in ("journal-article", ""):
            return None

        # Extract title
        titles = item.get("title", [])
        title = titles[0] if titles else None
        if not title:
            return None

        # Extract authors
        authors = []
        for author in item.get("author", []):
            given = author.get("given", "")
            family = author.get("family", "")
            name = f"{given} {family}".strip()
            if name:
                authors.append(name)

        # Extract publication date
        pub_date = item.get("published-print") or item.get("published-online") or {}
        date_parts = pub_date.get("date-parts", [[None]])[0]
        year = date_parts[0] if len(date_parts) > 0 else None
        month = date_parts[1] if len(date_parts) > 1 else None

        # Extract abstract (CrossRef sometimes includes JATS XML tags)
        abstract = item.get("abstract", "")
        if abstract:
            # Strip JATS XML tags
            import re
            abstract = re.sub(r"<[^>]+>", "", abstract).strip()

        return {
            "doi": doi,
            "title": title,
            "authors": authors,
            "journal_code": journal_code,
            "journal_name": journal_info["name"],
            "year": year,
            "month": month,
            "volume": item.get("volume"),
            "issue": item.get("issue"),
            "pages": item.get("page"),
            "abstract": abstract or None,
            "keywords": item.get("subject", []),
            "citation_count": item.get("is-referenced-by-count", 0),
            "reference_count": item.get("references-count", 0),
            "url": item.get("URL"),
            "crossref_fetched": 1,
        }

    def fetch_all_journals(
        self,
        from_year: int = config.DEFAULT_YEARS[0],
        to_year: int = config.DEFAULT_YEARS[1],
        max_per_journal: int = config.MAX_PAPERS_PER_JOURNAL,
    ) -> Dict[str, int]:
        """Fetch papers from all configured journals. Returns counts per journal."""
        results = {}
        for code in config.JOURNALS:
            count = self.fetch_journal_papers(code, from_year, to_year, max_per_journal)
            results[code] = count
        return results
