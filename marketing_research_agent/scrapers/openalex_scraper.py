"""
OpenAlex API scraper for enriching paper metadata.
Free API, no authentication required. Provides citation data, concepts, open access info.
"""

import logging
from typing import Any, Dict, List, Optional

from .. import config
from ..database import Database
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class OpenAlexScraper(BaseScraper):
    """Fetch and enrich paper metadata from the OpenAlex API."""

    def __init__(self, db: Database):
        super().__init__()
        self.db = db
        self.base_url = config.OPENALEX_API_URL

    def fetch_journal_papers(
        self,
        journal_code: str,
        from_year: int = config.DEFAULT_YEARS[0],
        to_year: int = config.DEFAULT_YEARS[1],
        max_results: int = config.MAX_PAPERS_PER_JOURNAL,
    ) -> int:
        """
        Fetch papers from a journal via OpenAlex.
        Returns the number of papers fetched/enriched.
        """
        journal_info = config.JOURNALS.get(journal_code)
        if not journal_info:
            logger.error("Unknown journal code: %s", journal_code)
            return 0

        issn = journal_info["issn"]
        logger.info(
            "Fetching papers from %s via OpenAlex...",
            journal_info["name"],
        )

        total_fetched = 0
        page = 1
        per_page = min(200, max_results)

        while total_fetched < max_results:
            params = {
                "filter": (
                    f"primary_location.source.issn:{issn},"
                    f"publication_year:{from_year}-{to_year},"
                    f"type:article"
                ),
                "per_page": per_page,
                "page": page,
                "sort": "cited_by_count:desc",
                "mailto": config.OPENALEX_MAILTO,
            }

            response = self._rate_limited_get(
                f"{self.base_url}/works", params=params
            )
            if not response:
                logger.warning("Failed to fetch from OpenAlex for %s", journal_code)
                break

            data = response.json()
            results = data.get("results", [])
            if not results:
                break

            for work in results:
                paper_data = self._parse_openalex_work(work, journal_code, journal_info)
                if paper_data:
                    self.db.upsert_paper(paper_data)
                    total_fetched += 1

            page += 1
            if total_fetched >= max_results:
                break

            total_available = data.get("meta", {}).get("count", 0)
            if total_fetched >= total_available:
                break

            logger.info(
                "  Fetched %d/%d papers from %s (OpenAlex)",
                total_fetched, min(max_results, total_available), journal_code,
            )

        logger.info(
            "Completed OpenAlex fetch for %s: %d papers",
            journal_code, total_fetched,
        )
        return total_fetched

    def _parse_openalex_work(
        self, work: Dict[str, Any], journal_code: str, journal_info: Dict
    ) -> Optional[Dict[str, Any]]:
        """Parse an OpenAlex work into our paper schema."""
        doi = work.get("doi", "")
        if doi and doi.startswith("https://doi.org/"):
            doi = doi.replace("https://doi.org/", "")
        if not doi:
            return None

        title = work.get("title")
        if not title:
            return None

        # Extract authors
        authors = []
        for authorship in work.get("authorships", []):
            author = authorship.get("author", {})
            name = author.get("display_name", "")
            if name:
                authors.append(name)

        # Extract year
        year = work.get("publication_year")

        # Extract abstract from inverted index
        abstract = self._reconstruct_abstract(work.get("abstract_inverted_index"))

        # Extract keywords/concepts
        keywords = []
        for concept in work.get("concepts", []):
            if concept.get("score", 0) > 0.3:
                keywords.append(concept.get("display_name", ""))

        # Extract volume/issue from biblio
        biblio = work.get("biblio", {})

        # Open access info
        oa = work.get("open_access", {})
        oa_url = oa.get("oa_url")

        return {
            "doi": doi,
            "title": title,
            "authors": authors,
            "journal_code": journal_code,
            "journal_name": journal_info["name"],
            "year": year,
            "volume": biblio.get("volume"),
            "issue": biblio.get("issue"),
            "pages": (
                f"{biblio.get('first_page', '')}-{biblio.get('last_page', '')}"
                if biblio.get("first_page")
                else None
            ),
            "abstract": abstract,
            "keywords": keywords,
            "citation_count": work.get("cited_by_count", 0),
            "reference_count": len(work.get("referenced_works", [])),
            "url": oa_url or work.get("doi"),
            "openalex_id": work.get("id"),
            "openalex_fetched": 1,
        }

    def _reconstruct_abstract(
        self, inverted_index: Optional[Dict[str, List[int]]]
    ) -> Optional[str]:
        """Reconstruct abstract text from OpenAlex inverted index format."""
        if not inverted_index:
            return None

        # Build position -> word mapping
        word_positions = []
        for word, positions in inverted_index.items():
            for pos in positions:
                word_positions.append((pos, word))

        if not word_positions:
            return None

        word_positions.sort(key=lambda x: x[0])
        return " ".join(word for _, word in word_positions)

    def enrich_existing_papers(self, limit: int = 500) -> int:
        """
        Enrich papers already in the DB (from CrossRef) with OpenAlex data.
        Adds citation counts, concepts, and open access URLs.
        """
        papers = self.db.get_papers(limit=limit)
        enriched = 0

        for paper in papers:
            if paper.get("openalex_fetched"):
                continue

            doi = paper.get("doi")
            if not doi:
                continue

            work_data = self._fetch_work_by_doi(doi)
            if not work_data:
                continue

            update = {
                "doi": doi,
                "openalex_id": work_data.get("id"),
                "citation_count": max(
                    paper.get("citation_count", 0),
                    work_data.get("cited_by_count", 0),
                ),
                "openalex_fetched": 1,
            }

            # Add abstract if missing
            if not paper.get("abstract"):
                abstract = self._reconstruct_abstract(
                    work_data.get("abstract_inverted_index")
                )
                if abstract:
                    update["abstract"] = abstract

            # Add keywords if missing
            if not paper.get("keywords") or paper["keywords"] == "[]":
                keywords = [
                    c.get("display_name", "")
                    for c in work_data.get("concepts", [])
                    if c.get("score", 0) > 0.3
                ]
                if keywords:
                    update["keywords"] = keywords

            # Add OA URL
            oa = work_data.get("open_access", {})
            if oa.get("oa_url") and not paper.get("url"):
                update["url"] = oa["oa_url"]

            self.db.upsert_paper(update)
            enriched += 1

            if enriched % 50 == 0:
                logger.info("  Enriched %d papers via OpenAlex", enriched)

        logger.info("Enriched %d papers with OpenAlex data", enriched)
        return enriched

    def _fetch_work_by_doi(self, doi: str) -> Optional[Dict]:
        """Fetch a single work from OpenAlex by DOI."""
        url = f"{self.base_url}/works/https://doi.org/{doi}"
        params = {"mailto": config.OPENALEX_MAILTO}
        response = self._rate_limited_get(url, params=params)
        if response:
            return response.json()
        return None

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
