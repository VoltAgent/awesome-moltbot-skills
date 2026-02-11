"""
Main orchestrator agent that coordinates the full research pipeline:
1. Scrape paper metadata from CrossRef and OpenAlex
2. Download PDFs (if credentials available)
3. Extract text from PDFs
4. Analyze papers for publishability factors
5. Find patterns across papers
6. Generate research ideas
7. Find datasets for ideas
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from . import config
from .database import Database
from .scrapers.crossref_scraper import CrossRefScraper
from .scrapers.openalex_scraper import OpenAlexScraper
from .scrapers.browser_downloader import BrowserDownloader
from .analyzers.pdf_parser import PDFParser
from .analyzers.paper_analyzer import PaperAnalyzer
from .analyzers.pattern_finder import PatternFinder
from .generators.idea_generator import IdeaGenerator
from .generators.dataset_finder import DatasetFinder

logger = logging.getLogger(__name__)


class MarketingResearchAgent:
    """
    Automated agent for marketing research paper analysis.

    Pipeline stages:
        1. collect  - Fetch paper metadata from CrossRef + OpenAlex
        2. download - Download PDFs (requires library credentials)
        3. extract  - Extract text from downloaded PDFs
        4. analyze  - Analyze papers for publishability factors
        5. patterns - Discover patterns across analyzed papers
        6. ideas    - Generate research ideas from patterns
        7. datasets - Find online datasets for generated ideas
        8. report   - Generate comprehensive report
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db = Database(db_path)
        self.crossref = CrossRefScraper(self.db)
        self.openalex = OpenAlexScraper(self.db)
        self.downloader = BrowserDownloader(self.db)
        self.pdf_parser = PDFParser(self.db)
        self.analyzer = PaperAnalyzer(self.db)
        self.pattern_finder = PatternFinder(self.db)
        self.idea_generator = IdeaGenerator(self.db)
        self.dataset_finder = DatasetFinder(self.db)

    # ─── Pipeline Stages ──────────────────────────────────────────────────────

    def collect_papers(
        self,
        from_year: int = config.DEFAULT_YEARS[0],
        to_year: int = config.DEFAULT_YEARS[1],
        max_per_journal: int = config.MAX_PAPERS_PER_JOURNAL,
        journals: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        Stage 1: Collect paper metadata from CrossRef and OpenAlex.
        No authentication required.
        """
        logger.info("=" * 60)
        logger.info("STAGE 1: Collecting paper metadata (%d-%d)", from_year, to_year)
        logger.info("=" * 60)

        target_journals = journals or list(config.JOURNALS.keys())
        results = {"crossref": {}, "openalex": {}}

        # Fetch from OpenAlex (generally better metadata)
        logger.info("\n--- Fetching from OpenAlex ---")
        for code in target_journals:
            count = self.openalex.fetch_journal_papers(
                code, from_year, to_year, max_per_journal
            )
            results["openalex"][code] = count

        # Enrich with CrossRef data
        logger.info("\n--- Enriching with CrossRef ---")
        for code in target_journals:
            count = self.crossref.fetch_journal_papers(
                code, from_year, to_year, max_per_journal
            )
            results["crossref"][code] = count

        # Cross-enrich
        logger.info("\n--- Cross-enriching data ---")
        self.openalex.enrich_existing_papers()

        total = self.db.count_papers()
        logger.info("\nCollection complete. Total papers in database: %d", total)
        return results

    def download_pdfs(
        self,
        journal_code: Optional[str] = None,
        limit: int = 50,
        open_access_only: bool = False,
    ) -> int:
        """
        Stage 2: Download paper PDFs.
        Requires library credentials for paywalled papers.
        """
        logger.info("=" * 60)
        logger.info("STAGE 2: Downloading PDFs")
        logger.info("=" * 60)

        downloaded = 0

        # Always try open access first
        logger.info("\n--- Downloading open access PDFs ---")
        downloaded += self.downloader.download_open_access(limit=limit)

        if not open_access_only:
            # Try authenticated downloads
            logger.info("\n--- Downloading via library proxy ---")
            downloaded += self.downloader.download_papers(
                journal_code=journal_code, limit=limit
            )

        logger.info("Downloaded %d PDFs total", downloaded)
        return downloaded

    def extract_text(self, limit: int = 100) -> int:
        """
        Stage 3: Extract text from downloaded PDFs.
        """
        logger.info("=" * 60)
        logger.info("STAGE 3: Extracting text from PDFs")
        logger.info("=" * 60)

        count = self.pdf_parser.extract_text_for_papers(limit=limit)
        logger.info("Extracted text from %d papers", count)
        return count

    def analyze_papers(self, limit: int = 500) -> int:
        """
        Stage 4: Analyze papers for publishability factors.
        Works with full text (from PDFs) or abstracts (from metadata).
        """
        logger.info("=" * 60)
        logger.info("STAGE 4: Analyzing papers")
        logger.info("=" * 60)

        count = self.analyzer.analyze_all_papers(limit=limit)
        logger.info("Analyzed %d papers", count)
        return count

    def find_patterns(self) -> Dict[str, int]:
        """
        Stage 5: Discover patterns across analyzed papers.
        """
        logger.info("=" * 60)
        logger.info("STAGE 5: Finding patterns")
        logger.info("=" * 60)

        results = self.pattern_finder.find_all_patterns()
        logger.info("Found patterns: %s", results)
        return results

    def generate_ideas(self, num_ideas: int = 10) -> List[Dict]:
        """
        Stage 6: Generate research ideas from discovered patterns.
        """
        logger.info("=" * 60)
        logger.info("STAGE 6: Generating research ideas")
        logger.info("=" * 60)

        ideas = self.idea_generator.generate_ideas(num_ideas=num_ideas)
        logger.info("Generated %d research ideas", len(ideas))
        return ideas

    def find_datasets(self, max_per_idea: int = 5) -> int:
        """
        Stage 7: Find online datasets for generated ideas.
        """
        logger.info("=" * 60)
        logger.info("STAGE 7: Finding datasets")
        logger.info("=" * 60)

        # Also add known marketing datasets
        ideas = self.db.get_ideas()
        total = 0

        for idea in ideas:
            datasets = self.dataset_finder.find_datasets_with_known_sources(idea)
            for ds in datasets[:max_per_idea]:
                ds["idea_id"] = idea.get("id")
                self.db.save_dataset(ds)
                total += 1

        logger.info("Found %d datasets total", total)
        return total

    def generate_report(self, output_path: Optional[Path] = None) -> str:
        """
        Stage 8: Generate a comprehensive report.
        """
        logger.info("=" * 60)
        logger.info("STAGE 8: Generating report")
        logger.info("=" * 60)

        report_parts = []
        report_parts.append(self._generate_header())
        report_parts.append(self._generate_stats_section())
        report_parts.append(self.pattern_finder.get_pattern_summary())
        report_parts.append(self.idea_generator.get_ideas_summary())
        report_parts.append(self.dataset_finder.get_datasets_summary())
        report_parts.append(self._generate_footer())

        report = "\n\n".join(report_parts)

        # Save report
        if output_path is None:
            output_path = config.DATA_DIR / "report.txt"

        output_path.write_text(report, encoding="utf-8")
        logger.info("Report saved to: %s", output_path)

        # Also save as JSON
        json_path = config.DATA_DIR / "report.json"
        self._save_json_report(json_path)
        logger.info("JSON report saved to: %s", json_path)

        return report

    # ─── Full Pipeline ────────────────────────────────────────────────────────

    def run_full_pipeline(
        self,
        from_year: int = config.DEFAULT_YEARS[0],
        to_year: int = config.DEFAULT_YEARS[1],
        max_per_journal: int = config.MAX_PAPERS_PER_JOURNAL,
        journals: Optional[List[str]] = None,
        skip_download: bool = False,
        num_ideas: int = 10,
    ) -> str:
        """
        Run the complete pipeline from collection to report generation.

        Args:
            from_year: Start year for paper collection
            to_year: End year for paper collection
            max_per_journal: Maximum papers to fetch per journal
            journals: List of journal codes (None = all)
            skip_download: Skip PDF download stage
            num_ideas: Number of research ideas to generate

        Returns:
            The generated report as a string.
        """
        start_time = datetime.now()
        logger.info("Starting full pipeline at %s", start_time.isoformat())

        # Stage 1: Collect metadata
        self.collect_papers(from_year, to_year, max_per_journal, journals)

        # Stage 2: Download PDFs (optional)
        if not skip_download:
            self.download_pdfs(open_access_only=not self.downloader.is_configured())

        # Stage 3: Extract text from PDFs
        self.extract_text()

        # Stage 4: Analyze papers
        self.analyze_papers()

        # Stage 5: Find patterns
        self.find_patterns()

        # Stage 6: Generate ideas
        self.generate_ideas(num_ideas=num_ideas)

        # Stage 7: Find datasets
        self.find_datasets()

        # Stage 8: Generate report
        report = self.generate_report()

        elapsed = datetime.now() - start_time
        logger.info("Pipeline completed in %s", elapsed)

        return report

    # ─── Report Helpers ───────────────────────────────────────────────────────

    def _generate_header(self) -> str:
        """Generate report header."""
        return f"""
{'#' * 70}
#  MARKETING RESEARCH AGENT — ANALYSIS REPORT
#  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#  Journals: {', '.join(config.JOURNALS.keys())}
{'#' * 70}

This report was automatically generated by the Marketing Research Agent.
It analyzes papers from top marketing journals to identify:
  • What makes papers publishable and successful
  • Patterns in methodology, data, and analytical techniques
  • Research gaps and opportunities
  • Concrete research ideas with suggested datasets
"""

    def _generate_stats_section(self) -> str:
        """Generate database statistics section."""
        stats = self.db.get_stats()
        parts = []
        parts.append("=" * 70)
        parts.append("DATABASE STATISTICS")
        parts.append("=" * 70)
        parts.append(f"  Total papers:        {stats['total_papers']}")
        parts.append(f"  Papers with text:    {stats['papers_with_text']}")
        parts.append(f"  Papers analyzed:     {stats['papers_analyzed']}")
        parts.append(f"  Patterns found:      {stats['total_patterns']}")
        parts.append(f"  Ideas generated:     {stats['total_ideas']}")
        parts.append(f"  Datasets found:      {stats['total_datasets']}")
        parts.append(f"\n  Papers by journal:")
        for journal, count in sorted(
            stats.get("papers_by_journal", {}).items(),
            key=lambda x: -x[1],
        ):
            name = config.JOURNALS.get(journal, {}).get("name", journal)
            parts.append(f"    {journal:6s} ({name}): {count}")
        return "\n".join(parts)

    def _generate_footer(self) -> str:
        """Generate report footer."""
        return f"""
{'=' * 70}
END OF REPORT
{'=' * 70}

Next steps:
  1. Review the generated research ideas above
  2. Explore the suggested datasets for promising ideas
  3. Refine ideas based on your expertise and interests
  4. Consider the journal preferences when targeting submissions
  5. Use the pattern analysis to strengthen your methodology

For more detailed data, see the JSON report: data/report.json
For the full database, see: data/knowledge.db

To re-run with different parameters:
  python -m marketing_research_agent.run_agent --help
"""

    def _save_json_report(self, path: Path):
        """Save a structured JSON report."""
        report = {
            "generated_at": datetime.now().isoformat(),
            "stats": self.db.get_stats(),
            "patterns": self.db.get_patterns(),
            "ideas": self.db.get_ideas(),
            "datasets": self.db.get_datasets(),
        }
        path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    def get_status(self) -> Dict:
        """Get current agent status and database statistics."""
        return self.db.get_stats()

    def close(self):
        """Clean up all resources."""
        self.crossref.close()
        self.openalex.close()
        self.downloader.close()
        self.dataset_finder.close()
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
