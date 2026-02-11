#!/usr/bin/env python3
"""
CLI entry point for the Marketing Research Agent.

Usage:
    # Run full pipeline (metadata only, no PDF downloads)
    python -m marketing_research_agent.run_agent

    # Run specific stages
    python -m marketing_research_agent.run_agent --stage collect
    python -m marketing_research_agent.run_agent --stage analyze
    python -m marketing_research_agent.run_agent --stage ideas

    # Custom parameters
    python -m marketing_research_agent.run_agent --from-year 2022 --to-year 2025 --max-papers 50

    # With library credentials for PDF downloads
    LIBRARY_PROXY_URL="https://proxy.library.uni.edu/login?url=" \\
    LIBRARY_USERNAME="user" \\
    LIBRARY_PASSWORD="pass" \\
    python -m marketing_research_agent.run_agent --download-pdfs
"""

import argparse
import logging
import sys
from pathlib import Path

from .agent import MarketingResearchAgent
from . import config


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                config.DATA_DIR / "agent.log", mode="a", encoding="utf-8"
            ),
        ],
    )


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Marketing Research Agent — Automated academic paper analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with defaults
  python -m marketing_research_agent.run_agent

  # Collect papers from 2023-2025, max 50 per journal
  python -m marketing_research_agent.run_agent --from-year 2023 --to-year 2025 --max-papers 50

  # Run only the analysis stage (assumes papers already collected)
  python -m marketing_research_agent.run_agent --stage analyze

  # Generate ideas and find datasets
  python -m marketing_research_agent.run_agent --stage ideas --num-ideas 15

  # Specific journals only
  python -m marketing_research_agent.run_agent --journals JM JMR MKSC

  # Show current database status
  python -m marketing_research_agent.run_agent --status

Environment Variables:
  CROSSREF_MAILTO     Email for CrossRef API (polite pool)
  OPENALEX_MAILTO     Email for OpenAlex API
  OPENAI_API_KEY      OpenAI API key (optional, for LLM-enhanced analysis)
  LIBRARY_PROXY_URL   University library proxy URL
  LIBRARY_USERNAME    Library username
  LIBRARY_PASSWORD    Library password
        """,
    )

    parser.add_argument(
        "--stage",
        choices=["collect", "download", "extract", "analyze", "patterns", "ideas", "datasets", "report", "full"],
        default="full",
        help="Pipeline stage to run (default: full)",
    )
    parser.add_argument(
        "--from-year", type=int, default=config.DEFAULT_YEARS[0],
        help=f"Start year for paper collection (default: {config.DEFAULT_YEARS[0]})",
    )
    parser.add_argument(
        "--to-year", type=int, default=config.DEFAULT_YEARS[1],
        help=f"End year for paper collection (default: {config.DEFAULT_YEARS[1]})",
    )
    parser.add_argument(
        "--max-papers", type=int, default=config.MAX_PAPERS_PER_JOURNAL,
        help=f"Max papers per journal (default: {config.MAX_PAPERS_PER_JOURNAL})",
    )
    parser.add_argument(
        "--journals", nargs="+", choices=list(config.JOURNALS.keys()),
        help="Specific journals to process (default: all)",
    )
    parser.add_argument(
        "--num-ideas", type=int, default=10,
        help="Number of research ideas to generate (default: 10)",
    )
    parser.add_argument(
        "--download-pdfs", action="store_true",
        help="Enable PDF downloads (requires library credentials)",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show current database status and exit",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose/debug logging",
    )

    return parser.parse_args()


def print_status(agent: MarketingResearchAgent):
    """Print current database status."""
    stats = agent.get_status()
    print("\n" + "=" * 50)
    print("  MARKETING RESEARCH AGENT — STATUS")
    print("=" * 50)
    print(f"  Database: {config.DB_PATH}")
    print(f"  Total papers:     {stats['total_papers']}")
    print(f"  With full text:   {stats['papers_with_text']}")
    print(f"  Analyzed:         {stats['papers_analyzed']}")
    print(f"  Patterns found:   {stats['total_patterns']}")
    print(f"  Ideas generated:  {stats['total_ideas']}")
    print(f"  Datasets found:   {stats['total_datasets']}")
    print(f"\n  Papers by journal:")
    for journal, count in sorted(
        stats.get("papers_by_journal", {}).items(), key=lambda x: -x[1]
    ):
        name = config.JOURNALS.get(journal, {}).get("name", journal)
        print(f"    {journal:6s} {name}: {count}")
    print("=" * 50)


def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    logger = logging.getLogger(__name__)
    logger.info("Marketing Research Agent starting...")

    with MarketingResearchAgent() as agent:
        if args.status:
            print_status(agent)
            return

        stage = args.stage

        if stage == "full":
            report = agent.run_full_pipeline(
                from_year=args.from_year,
                to_year=args.to_year,
                max_per_journal=args.max_papers,
                journals=args.journals,
                skip_download=not args.download_pdfs,
                num_ideas=args.num_ideas,
            )
            print("\n" + report)

        elif stage == "collect":
            results = agent.collect_papers(
                from_year=args.from_year,
                to_year=args.to_year,
                max_per_journal=args.max_papers,
                journals=args.journals,
            )
            print(f"\nCollection results: {results}")

        elif stage == "download":
            count = agent.download_pdfs(
                open_access_only=not args.download_pdfs,
            )
            print(f"\nDownloaded {count} PDFs")

        elif stage == "extract":
            count = agent.extract_text()
            print(f"\nExtracted text from {count} papers")

        elif stage == "analyze":
            count = agent.analyze_papers()
            print(f"\nAnalyzed {count} papers")

        elif stage == "patterns":
            results = agent.find_patterns()
            print(f"\nPattern results: {results}")
            print("\n" + agent.pattern_finder.get_pattern_summary())

        elif stage == "ideas":
            ideas = agent.generate_ideas(num_ideas=args.num_ideas)
            print(f"\nGenerated {len(ideas)} ideas")
            print("\n" + agent.idea_generator.get_ideas_summary())

        elif stage == "datasets":
            count = agent.find_datasets()
            print(f"\nFound {count} datasets")
            print("\n" + agent.dataset_finder.get_datasets_summary())

        elif stage == "report":
            report = agent.generate_report()
            print("\n" + report)

        print_status(agent)

    logger.info("Agent finished.")


if __name__ == "__main__":
    main()
