"""Scrapers for fetching paper metadata and PDFs from academic sources."""
from .crossref_scraper import CrossRefScraper
from .openalex_scraper import OpenAlexScraper
from .browser_downloader import BrowserDownloader

__all__ = ["CrossRefScraper", "OpenAlexScraper", "BrowserDownloader"]
