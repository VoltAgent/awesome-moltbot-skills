"""
Browser-based PDF downloader using Selenium.
Handles authenticated access through university library proxies.
Requires: selenium, a webdriver (Chrome/Firefox), and library credentials.
"""

import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

from .. import config
from ..database import Database

logger = logging.getLogger(__name__)


class BrowserDownloader:
    """
    Download PDFs through university library proxy using Selenium.

    This component requires:
    1. Selenium and a browser driver (Chrome recommended)
    2. Library proxy URL (e.g., https://proxy.library.uni.edu/login?url=)
    3. Library credentials (username/password)

    Set via environment variables:
        LIBRARY_PROXY_URL, LIBRARY_USERNAME, LIBRARY_PASSWORD

    Or pass directly to the constructor.
    """

    def __init__(
        self,
        db: Database,
        proxy_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        download_dir: Optional[Path] = None,
    ):
        self.db = db
        self.proxy_url = proxy_url or config.LIBRARY_PROXY_URL
        self.username = username or config.LIBRARY_USERNAME
        self.password = password or config.LIBRARY_PASSWORD
        self.download_dir = download_dir or config.PDF_DIR
        self.driver = None

        self.download_dir.mkdir(parents=True, exist_ok=True)

    def is_configured(self) -> bool:
        """Check if library credentials are configured."""
        return bool(self.proxy_url and self.username and self.password)

    def _init_driver(self):
        """Initialize Selenium WebDriver with download preferences."""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
        except ImportError:
            logger.error(
                "Selenium is not installed. Run: pip install selenium\n"
                "Also install ChromeDriver: https://chromedriver.chromium.org/"
            )
            return False

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

        # Configure download directory
        prefs = {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        }
        options.add_experimental_option("prefs", prefs)

        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(60)
            logger.info("Chrome WebDriver initialized.")
            return True
        except Exception as e:
            logger.error("Failed to initialize WebDriver: %s", e)
            logger.info(
                "Make sure Chrome and ChromeDriver are installed.\n"
                "Install ChromeDriver: https://chromedriver.chromium.org/\n"
                "Or use: pip install webdriver-manager"
            )
            return False

    def _authenticate(self) -> bool:
        """Authenticate with the university library proxy."""
        if not self.driver:
            return False

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            # Navigate to proxy login page
            self.driver.get(self.proxy_url)
            time.sleep(3)

            # Try common login form patterns
            # Pattern 1: Standard username/password form
            username_selectors = [
                "input[name='username']",
                "input[name='user']",
                "input[name='login']",
                "input[id='username']",
                "input[id='user']",
                "input[type='text']",
                "input[type='email']",
            ]
            password_selectors = [
                "input[name='password']",
                "input[name='pass']",
                "input[id='password']",
                "input[type='password']",
            ]

            username_field = None
            for selector in username_selectors:
                try:
                    username_field = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if username_field.is_displayed():
                        break
                    username_field = None
                except Exception:
                    continue

            password_field = None
            for selector in password_selectors:
                try:
                    password_field = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if password_field.is_displayed():
                        break
                    password_field = None
                except Exception:
                    continue

            if username_field and password_field:
                username_field.clear()
                username_field.send_keys(self.username)
                password_field.clear()
                password_field.send_keys(self.password)

                # Find and click submit button
                submit_selectors = [
                    "button[type='submit']",
                    "input[type='submit']",
                    "button.login",
                    "input.login",
                ]
                for selector in submit_selectors:
                    try:
                        submit = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if submit.is_displayed():
                            submit.click()
                            break
                    except Exception:
                        continue

                time.sleep(5)
                logger.info("Library authentication attempted.")
                return True
            else:
                logger.warning(
                    "Could not find login form fields. "
                    "The proxy login page may have a different structure. "
                    "Please check the proxy URL and try manual login first."
                )
                return False

        except Exception as e:
            logger.error("Authentication failed: %s", e)
            return False

    def download_paper_pdf(self, paper: Dict) -> Optional[str]:
        """
        Download a single paper's PDF.
        Returns the local file path if successful, None otherwise.
        """
        doi = paper.get("doi")
        if not doi:
            return None

        # Check if already downloaded
        if paper.get("pdf_downloaded") and paper.get("pdf_path"):
            pdf_path = Path(paper["pdf_path"])
            if pdf_path.exists():
                return str(pdf_path)

        # Build filename from DOI
        safe_doi = re.sub(r"[^\w\-.]", "_", doi)
        pdf_path = self.download_dir / f"{safe_doi}.pdf"

        if pdf_path.exists():
            self._update_paper_pdf_path(paper, pdf_path)
            return str(pdf_path)

        # Try to download
        url = self._build_download_url(paper)
        if not url:
            return None

        success = self._download_via_browser(url, pdf_path)
        if success:
            self._update_paper_pdf_path(paper, pdf_path)
            return str(pdf_path)

        return None

    def _build_download_url(self, paper: Dict) -> Optional[str]:
        """Build the download URL, optionally through library proxy."""
        doi = paper.get("doi")
        if not doi:
            return None

        # Direct DOI URL
        doi_url = f"https://doi.org/{doi}"

        # If we have a proxy, route through it
        if self.proxy_url:
            return f"{self.proxy_url}{doi_url}"

        return doi_url

    def _download_via_browser(self, url: str, target_path: Path) -> bool:
        """Download a PDF using the browser."""
        if not self.driver:
            return False

        try:
            self.driver.get(url)
            time.sleep(5)

            # Check if a PDF was downloaded to the download directory
            # Look for recently created PDF files
            for _ in range(30):  # Wait up to 30 seconds
                pdf_files = list(self.download_dir.glob("*.pdf"))
                new_files = [
                    f for f in pdf_files
                    if f.stat().st_mtime > time.time() - 60
                    and f.name != target_path.name
                ]
                if new_files:
                    # Rename the most recent download to our target name
                    newest = max(new_files, key=lambda f: f.stat().st_mtime)
                    newest.rename(target_path)
                    logger.info("Downloaded PDF: %s", target_path.name)
                    return True
                time.sleep(1)

            # Check if the page itself is a PDF
            content_type = self.driver.execute_script(
                "return document.contentType"
            )
            if content_type and "pdf" in content_type.lower():
                # Page is a PDF, save it
                import urllib.request
                urllib.request.urlretrieve(url, str(target_path))
                logger.info("Downloaded PDF (direct): %s", target_path.name)
                return True

            logger.warning("Could not download PDF for: %s", url)
            return False

        except Exception as e:
            logger.error("Error downloading PDF from %s: %s", url, e)
            return False

    def _update_paper_pdf_path(self, paper: Dict, pdf_path: Path):
        """Update the paper record with the PDF path."""
        self.db.upsert_paper({
            "doi": paper["doi"],
            "pdf_path": str(pdf_path),
            "pdf_downloaded": 1,
        })

    def download_papers(
        self,
        journal_code: Optional[str] = None,
        limit: int = 50,
    ) -> int:
        """
        Download PDFs for papers in the database.
        Returns the number of successfully downloaded papers.
        """
        if not self.is_configured():
            logger.warning(
                "Browser downloader not configured. Set environment variables:\n"
                "  LIBRARY_PROXY_URL - Your library's proxy URL\n"
                "  LIBRARY_USERNAME  - Your library username\n"
                "  LIBRARY_PASSWORD  - Your library password\n"
                "\nSkipping PDF downloads. The agent will analyze using "
                "abstracts and metadata instead."
            )
            return 0

        if not self._init_driver():
            return 0

        if not self._authenticate():
            logger.warning("Authentication failed. Attempting downloads anyway...")

        papers = self.db.get_papers(journal_code=journal_code, limit=limit)
        papers = [p for p in papers if not p.get("pdf_downloaded")]

        downloaded = 0
        for paper in papers[:limit]:
            result = self.download_paper_pdf(paper)
            if result:
                downloaded += 1
            time.sleep(2)  # Be polite

            if downloaded % 10 == 0 and downloaded > 0:
                logger.info("  Downloaded %d/%d PDFs", downloaded, len(papers))

        logger.info("Downloaded %d PDFs total", downloaded)
        return downloaded

    def download_open_access(self, limit: int = 100) -> int:
        """
        Download open access PDFs that don't require authentication.
        Uses URLs from OpenAlex OA data.
        """
        papers = self.db.get_papers(limit=limit)
        papers = [
            p for p in papers
            if not p.get("pdf_downloaded")
            and p.get("url")
            and ("pdf" in (p.get("url") or "").lower()
                 or "oa" in (p.get("url") or "").lower())
        ]

        downloaded = 0
        import requests

        for paper in papers:
            doi = paper.get("doi")
            url = paper.get("url")
            if not doi or not url:
                continue

            safe_doi = re.sub(r"[^\w\-.]", "_", doi)
            pdf_path = self.download_dir / f"{safe_doi}.pdf"

            if pdf_path.exists():
                self._update_paper_pdf_path(paper, pdf_path)
                downloaded += 1
                continue

            try:
                response = requests.get(url, timeout=30, allow_redirects=True)
                if (
                    response.status_code == 200
                    and len(response.content) > 1000
                    and (
                        "pdf" in response.headers.get("content-type", "").lower()
                        or response.content[:5] == b"%PDF-"
                    )
                ):
                    pdf_path.write_bytes(response.content)
                    self._update_paper_pdf_path(paper, pdf_path)
                    downloaded += 1
                    logger.info("Downloaded OA PDF: %s", pdf_path.name)
            except Exception as e:
                logger.debug("Failed to download OA PDF for %s: %s", doi, e)

            time.sleep(1)

        logger.info("Downloaded %d open access PDFs", downloaded)
        return downloaded

    def close(self):
        """Close the browser."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
