"""
PDF text extraction using PyMuPDF (fitz).
Extracts full text, sections, and metadata from academic paper PDFs.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..database import Database

logger = logging.getLogger(__name__)


class PDFParser:
    """Extract and structure text from academic paper PDFs."""

    # Common section headers in marketing papers
    SECTION_PATTERNS = [
        r"(?i)^(?:1\.?\s*)?introduction",
        r"(?i)^(?:2\.?\s*)?(?:literature\s+review|theoretical\s+(?:background|framework)|conceptual\s+(?:background|framework))",
        r"(?i)^(?:3\.?\s*)?(?:hypothes[ei]s\s+development|theory\s+and\s+hypothes[ei]s|conceptual\s+model)",
        r"(?i)^(?:\d\.?\s*)?(?:research\s+design|method(?:ology)?|data(?:\s+and\s+method)?|empirical\s+(?:strategy|approach))",
        r"(?i)^(?:\d\.?\s*)?(?:study\s+\d|experiment\s+\d)",
        r"(?i)^(?:\d\.?\s*)?(?:results?|findings|empirical\s+results)",
        r"(?i)^(?:\d\.?\s*)?(?:general\s+)?discussion",
        r"(?i)^(?:\d\.?\s*)?(?:implications?|managerial\s+implications?|theoretical\s+implications?)",
        r"(?i)^(?:\d\.?\s*)?(?:limitations?\s+and\s+future|future\s+(?:research|directions?))",
        r"(?i)^(?:\d\.?\s*)?(?:conclusion|concluding\s+remarks)",
        r"(?i)^references?$",
        r"(?i)^(?:appendix|online\s+appendix|supplementary|web\s+appendix)",
    ]

    def __init__(self, db: Database):
        self.db = db

    def extract_text(self, pdf_path: str) -> Optional[str]:
        """
        Extract full text from a PDF file.
        Returns the extracted text or None if extraction fails.
        """
        try:
            import fitz  # PyMuPDF
        except ImportError:
            logger.error(
                "PyMuPDF not installed. Run: pip install PyMuPDF"
            )
            return None

        path = Path(pdf_path)
        if not path.exists():
            logger.error("PDF file not found: %s", pdf_path)
            return None

        try:
            doc = fitz.open(str(path))
            text_parts = []

            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text("text")
                if text.strip():
                    text_parts.append(text)

            doc.close()

            full_text = "\n\n".join(text_parts)
            # Clean up common PDF artifacts
            full_text = self._clean_text(full_text)

            if len(full_text) < 100:
                logger.warning("Very short text extracted from %s", pdf_path)
                return None

            return full_text

        except Exception as e:
            logger.error("Error extracting text from %s: %s", pdf_path, e)
            return None

    def _clean_text(self, text: str) -> str:
        """Clean extracted PDF text."""
        # Remove excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        # Remove page numbers
        text = re.sub(r"\n\s*\d+\s*\n", "\n", text)
        # Remove header/footer artifacts (repeated short lines)
        lines = text.split("\n")
        cleaned_lines = []
        for line in lines:
            stripped = line.strip()
            # Skip very short lines that are likely headers/footers
            if len(stripped) < 3 and not stripped.isdigit():
                continue
            cleaned_lines.append(line)
        text = "\n".join(cleaned_lines)
        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()

    def extract_sections(self, text: str) -> Dict[str, str]:
        """
        Split paper text into sections based on common academic headers.
        Returns a dict of section_name -> section_text.
        """
        sections = {}
        current_section = "preamble"
        current_text = []

        for line in text.split("\n"):
            stripped = line.strip()
            matched_section = None

            for pattern in self.SECTION_PATTERNS:
                if re.match(pattern, stripped):
                    matched_section = stripped.lower()
                    break

            if matched_section:
                # Save previous section
                if current_text:
                    sections[current_section] = "\n".join(current_text).strip()
                current_section = matched_section
                current_text = []
            else:
                current_text.append(line)

        # Save last section
        if current_text:
            sections[current_section] = "\n".join(current_text).strip()

        return sections

    def extract_text_for_papers(self, limit: int = 100) -> int:
        """
        Extract text from downloaded PDFs and store in the database.
        Returns the number of papers processed.
        """
        papers = self.db.get_papers(limit=limit)
        papers = [
            p for p in papers
            if p.get("pdf_downloaded")
            and p.get("pdf_path")
            and not p.get("text_extracted")
        ]

        processed = 0
        for paper in papers:
            pdf_path = paper.get("pdf_path")
            if not pdf_path:
                continue

            text = self.extract_text(pdf_path)
            if text:
                self.db.upsert_paper({
                    "doi": paper["doi"],
                    "full_text": text,
                    "text_extracted": 1,
                })
                processed += 1
                logger.info(
                    "Extracted text from: %s (%d chars)",
                    paper.get("title", "")[:60], len(text),
                )

        logger.info("Extracted text from %d papers", processed)
        return processed

    def get_methodology_section(self, text: str) -> str:
        """Extract the methodology/data section from paper text."""
        sections = self.extract_sections(text)

        # Look for methodology-related sections
        method_keywords = [
            "method", "data", "research design", "empirical",
            "study 1", "study 2", "experiment 1", "experiment 2",
        ]

        method_texts = []
        for section_name, section_text in sections.items():
            if any(kw in section_name for kw in method_keywords):
                method_texts.append(section_text)

        if method_texts:
            return "\n\n".join(method_texts)

        # Fallback: return middle portion of the paper (likely methods/results)
        total_len = len(text)
        start = total_len // 4
        end = 3 * total_len // 4
        return text[start:end]
