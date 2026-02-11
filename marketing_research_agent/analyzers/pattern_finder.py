"""
Pattern finder that discovers trends and patterns across analyzed papers.
Identifies what makes papers successful across journals and over time.
"""

import json
import logging
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

from .. import config
from ..database import Database

logger = logging.getLogger(__name__)


class PatternFinder:
    """Discover patterns across analyzed marketing research papers."""

    def __init__(self, db: Database):
        self.db = db

    def find_all_patterns(self) -> Dict[str, int]:
        """
        Run all pattern detection routines.
        Returns a dict of pattern_type -> count of patterns found.
        """
        # Clear old patterns before recomputing
        self.db.clear_patterns()

        analyses = self.db.get_all_analyses()
        if not analyses:
            logger.warning("No analyses found. Run paper analysis first.")
            return {}

        logger.info("Finding patterns across %d analyzed papers...", len(analyses))

        results = {}
        results["methodology"] = self._find_methodology_patterns(analyses)
        results["analytical_technique"] = self._find_technique_patterns(analyses)
        results["data_source"] = self._find_data_patterns(analyses)
        results["domain"] = self._find_domain_patterns(analyses)
        results["publishability"] = self._find_publishability_patterns(analyses)
        results["temporal"] = self._find_temporal_patterns(analyses)
        results["journal_preference"] = self._find_journal_preferences(analyses)
        results["cross_method"] = self._find_cross_method_patterns(analyses)

        total = sum(results.values())
        logger.info("Found %d patterns total across %d categories", total, len(results))
        return results

    # ─── Methodology Patterns ─────────────────────────────────────────────────

    def _find_methodology_patterns(self, analyses: List[Dict]) -> int:
        """Find patterns in methodology usage across papers."""
        method_counts = Counter()
        method_by_journal = defaultdict(Counter)
        method_papers = defaultdict(list)

        for a in analyses:
            methods = a.get("methodology_types", [])
            if isinstance(methods, str):
                try:
                    methods = json.loads(methods)
                except (json.JSONDecodeError, TypeError):
                    methods = [methods]

            journal = a.get("journal_code", "unknown")
            paper_id = a.get("paper_id")

            for method in methods:
                method_counts[method] += 1
                method_by_journal[method][journal] += 1
                if paper_id:
                    method_papers[method].append(paper_id)

        count = 0
        total_papers = len(analyses)

        for method, freq in method_counts.most_common():
            if freq < 2:
                continue

            pct = freq / total_papers * 100
            journals = dict(method_by_journal[method])
            top_journals = sorted(journals, key=journals.get, reverse=True)

            self.db.save_pattern({
                "pattern_type": "methodology",
                "description": (
                    f"{method.replace('_', ' ').title()} methodology used in "
                    f"{freq} papers ({pct:.1f}%). "
                    f"Most common in: {', '.join(top_journals[:3])}"
                ),
                "frequency": freq,
                "journals": top_journals,
                "example_paper_ids": method_papers[method][:5],
                "strength": min(1.0, pct / 50),
                "details": {
                    "method": method,
                    "percentage": round(pct, 1),
                    "by_journal": journals,
                },
            })
            count += 1

        return count

    # ─── Analytical Technique Patterns ────────────────────────────────────────

    def _find_technique_patterns(self, analyses: List[Dict]) -> int:
        """Find patterns in analytical techniques."""
        technique_counts = Counter()
        technique_by_journal = defaultdict(Counter)
        technique_papers = defaultdict(list)

        for a in analyses:
            techniques = a.get("analytical_techniques", [])
            if isinstance(techniques, str):
                try:
                    techniques = json.loads(techniques)
                except (json.JSONDecodeError, TypeError):
                    techniques = [techniques]

            journal = a.get("journal_code", "unknown")
            paper_id = a.get("paper_id")

            for tech in techniques:
                if tech.startswith("software:"):
                    continue  # Skip software mentions
                technique_counts[tech] += 1
                technique_by_journal[tech][journal] += 1
                if paper_id:
                    technique_papers[tech].append(paper_id)

        count = 0
        total_papers = len(analyses)

        for tech, freq in technique_counts.most_common():
            if freq < 2:
                continue

            pct = freq / total_papers * 100
            journals = dict(technique_by_journal[tech])

            self.db.save_pattern({
                "pattern_type": "analytical_technique",
                "description": (
                    f"{tech.replace('_', ' ').title()} technique used in "
                    f"{freq} papers ({pct:.1f}%)"
                ),
                "frequency": freq,
                "journals": list(journals.keys()),
                "example_paper_ids": technique_papers[tech][:5],
                "strength": min(1.0, pct / 40),
                "details": {
                    "technique": tech,
                    "percentage": round(pct, 1),
                    "by_journal": journals,
                },
            })
            count += 1

        return count

    # ─── Data Source Patterns ─────────────────────────────────────────────────

    def _find_data_patterns(self, analyses: List[Dict]) -> int:
        """Find patterns in data sources and types."""
        data_type_counts = Counter()
        source_counts = Counter()
        source_papers = defaultdict(list)

        for a in analyses:
            data_type = a.get("data_type", "unknown")
            data_type_counts[data_type] += 1

            sources = a.get("data_sources", [])
            if isinstance(sources, str):
                try:
                    sources = json.loads(sources)
                except (json.JSONDecodeError, TypeError):
                    sources = [sources]

            paper_id = a.get("paper_id")
            for source in sources:
                source_counts[source] += 1
                if paper_id:
                    source_papers[source].append(paper_id)

        count = 0
        total_papers = len(analyses)

        # Data type patterns
        for dtype, freq in data_type_counts.most_common():
            if freq < 2:
                continue
            pct = freq / total_papers * 100
            self.db.save_pattern({
                "pattern_type": "data_source",
                "description": (
                    f"{dtype.title()} data used in {freq} papers ({pct:.1f}%)"
                ),
                "frequency": freq,
                "strength": min(1.0, pct / 50),
                "details": {"data_type": dtype, "percentage": round(pct, 1)},
            })
            count += 1

        # Specific source patterns
        for source, freq in source_counts.most_common(15):
            if freq < 2:
                continue
            self.db.save_pattern({
                "pattern_type": "data_source",
                "description": f"Data source '{source}' used in {freq} papers",
                "frequency": freq,
                "example_paper_ids": source_papers[source][:5],
                "strength": min(1.0, freq / 20),
                "details": {"source": source, "count": freq},
            })
            count += 1

        return count

    # ─── Domain Patterns ──────────────────────────────────────────────────────

    def _find_domain_patterns(self, analyses: List[Dict]) -> int:
        """Find patterns in research domains."""
        domain_counts = Counter()
        domain_by_journal = defaultdict(Counter)
        domain_citations = defaultdict(list)

        for a in analyses:
            domain = a.get("research_domain", "general_marketing")
            journal = a.get("journal_code", "unknown")
            citations = a.get("citation_count", 0)

            domain_counts[domain] += 1
            domain_by_journal[domain][journal] += 1
            domain_citations[domain].append(citations)

        count = 0
        total_papers = len(analyses)

        for domain, freq in domain_counts.most_common():
            if freq < 2:
                continue

            pct = freq / total_papers * 100
            avg_citations = (
                sum(domain_citations[domain]) / len(domain_citations[domain])
                if domain_citations[domain]
                else 0
            )
            journals = dict(domain_by_journal[domain])

            self.db.save_pattern({
                "pattern_type": "domain",
                "description": (
                    f"{domain.replace('_', ' ').title()}: {freq} papers ({pct:.1f}%), "
                    f"avg {avg_citations:.0f} citations"
                ),
                "frequency": freq,
                "journals": list(journals.keys()),
                "strength": min(1.0, pct / 30),
                "details": {
                    "domain": domain,
                    "percentage": round(pct, 1),
                    "avg_citations": round(avg_citations, 1),
                    "by_journal": journals,
                },
            })
            count += 1

        return count

    # ─── Publishability Patterns ──────────────────────────────────────────────

    def _find_publishability_patterns(self, analyses: List[Dict]) -> int:
        """Find patterns in what makes papers publishable."""
        factor_scores = defaultdict(list)
        high_cite_factors = defaultdict(list)

        for a in analyses:
            factors = a.get("publishability_factors", {})
            if isinstance(factors, str):
                try:
                    factors = json.loads(factors)
                except (json.JSONDecodeError, TypeError):
                    factors = {}

            citations = a.get("citation_count", 0)
            is_high_cite = citations > 50  # Threshold for "successful"

            for factor, score in factors.items():
                if isinstance(score, (int, float)):
                    factor_scores[factor].append(score)
                    if is_high_cite:
                        high_cite_factors[factor].append(score)

        count = 0
        for factor, scores in factor_scores.items():
            if len(scores) < 3:
                continue

            avg_score = sum(scores) / len(scores)
            high_cite_avg = (
                sum(high_cite_factors[factor]) / len(high_cite_factors[factor])
                if high_cite_factors[factor]
                else 0
            )

            self.db.save_pattern({
                "pattern_type": "publishability",
                "description": (
                    f"{factor.replace('_', ' ').title()}: "
                    f"avg score {avg_score:.2f} across all papers, "
                    f"{high_cite_avg:.2f} for highly-cited papers"
                ),
                "frequency": len(scores),
                "strength": avg_score,
                "details": {
                    "factor": factor,
                    "avg_score": round(avg_score, 3),
                    "high_cite_avg": round(high_cite_avg, 3),
                    "sample_size": len(scores),
                    "high_cite_sample": len(high_cite_factors[factor]),
                },
            })
            count += 1

        return count

    # ─── Temporal Patterns ────────────────────────────────────────────────────

    def _find_temporal_patterns(self, analyses: List[Dict]) -> int:
        """Find trends over time in methodology and topics."""
        year_methods = defaultdict(Counter)
        year_domains = defaultdict(Counter)

        for a in analyses:
            year = a.get("year")
            if not year:
                continue

            methods = a.get("methodology_types", [])
            if isinstance(methods, str):
                try:
                    methods = json.loads(methods)
                except (json.JSONDecodeError, TypeError):
                    methods = []

            domain = a.get("research_domain", "")

            for method in methods:
                year_methods[year][method] += 1
            if domain:
                year_domains[year][domain] += 1

        count = 0
        years = sorted(year_methods.keys())

        if len(years) >= 2:
            # Compare early vs late periods
            mid = len(years) // 2
            early_years = years[:mid]
            late_years = years[mid:]

            early_methods = Counter()
            late_methods = Counter()
            for y in early_years:
                early_methods.update(year_methods[y])
            for y in late_years:
                late_methods.update(year_methods[y])

            # Find growing methods
            for method in set(list(early_methods.keys()) + list(late_methods.keys())):
                early_pct = early_methods.get(method, 0)
                late_pct = late_methods.get(method, 0)

                if late_pct > early_pct * 1.5 and late_pct >= 3:
                    self.db.save_pattern({
                        "pattern_type": "temporal",
                        "description": (
                            f"Growing trend: {method.replace('_', ' ')} "
                            f"increased from {early_pct} to {late_pct} papers "
                            f"({early_years[0]}-{early_years[-1]} vs "
                            f"{late_years[0]}-{late_years[-1]})"
                        ),
                        "frequency": late_pct,
                        "year_range": f"{years[0]}-{years[-1]}",
                        "strength": min(1.0, (late_pct - early_pct) / max(early_pct, 1)),
                        "details": {
                            "method": method,
                            "early_count": early_pct,
                            "late_count": late_pct,
                            "trend": "growing",
                        },
                    })
                    count += 1

                elif early_pct > late_pct * 1.5 and early_pct >= 3:
                    self.db.save_pattern({
                        "pattern_type": "temporal",
                        "description": (
                            f"Declining trend: {method.replace('_', ' ')} "
                            f"decreased from {early_pct} to {late_pct} papers"
                        ),
                        "frequency": early_pct,
                        "year_range": f"{years[0]}-{years[-1]}",
                        "strength": min(1.0, (early_pct - late_pct) / max(late_pct, 1)),
                        "details": {
                            "method": method,
                            "early_count": early_pct,
                            "late_count": late_pct,
                            "trend": "declining",
                        },
                    })
                    count += 1

        return count

    # ─── Journal Preference Patterns ──────────────────────────────────────────

    def _find_journal_preferences(self, analyses: List[Dict]) -> int:
        """Find what each journal tends to prefer."""
        journal_methods = defaultdict(Counter)
        journal_techniques = defaultdict(Counter)
        journal_domains = defaultdict(Counter)

        for a in analyses:
            journal = a.get("journal_code", "unknown")

            methods = a.get("methodology_types", [])
            if isinstance(methods, str):
                try:
                    methods = json.loads(methods)
                except (json.JSONDecodeError, TypeError):
                    methods = []

            techniques = a.get("analytical_techniques", [])
            if isinstance(techniques, str):
                try:
                    techniques = json.loads(techniques)
                except (json.JSONDecodeError, TypeError):
                    techniques = []

            domain = a.get("research_domain", "")

            for m in methods:
                journal_methods[journal][m] += 1
            for t in techniques:
                if not t.startswith("software:"):
                    journal_techniques[journal][t] += 1
            if domain:
                journal_domains[journal][domain] += 1

        count = 0
        for journal in journal_methods:
            top_methods = journal_methods[journal].most_common(3)
            top_techniques = journal_techniques[journal].most_common(3)
            top_domains = journal_domains[journal].most_common(3)

            journal_name = config.JOURNALS.get(journal, {}).get("name", journal)

            self.db.save_pattern({
                "pattern_type": "journal_preference",
                "description": (
                    f"{journal_name} preferences: "
                    f"Methods: {', '.join(m for m, _ in top_methods)}; "
                    f"Techniques: {', '.join(t for t, _ in top_techniques)}; "
                    f"Domains: {', '.join(d for d, _ in top_domains)}"
                ),
                "frequency": sum(journal_methods[journal].values()),
                "journals": [journal],
                "strength": 0.7,
                "details": {
                    "journal": journal,
                    "top_methods": dict(top_methods),
                    "top_techniques": dict(top_techniques),
                    "top_domains": dict(top_domains),
                },
            })
            count += 1

        return count

    # ─── Cross-Method Patterns ────────────────────────────────────────────────

    def _find_cross_method_patterns(self, analyses: List[Dict]) -> int:
        """Find common combinations of methods and techniques."""
        combos = Counter()
        combo_papers = defaultdict(list)

        for a in analyses:
            methods = a.get("methodology_types", [])
            techniques = a.get("analytical_techniques", [])

            if isinstance(methods, str):
                try:
                    methods = json.loads(methods)
                except (json.JSONDecodeError, TypeError):
                    methods = []
            if isinstance(techniques, str):
                try:
                    techniques = json.loads(techniques)
                except (json.JSONDecodeError, TypeError):
                    techniques = []

            # Filter out software mentions
            techniques = [t for t in techniques if not t.startswith("software:")]

            paper_id = a.get("paper_id")

            # Method + technique combinations
            for m in methods:
                for t in techniques:
                    combo = f"{m} + {t}"
                    combos[combo] += 1
                    if paper_id:
                        combo_papers[combo].append(paper_id)

        count = 0
        for combo, freq in combos.most_common(20):
            if freq < 3:
                continue

            self.db.save_pattern({
                "pattern_type": "cross_method",
                "description": (
                    f"Common combination: {combo.replace('_', ' ')} "
                    f"({freq} papers)"
                ),
                "frequency": freq,
                "example_paper_ids": combo_papers[combo][:5],
                "strength": min(1.0, freq / 15),
                "details": {"combination": combo, "count": freq},
            })
            count += 1

        return count

    def get_pattern_summary(self) -> str:
        """Generate a human-readable summary of all discovered patterns."""
        patterns = self.db.get_patterns()
        if not patterns:
            return "No patterns found. Run analysis first."

        summary_parts = []
        summary_parts.append("=" * 70)
        summary_parts.append("PATTERN ANALYSIS SUMMARY")
        summary_parts.append("=" * 70)

        # Group by type
        by_type = defaultdict(list)
        for p in patterns:
            by_type[p["pattern_type"]].append(p)

        for ptype, plist in sorted(by_type.items()):
            summary_parts.append(f"\n{'─' * 50}")
            summary_parts.append(f"  {ptype.upper().replace('_', ' ')} PATTERNS ({len(plist)})")
            summary_parts.append(f"{'─' * 50}")

            for p in sorted(plist, key=lambda x: x.get("strength", 0), reverse=True)[:10]:
                strength_bar = "█" * int(p.get("strength", 0) * 10)
                summary_parts.append(
                    f"  [{strength_bar:<10}] {p['description']}"
                )

        return "\n".join(summary_parts)
