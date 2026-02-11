"""
Research idea generator based on discovered patterns.
Combines pattern analysis with gap identification to propose novel research ideas.
"""

import json
import logging
import random
from collections import defaultdict
from typing import Any, Dict, List, Optional

from .. import config
from ..database import Database

logger = logging.getLogger(__name__)


class IdeaGenerator:
    """Generate research ideas based on patterns found in marketing literature."""

    # Templates for generating research ideas from patterns
    IDEA_TEMPLATES = [
        {
            "type": "method_transfer",
            "template": (
                "Apply {method} methodology (common in {source_domain}) "
                "to {target_domain}, which currently relies on {current_method}"
            ),
            "rationale": (
                "Cross-pollination of methods across domains often yields novel insights. "
                "{method} has proven effective in {source_domain} but is underutilized in {target_domain}."
            ),
        },
        {
            "type": "emerging_trend",
            "template": (
                "Investigate the role of {emerging_topic} in {domain} "
                "using {technique} with {data_type} data"
            ),
            "rationale": (
                "The growing trend of {emerging_topic} research suggests untapped potential. "
                "Combining it with {technique} could provide novel theoretical and practical insights."
            ),
        },
        {
            "type": "gap_filling",
            "template": (
                "Examine the {relationship} between {concept_a} and {concept_b} "
                "in the context of {context}"
            ),
            "rationale": (
                "While {concept_a} and {concept_b} have been studied separately, "
                "their interaction in {context} remains unexplored."
            ),
        },
        {
            "type": "replication_extension",
            "template": (
                "Replicate and extend findings on {topic} using {new_data} "
                "and {new_method} to test boundary conditions"
            ),
            "rationale": (
                "Replication with extension is valued in top journals. "
                "Testing boundary conditions with {new_data} adds theoretical value."
            ),
        },
        {
            "type": "multi_method",
            "template": (
                "Combine {method_a} and {method_b} to study {topic}, "
                "providing both causal evidence and external validity"
            ),
            "rationale": (
                "Multi-method designs are increasingly valued. "
                "Combining {method_a} (for internal validity) with {method_b} "
                "(for external validity) strengthens the contribution."
            ),
        },
    ]

    def __init__(self, db: Database):
        self.db = db
        self._openai_client = None

    def generate_ideas(self, num_ideas: int = 10) -> List[Dict]:
        """
        Generate research ideas based on discovered patterns.
        Returns a list of idea dicts.
        """
        patterns = self.db.get_patterns()
        analyses = self.db.get_all_analyses()

        if not patterns:
            logger.warning("No patterns found. Run pattern analysis first.")
            return []

        # Clear old ideas
        self.db.clear_ideas()

        ideas = []

        # Strategy 1: Method transfer ideas
        ideas.extend(self._generate_method_transfer_ideas(patterns, analyses))

        # Strategy 2: Emerging trend ideas
        ideas.extend(self._generate_trend_ideas(patterns, analyses))

        # Strategy 3: Gap-filling ideas
        ideas.extend(self._generate_gap_ideas(patterns, analyses))

        # Strategy 4: Multi-method combination ideas
        ideas.extend(self._generate_multi_method_ideas(patterns, analyses))

        # Strategy 5: LLM-generated ideas (if available)
        if config.OPENAI_API_KEY:
            llm_ideas = self._generate_llm_ideas(patterns, analyses)
            ideas.extend(llm_ideas)

        # Score and rank ideas
        ideas = self._score_ideas(ideas)

        # Take top N
        ideas = sorted(
            ideas,
            key=lambda x: (
                x.get("feasibility_score", 0)
                + x.get("novelty_score", 0)
                + x.get("impact_score", 0)
            ),
            reverse=True,
        )[:num_ideas]

        # Save to database (strip internal fields not in schema)
        for idea in ideas:
            save_data = {k: v for k, v in idea.items() if k != "idea_type"}
            self.db.save_idea(save_data)

        logger.info("Generated %d research ideas", len(ideas))
        return ideas

    # ─── Idea Generation Strategies ───────────────────────────────────────────

    def _generate_method_transfer_ideas(
        self, patterns: List[Dict], analyses: List[Dict]
    ) -> List[Dict]:
        """Generate ideas by transferring methods across domains."""
        ideas = []

        # Find method-domain associations
        method_domains = defaultdict(Counter_like)
        domain_methods = defaultdict(Counter_like)

        for a in analyses:
            methods = a.get("methodology_types", [])
            if isinstance(methods, str):
                try:
                    methods = json.loads(methods)
                except (json.JSONDecodeError, TypeError):
                    methods = []

            domain = a.get("research_domain", "general_marketing")

            for m in methods:
                method_domains[m][domain] = method_domains[m].get(domain, 0) + 1
                domain_methods[domain][m] = domain_methods[domain].get(m, 0) + 1

        # Find methods common in one domain but rare in another
        all_domains = list(domain_methods.keys())
        all_methods = list(method_domains.keys())

        for method in all_methods:
            if method == "unclassified":
                continue

            domains_using = method_domains[method]
            if len(domains_using) < 1:
                continue

            # Find the domain where this method is most common
            source_domain = max(domains_using, key=domains_using.get)

            # Find domains where this method is rare or absent
            for target_domain in all_domains:
                if target_domain == source_domain:
                    continue
                if domains_using.get(target_domain, 0) <= 1:
                    # This method is rare in target_domain
                    current_methods = domain_methods[target_domain]
                    current_top = max(current_methods, key=current_methods.get) if current_methods else "traditional"

                    ideas.append({
                        "title": (
                            f"Applying {method.replace('_', ' ')} to "
                            f"{target_domain.replace('_', ' ')} research"
                        ),
                        "description": (
                            f"Transfer {method.replace('_', ' ')} methodology, "
                            f"which is well-established in {source_domain.replace('_', ' ')} research, "
                            f"to {target_domain.replace('_', ' ')} where it is currently underutilized. "
                            f"This domain currently relies primarily on {current_top.replace('_', ' ')}."
                        ),
                        "research_questions": [
                            f"How can {method.replace('_', ' ')} provide new insights in {target_domain.replace('_', ' ')}?",
                            f"What are the boundary conditions for applying {method.replace('_', ' ')} in this context?",
                        ],
                        "rationale": (
                            f"Cross-pollination of methods across domains often yields novel insights. "
                            f"{method.replace('_', ' ').title()} has proven effective in "
                            f"{source_domain.replace('_', ' ')} but is underutilized in "
                            f"{target_domain.replace('_', ' ')}."
                        ),
                        "suggested_methodology": [method],
                        "suggested_techniques": [],
                        "target_journals": self._suggest_journals(target_domain, patterns),
                        "idea_type": "method_transfer",
                    })

        return ideas[:5]

    def _generate_trend_ideas(
        self, patterns: List[Dict], analyses: List[Dict]
    ) -> List[Dict]:
        """Generate ideas based on emerging trends."""
        ideas = []

        # Find temporal patterns (growing trends)
        growing_trends = [
            p for p in patterns
            if p.get("pattern_type") == "temporal"
            and p.get("details", {}).get("trend") == "growing"
        ]

        # Find underexplored domains
        domain_patterns = [
            p for p in patterns if p.get("pattern_type") == "domain"
        ]
        domains_by_freq = sorted(domain_patterns, key=lambda x: x.get("frequency", 0))

        for trend in growing_trends:
            method = trend.get("details", {}).get("method", "")
            if not method or method == "unclassified":
                continue

            # Pair growing method with less-explored domains
            for domain_p in domains_by_freq[:3]:
                domain = domain_p.get("details", {}).get("domain", "")
                if not domain:
                    continue

                ideas.append({
                    "title": (
                        f"Leveraging the rise of {method.replace('_', ' ')} "
                        f"in {domain.replace('_', ' ')}"
                    ),
                    "description": (
                        f"The use of {method.replace('_', ' ')} is growing rapidly in marketing research. "
                        f"Apply this emerging approach to {domain.replace('_', ' ')}, "
                        f"which has received relatively less attention."
                    ),
                    "research_questions": [
                        f"How does {method.replace('_', ' ')} change our understanding of {domain.replace('_', ' ')}?",
                        f"What new phenomena can be uncovered using {method.replace('_', ' ')} in this domain?",
                    ],
                    "rationale": (
                        f"Riding the wave of a growing methodological trend while "
                        f"applying it to an underexplored domain maximizes both "
                        f"timeliness and novelty."
                    ),
                    "suggested_methodology": [method],
                    "target_journals": self._suggest_journals(domain, patterns),
                    "idea_type": "emerging_trend",
                })

        return ideas[:4]

    def _generate_gap_ideas(
        self, patterns: List[Dict], analyses: List[Dict]
    ) -> List[Dict]:
        """Generate ideas by identifying gaps in the literature."""
        ideas = []

        # Collect future directions from analyses
        all_directions = []
        for a in analyses:
            directions = a.get("future_directions", [])
            if isinstance(directions, str):
                try:
                    directions = json.loads(directions)
                except (json.JSONDecodeError, TypeError):
                    directions = [directions] if directions else []
            all_directions.extend(directions)

        # Collect limitations
        all_limitations = []
        for a in analyses:
            lim = a.get("limitations", "")
            if lim:
                all_limitations.append(lim)

        # Generate ideas from future directions
        for direction in all_directions[:5]:
            if len(direction) < 20:
                continue

            ideas.append({
                "title": f"Addressing: {direction[:80]}",
                "description": (
                    f"Multiple papers in top marketing journals have identified "
                    f"this as a promising future direction: {direction}"
                ),
                "research_questions": [
                    direction if "?" in direction else f"Can we {direction}?",
                ],
                "rationale": (
                    "This direction was explicitly suggested by published authors, "
                    "indicating both a recognized gap and editorial receptiveness."
                ),
                "suggested_methodology": [],
                "target_journals": [],
                "idea_type": "gap_filling",
            })

        return ideas[:4]

    def _generate_multi_method_ideas(
        self, patterns: List[Dict], analyses: List[Dict]
    ) -> List[Dict]:
        """Generate ideas combining multiple methods."""
        ideas = []

        # Find common method combinations
        cross_patterns = [
            p for p in patterns if p.get("pattern_type") == "cross_method"
        ]

        # Find methods that are rarely combined
        method_counts = defaultdict(int)
        for a in analyses:
            methods = a.get("methodology_types", [])
            if isinstance(methods, str):
                try:
                    methods = json.loads(methods)
                except (json.JSONDecodeError, TypeError):
                    methods = []
            for m in methods:
                method_counts[m] += 1

        common_methods = [m for m, c in sorted(method_counts.items(), key=lambda x: -x[1]) if c >= 3]

        # Suggest novel combinations
        for i, method_a in enumerate(common_methods):
            for method_b in common_methods[i + 1:]:
                if method_a == "unclassified" or method_b == "unclassified":
                    continue

                # Check if this combination is already common
                combo_key = f"{method_a} + {method_b}"
                already_common = any(
                    combo_key in str(p.get("details", {}))
                    for p in cross_patterns
                )

                if not already_common:
                    ideas.append({
                        "title": (
                            f"Multi-method study combining "
                            f"{method_a.replace('_', ' ')} and {method_b.replace('_', ' ')}"
                        ),
                        "description": (
                            f"Design a multi-method study that combines "
                            f"{method_a.replace('_', ' ')} (for one type of evidence) "
                            f"with {method_b.replace('_', ' ')} (for complementary evidence). "
                            f"This combination is novel and addresses calls for methodological triangulation."
                        ),
                        "research_questions": [
                            f"How do findings from {method_a.replace('_', ' ')} converge with {method_b.replace('_', ' ')}?",
                        ],
                        "rationale": (
                            "Multi-method designs are increasingly valued in top journals. "
                            "This novel combination provides both internal and external validity."
                        ),
                        "suggested_methodology": [method_a, method_b],
                        "target_journals": [],
                        "idea_type": "multi_method",
                    })

        return ideas[:3]

    # ─── LLM-Enhanced Idea Generation ─────────────────────────────────────────

    def _generate_llm_ideas(
        self, patterns: List[Dict], analyses: List[Dict]
    ) -> List[Dict]:
        """Use LLM to generate creative research ideas."""
        client = self._get_openai_client()
        if not client:
            return []

        # Prepare pattern summary for the LLM
        pattern_summary = []
        for p in patterns[:30]:
            pattern_summary.append(f"- {p['description']}")

        prompt = f"""You are a creative marketing research professor at a top business school.
Based on the following patterns discovered in recent top marketing journal publications,
generate 5 novel and feasible research ideas.

PATTERNS DISCOVERED:
{chr(10).join(pattern_summary)}

For each idea, provide:
1. Title (concise, academic)
2. Description (2-3 sentences)
3. Research questions (2-3 specific questions)
4. Suggested methodology
5. Why this would be publishable in a top journal
6. Target journals (from: JM, JMR, Marketing Science, JCR, JAMS, IJRM)

Focus on ideas that are:
- Novel but feasible
- Theoretically grounded
- Methodologically rigorous
- Practically relevant
- Timely (addressing current marketing challenges)

Return as a JSON array of objects."""

        try:
            response = client.chat.completions.create(
                model=config.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You are a top marketing research professor."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7,
                max_tokens=3000,
            )

            content = response.choices[0].message.content
            # Parse JSON from response
            import re
            json_match = re.search(r"\[[\s\S]*\]", content)
            if json_match:
                llm_ideas = json.loads(json_match.group())
                ideas = []
                for item in llm_ideas:
                    ideas.append({
                        "title": item.get("title", item.get("Title", "")),
                        "description": item.get("description", item.get("Description", "")),
                        "research_questions": item.get("research_questions", item.get("Research questions", [])),
                        "rationale": item.get("rationale", item.get("Why this would be publishable", "")),
                        "suggested_methodology": item.get("suggested_methodology", item.get("Suggested methodology", [])),
                        "target_journals": item.get("target_journals", item.get("Target journals", [])),
                        "idea_type": "llm_generated",
                    })
                return ideas

        except Exception as e:
            logger.warning("LLM idea generation failed: %s", e)

        return []

    # ─── Scoring & Utilities ──────────────────────────────────────────────────

    def _score_ideas(self, ideas: List[Dict]) -> List[Dict]:
        """Score ideas on feasibility, novelty, and impact."""
        for idea in ideas:
            # Feasibility: based on methodology availability and data accessibility
            feasibility = 0.5  # baseline
            methods = idea.get("suggested_methodology", [])
            if methods:
                # Common methods are more feasible
                common_methods = ["experiment", "survey", "archival"]
                if any(m in common_methods for m in methods):
                    feasibility += 0.2
                if len(methods) <= 2:
                    feasibility += 0.1
            idea["feasibility_score"] = min(1.0, feasibility)

            # Novelty: based on idea type
            novelty_by_type = {
                "method_transfer": 0.7,
                "emerging_trend": 0.6,
                "gap_filling": 0.65,
                "multi_method": 0.6,
                "llm_generated": 0.75,
            }
            idea["novelty_score"] = novelty_by_type.get(
                idea.get("idea_type", ""), 0.5
            )

            # Impact: based on target journal tier and domain relevance
            impact = 0.5
            target_journals = idea.get("target_journals", [])
            if target_journals:
                impact += 0.1 * min(len(target_journals), 3)
            idea["impact_score"] = min(1.0, impact)

        return ideas

    def _suggest_journals(self, domain: str, patterns: List[Dict]) -> List[str]:
        """Suggest target journals based on domain and patterns."""
        journal_prefs = [
            p for p in patterns if p.get("pattern_type") == "journal_preference"
        ]

        suggested = []
        for pref in journal_prefs:
            details = pref.get("details", {})
            top_domains = details.get("top_domains", {})
            if domain in top_domains:
                journals = pref.get("journals", [])
                suggested.extend(journals)

        # Default suggestions if none found
        if not suggested:
            domain_journal_map = {
                "consumer_behavior": ["JCR", "JMR", "JM"],
                "digital_marketing": ["MKSC", "JMR", "JM"],
                "branding": ["JM", "JCR", "JMR"],
                "pricing": ["MKSC", "JMR", "JM"],
                "advertising": ["JM", "JMR", "JCR"],
                "marketing_strategy": ["JM", "MKSC", "JAMS"],
                "marketing_analytics": ["MKSC", "JMR", "JAMS"],
                "services_marketing": ["JM", "JAMS", "JMR"],
                "retailing": ["JMR", "JM", "MKSC"],
            }
            suggested = domain_journal_map.get(domain, ["JM", "JMR", "MKSC"])

        return list(dict.fromkeys(suggested))[:3]

    def _get_openai_client(self):
        """Lazily initialize OpenAI client."""
        if self._openai_client is None and config.OPENAI_API_KEY:
            try:
                import openai
                self._openai_client = openai.OpenAI(api_key=config.OPENAI_API_KEY)
            except ImportError:
                logger.warning("OpenAI not installed.")
        return self._openai_client

    def get_ideas_summary(self) -> str:
        """Generate a human-readable summary of all ideas."""
        ideas = self.db.get_ideas()
        if not ideas:
            return "No ideas generated yet. Run the idea generator first."

        parts = []
        parts.append("=" * 70)
        parts.append("GENERATED RESEARCH IDEAS")
        parts.append("=" * 70)

        for i, idea in enumerate(ideas, 1):
            total_score = (
                idea.get("feasibility_score", 0)
                + idea.get("novelty_score", 0)
                + idea.get("impact_score", 0)
            )
            parts.append(f"\n{'─' * 60}")
            parts.append(f"  IDEA {i}: {idea['title']}")
            parts.append(f"  Score: {total_score:.2f}/3.0 "
                        f"(F:{idea.get('feasibility_score', 0):.1f} "
                        f"N:{idea.get('novelty_score', 0):.1f} "
                        f"I:{idea.get('impact_score', 0):.1f})")
            parts.append(f"{'─' * 60}")
            parts.append(f"  {idea['description']}")

            rqs = idea.get("research_questions", [])
            if isinstance(rqs, str):
                try:
                    rqs = json.loads(rqs)
                except (json.JSONDecodeError, TypeError):
                    rqs = [rqs]
            if rqs:
                parts.append("\n  Research Questions:")
                for rq in rqs:
                    parts.append(f"    • {rq}")

            parts.append(f"\n  Rationale: {idea.get('rationale', 'N/A')}")

            methods = idea.get("suggested_methodology", [])
            if isinstance(methods, str):
                try:
                    methods = json.loads(methods)
                except (json.JSONDecodeError, TypeError):
                    methods = [methods]
            if methods:
                parts.append(f"  Suggested Methods: {', '.join(methods)}")

            journals = idea.get("target_journals", [])
            if isinstance(journals, str):
                try:
                    journals = json.loads(journals)
                except (json.JSONDecodeError, TypeError):
                    journals = [journals]
            if journals:
                parts.append(f"  Target Journals: {', '.join(journals)}")

        return "\n".join(parts)


class Counter_like(dict):
    """Simple counter-like dict for compatibility."""
    pass
