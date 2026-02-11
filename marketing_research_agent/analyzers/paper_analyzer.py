"""
Paper analyzer that extracts publishability factors from academic papers.
Uses keyword-based NLP analysis with optional LLM enhancement.
"""

import json
import logging
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from .. import config
from ..database import Database

logger = logging.getLogger(__name__)


class PaperAnalyzer:
    """Analyze papers to extract what made them publishable and successful."""

    def __init__(self, db: Database):
        self.db = db
        self._openai_client = None

    # ─── Main Analysis Entry Point ────────────────────────────────────────────

    def analyze_paper(self, paper: Dict) -> Optional[Dict]:
        """
        Analyze a single paper and return the analysis dict.
        Uses full text if available, otherwise falls back to abstract.
        """
        text = paper.get("full_text") or paper.get("abstract") or ""
        if not text:
            logger.warning(
                "No text available for paper: %s", paper.get("title", "Unknown")
            )
            return None

        title = paper.get("title", "")
        abstract = paper.get("abstract", "")

        analysis = {
            "paper_id": paper["id"],
            "methodology_types": self._detect_methodology(text),
            "data_type": self._detect_data_type(text),
            "data_sources": self._extract_data_sources(text),
            "sample_description": self._extract_sample_info(text),
            "sample_size": self._extract_sample_size(text),
            "analytical_techniques": self._detect_analytical_techniques(text),
            "theoretical_frameworks": self._extract_theories(text),
            "key_contributions": self._extract_contributions(text, abstract),
            "key_findings": self._extract_key_findings(abstract),
            "research_domain": self._classify_domain(text, title),
            "publishability_factors": self._assess_publishability(paper, text),
            "novelty_assessment": self._assess_novelty(text, abstract),
            "practical_implications": self._extract_implications(text),
            "limitations": self._extract_limitations(text),
            "future_directions": self._extract_future_directions(text),
        }

        # Try LLM-enhanced analysis if API key is available
        if config.OPENAI_API_KEY:
            llm_analysis = self._llm_analyze(paper, text)
            if llm_analysis:
                analysis["raw_llm_analysis"] = llm_analysis
                self._merge_llm_analysis(analysis, llm_analysis)

        return analysis

    def analyze_all_papers(self, limit: int = 500) -> int:
        """Analyze all unanalyzed papers. Returns count of papers analyzed."""
        papers = self.db.get_unanalyzed_papers(limit=limit)
        analyzed = 0

        for paper in papers:
            analysis = self.analyze_paper(paper)
            if analysis:
                self.db.save_analysis(analysis)
                analyzed += 1
                if analyzed % 20 == 0:
                    logger.info("  Analyzed %d/%d papers", analyzed, len(papers))

        logger.info("Analyzed %d papers total", analyzed)
        return analyzed

    # ─── Methodology Detection ────────────────────────────────────────────────

    def _detect_methodology(self, text: str) -> List[str]:
        """Detect methodology types used in the paper."""
        text_lower = text.lower()
        detected = []

        for method_type, keywords in config.METHODOLOGY_KEYWORDS.items():
            score = sum(
                1 for kw in keywords
                if kw.lower() in text_lower
            )
            # Require at least 2 keyword matches for confidence
            if score >= 2:
                detected.append(method_type)

        return detected if detected else ["unclassified"]

    def _detect_data_type(self, text: str) -> str:
        """Classify the data type as primary, secondary, or both."""
        text_lower = text.lower()

        primary_signals = [
            "we collected", "we gathered", "we conducted",
            "our survey", "our experiment", "we recruited",
            "participants were", "respondents completed",
            "we designed", "we administered",
        ]
        secondary_signals = [
            "archival data", "secondary data", "publicly available",
            "obtained from", "downloaded from", "panel data from",
            "scanner data", "transaction records", "administrative data",
            "we obtained", "data were obtained",
        ]

        has_primary = any(s in text_lower for s in primary_signals)
        has_secondary = any(s in text_lower for s in secondary_signals)

        if has_primary and has_secondary:
            return "both"
        elif has_primary:
            return "primary"
        elif has_secondary:
            return "secondary"
        return "unknown"

    def _extract_data_sources(self, text: str) -> List[str]:
        """Extract specific data sources mentioned in the paper."""
        sources = []
        text_lower = text.lower()

        # Common marketing data sources
        known_sources = {
            "mturk": "Amazon Mechanical Turk",
            "mechanical turk": "Amazon Mechanical Turk",
            "prolific": "Prolific",
            "qualtrics": "Qualtrics",
            "nielsen": "Nielsen",
            "iri": "IRI",
            "compustat": "Compustat",
            "crsp": "CRSP",
            "euromonitor": "Euromonitor",
            "mintel": "Mintel",
            "simmons": "Simmons",
            "kantar": "Kantar",
            "gfk": "GfK",
            "twitter": "Twitter/X",
            "yelp": "Yelp",
            "amazon review": "Amazon Reviews",
            "google trend": "Google Trends",
            "facebook": "Facebook",
            "instagram": "Instagram",
            "reddit": "Reddit",
            "census": "Census Data",
            "bls": "Bureau of Labor Statistics",
            "world bank": "World Bank",
            "eurobarometer": "Eurobarometer",
        }

        for keyword, source_name in known_sources.items():
            if keyword in text_lower:
                sources.append(source_name)

        # Extract "data from X" patterns
        patterns = [
            r"data (?:from|provided by|obtained from|collected from)\s+([A-Z][^,.;]{3,50})",
            r"(?:using|used)\s+(?:the\s+)?([A-Z][^,.;]{3,40})\s+(?:data|dataset|database|panel)",
            r"(?:survey|panel)\s+(?:by|from|conducted by)\s+([A-Z][^,.;]{3,40})",
        ]
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                clean = match.strip()
                if 3 < len(clean) < 60 and clean not in sources:
                    sources.append(clean)

        return list(set(sources))[:10]

    def _extract_sample_size(self, text: str) -> str:
        """Extract sample size information."""
        patterns = [
            r"(?:n|N)\s*=\s*(\d[\d,]*)",
            r"(\d[\d,]*)\s+(?:participants|respondents|consumers|subjects|observations|firms|customers|users)",
            r"sample\s+(?:of|size[:\s]+)(\d[\d,]*)",
            r"(\d[\d,]*)\s+(?:usable|valid|complete)\s+(?:responses|surveys|questionnaires)",
        ]

        sizes = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    n = int(match.replace(",", ""))
                    if 10 <= n <= 10_000_000:
                        sizes.append(n)
                except ValueError:
                    continue

        if sizes:
            if len(sizes) == 1:
                return str(sizes[0])
            return f"{min(sizes)}-{max(sizes)} (across studies)"
        return "not specified"

    def _extract_sample_info(self, text: str) -> str:
        """Extract sample description."""
        text_lower = text.lower()
        descriptions = []

        sample_types = {
            "student": "student sample",
            "mturk": "MTurk workers",
            "prolific": "Prolific participants",
            "consumer panel": "consumer panel",
            "nationally representative": "nationally representative sample",
            "undergraduate": "undergraduate students",
            "mba student": "MBA students",
            "manager": "managers",
            "executive": "executives",
            "online panel": "online panel participants",
            "field": "field sample",
        }

        for keyword, description in sample_types.items():
            if keyword in text_lower:
                descriptions.append(description)

        return "; ".join(descriptions) if descriptions else "not specified"

    # ─── Analytical Techniques Detection ──────────────────────────────────────

    def _detect_analytical_techniques(self, text: str) -> List[str]:
        """Detect analytical techniques used in the paper."""
        text_lower = text.lower()
        detected = []

        for technique_type, keywords in config.ANALYTICAL_TECHNIQUES.items():
            score = sum(1 for kw in keywords if kw.lower() in text_lower)
            if score >= 2:
                detected.append(technique_type)

        # Also detect specific software/tools
        tools = {
            "stata": "Stata",
            "spss": "SPSS",
            "r software": "R",
            "mplus": "Mplus",
            "amos": "AMOS",
            "lisrel": "LISREL",
            "python": "Python",
            "matlab": "MATLAB",
        }
        for keyword, tool_name in tools.items():
            if keyword in text_lower:
                detected.append(f"software:{tool_name}")

        return detected if detected else ["unclassified"]

    # ─── Theory Extraction ────────────────────────────────────────────────────

    def _extract_theories(self, text: str) -> List[str]:
        """Extract theoretical frameworks mentioned in the paper."""
        text_lower = text.lower()
        theories = []

        known_theories = [
            "resource-based view", "rbv",
            "theory of planned behavior", "tpb",
            "technology acceptance model", "tam",
            "elaboration likelihood model", "elm",
            "signaling theory",
            "agency theory",
            "institutional theory",
            "social identity theory",
            "self-determination theory",
            "prospect theory",
            "construal level theory",
            "regulatory focus theory",
            "attribution theory",
            "social exchange theory",
            "cognitive dissonance",
            "dual process theory",
            "information processing theory",
            "schema theory",
            "equity theory",
            "expectancy theory",
            "goal-setting theory",
            "self-concept",
            "identity-based motivation",
            "psychological ownership",
            "mental accounting",
            "anchoring",
            "framing effect",
            "endowment effect",
            "status quo bias",
            "social comparison theory",
            "social learning theory",
            "network theory",
            "diffusion of innovation",
            "service-dominant logic",
            "customer engagement",
            "relationship marketing",
            "brand equity",
            "customer lifetime value",
        ]

        for theory in known_theories:
            if theory.lower() in text_lower:
                theories.append(theory)

        # Extract "X theory" patterns
        theory_patterns = re.findall(
            r"([A-Z][a-z]+(?:\s+[A-Z]?[a-z]+)*)\s+(?:theory|framework|model|perspective)",
            text,
        )
        for match in theory_patterns:
            clean = match.strip()
            if 3 < len(clean) < 50 and clean.lower() not in [t.lower() for t in theories]:
                theories.append(clean)

        return list(set(theories))[:15]

    # ─── Contribution & Findings Extraction ───────────────────────────────────

    def _extract_contributions(self, text: str, abstract: str) -> List[str]:
        """Extract key contributions claimed by the paper."""
        contributions = []
        search_text = abstract + "\n" + text[:5000]  # Focus on intro/abstract

        patterns = [
            r"(?:we|this (?:paper|study|research|article))\s+contribut[es]+\s+(?:to\s+)?([^.]{20,150})\.",
            r"(?:our|the)\s+contribution[s]?\s+(?:is|are|include)\s+([^.]{20,150})\.",
            r"(?:first|second|third|finally),?\s+(?:we|this study)\s+([^.]{20,150})\.",
            r"this is the first\s+([^.]{20,150})\.",
            r"novel(?:ty)?\s+(?:of|in)\s+([^.]{20,150})\.",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, search_text, re.IGNORECASE)
            for match in matches:
                clean = match.strip()
                if 20 < len(clean) < 200:
                    contributions.append(clean)

        return contributions[:5]

    def _extract_key_findings(self, abstract: str) -> str:
        """Extract key findings from the abstract."""
        if not abstract:
            return ""

        # Look for results/findings sentences
        sentences = re.split(r"(?<=[.!?])\s+", abstract)
        finding_sentences = []

        finding_signals = [
            "find that", "found that", "show that", "showed that",
            "demonstrate", "reveal", "suggest that", "indicate",
            "result", "effect", "impact", "influence",
        ]

        for sentence in sentences:
            if any(signal in sentence.lower() for signal in finding_signals):
                finding_sentences.append(sentence.strip())

        return " ".join(finding_sentences[:3])

    # ─── Domain Classification ────────────────────────────────────────────────

    def _classify_domain(self, text: str, title: str) -> str:
        """Classify the research domain of the paper."""
        combined = (title + " " + text[:3000]).lower()

        domains = {
            "consumer_behavior": [
                "consumer", "purchase", "buying", "shopping", "choice",
                "preference", "attitude", "satisfaction", "loyalty",
            ],
            "digital_marketing": [
                "online", "digital", "social media", "e-commerce",
                "platform", "algorithm", "recommendation", "click",
                "search engine", "website", "app",
            ],
            "branding": [
                "brand", "branding", "brand equity", "brand extension",
                "brand loyalty", "brand image", "logo",
            ],
            "pricing": [
                "price", "pricing", "willingness to pay", "discount",
                "promotion", "coupon", "dynamic pricing",
            ],
            "advertising": [
                "advertising", "ad", "commercial", "creative",
                "media", "exposure", "persuasion", "endorsement",
            ],
            "sales_management": [
                "sales", "salesperson", "sales force", "B2B",
                "selling", "negotiation", "account management",
            ],
            "product_innovation": [
                "new product", "innovation", "product design",
                "product development", "launch", "diffusion",
            ],
            "services_marketing": [
                "service", "customer experience", "service quality",
                "complaint", "recovery", "frontline",
            ],
            "marketing_strategy": [
                "strategy", "competitive", "market entry",
                "market share", "positioning", "segmentation",
            ],
            "marketing_analytics": [
                "analytics", "big data", "machine learning",
                "prediction", "forecasting", "text mining",
            ],
            "sustainability": [
                "sustainability", "green", "ethical", "CSR",
                "environmental", "social responsibility",
            ],
            "retailing": [
                "retail", "store", "shelf", "assortment",
                "omnichannel", "brick-and-mortar",
            ],
        }

        scores = {}
        for domain, keywords in domains.items():
            score = sum(1 for kw in keywords if kw in combined)
            if score > 0:
                scores[domain] = score

        if scores:
            return max(scores, key=scores.get)
        return "general_marketing"

    # ─── Publishability Assessment ────────────────────────────────────────────

    def _assess_publishability(self, paper: Dict, text: str) -> Dict[str, float]:
        """
        Assess factors that contributed to publishability.
        Returns a dict of factor -> score (0-1).
        """
        factors = {}

        # 1. Methodological rigor
        methods = self._detect_methodology(text)
        techniques = self._detect_analytical_techniques(text)
        factors["methodological_rigor"] = min(
            1.0, (len(methods) * 0.2 + len(techniques) * 0.15)
        )

        # 2. Multi-study design
        study_count = len(re.findall(
            r"(?i)(?:study|experiment)\s+\d", text
        ))
        factors["multi_study"] = min(1.0, study_count * 0.25)

        # 3. Sample quality
        text_lower = text.lower()
        sample_score = 0.3  # baseline
        if "nationally representative" in text_lower:
            sample_score += 0.3
        if "field" in text_lower and ("experiment" in text_lower or "data" in text_lower):
            sample_score += 0.2
        if any(s in text_lower for s in ["longitudinal", "panel data", "over time"]):
            sample_score += 0.2
        factors["sample_quality"] = min(1.0, sample_score)

        # 4. Theoretical contribution
        theories = self._extract_theories(text)
        contributions = self._extract_contributions(text, paper.get("abstract", ""))
        factors["theoretical_contribution"] = min(
            1.0, len(theories) * 0.1 + len(contributions) * 0.15
        )

        # 5. Practical relevance
        practical_signals = [
            "managerial", "practical", "implication", "practitioner",
            "manager", "firm", "company", "industry",
        ]
        practical_count = sum(1 for s in practical_signals if s in text_lower)
        factors["practical_relevance"] = min(1.0, practical_count * 0.12)

        # 6. Novelty
        novelty_signals = [
            "first to", "novel", "new approach", "unique",
            "unexplored", "gap in", "overlooked", "underexplored",
        ]
        novelty_count = sum(1 for s in novelty_signals if s in text_lower)
        factors["novelty"] = min(1.0, novelty_count * 0.15)

        # 7. Robustness
        robustness_signals = [
            "robustness", "robust", "alternative specification",
            "sensitivity analysis", "placebo", "falsification",
            "endogeneity", "selection bias", "omitted variable",
        ]
        robustness_count = sum(1 for s in robustness_signals if s in text_lower)
        factors["robustness_checks"] = min(1.0, robustness_count * 0.15)

        # 8. Citation impact (if available)
        citations = paper.get("citation_count", 0)
        if citations > 0:
            # Log scale: 10 citations = 0.3, 50 = 0.5, 200 = 0.7, 1000 = 0.9
            import math
            factors["citation_impact"] = min(1.0, math.log10(citations + 1) / 3.5)
        else:
            factors["citation_impact"] = 0.0

        return factors

    def _assess_novelty(self, text: str, abstract: str) -> str:
        """Assess the novelty of the paper."""
        combined = (abstract + " " + text[:3000]).lower()

        novelty_indicators = []
        if "first" in combined and any(
            w in combined for w in ["to show", "to demonstrate", "to examine", "to study"]
        ):
            novelty_indicators.append("claims first-mover contribution")
        if "novel" in combined or "new approach" in combined:
            novelty_indicators.append("novel methodology or approach")
        if "gap" in combined and "literature" in combined:
            novelty_indicators.append("addresses literature gap")
        if "unexplored" in combined or "underexplored" in combined:
            novelty_indicators.append("explores underexplored area")
        if "reconcile" in combined or "integrate" in combined:
            novelty_indicators.append("integrates disparate streams")

        return "; ".join(novelty_indicators) if novelty_indicators else "standard contribution"

    def _extract_implications(self, text: str) -> str:
        """Extract practical implications."""
        text_lower = text.lower()

        # Find implications section
        impl_start = -1
        for marker in ["managerial implications", "practical implications", "implications for practice"]:
            idx = text_lower.find(marker)
            if idx != -1:
                impl_start = idx
                break

        if impl_start != -1:
            # Extract ~500 chars after the marker
            snippet = text[impl_start:impl_start + 500]
            # Clean up
            sentences = re.split(r"(?<=[.!?])\s+", snippet)
            return " ".join(sentences[:3])

        return ""

    def _extract_limitations(self, text: str) -> str:
        """Extract stated limitations."""
        text_lower = text.lower()

        lim_start = -1
        for marker in ["limitation", "caveat"]:
            idx = text_lower.find(marker)
            if idx != -1:
                lim_start = idx
                break

        if lim_start != -1:
            snippet = text[lim_start:lim_start + 500]
            sentences = re.split(r"(?<=[.!?])\s+", snippet)
            return " ".join(sentences[:3])

        return ""

    def _extract_future_directions(self, text: str) -> List[str]:
        """Extract future research directions."""
        directions = []
        text_lower = text.lower()

        patterns = [
            r"future\s+research\s+(?:could|should|might|may)\s+([^.]{20,150})\.",
            r"(?:avenue|direction)\s+for\s+future\s+research[:\s]+([^.]{20,150})\.",
            r"it would be (?:interesting|valuable|worthwhile)\s+to\s+([^.]{20,150})\.",
            r"researchers\s+(?:could|should|might)\s+([^.]{20,150})\.",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                clean = match.strip()
                if 20 < len(clean) < 200:
                    directions.append(clean)

        return directions[:5]

    # ─── LLM-Enhanced Analysis ────────────────────────────────────────────────

    def _get_openai_client(self):
        """Lazily initialize OpenAI client."""
        if self._openai_client is None and config.OPENAI_API_KEY:
            try:
                import openai
                self._openai_client = openai.OpenAI(api_key=config.OPENAI_API_KEY)
            except ImportError:
                logger.warning("OpenAI package not installed. Using rule-based analysis only.")
        return self._openai_client

    def _llm_analyze(self, paper: Dict, text: str) -> Optional[str]:
        """Use LLM for deeper analysis of the paper."""
        client = self._get_openai_client()
        if not client:
            return None

        # Truncate text to fit context window
        max_chars = 12000
        analysis_text = text[:max_chars] if len(text) > max_chars else text

        prompt = f"""Analyze this marketing research paper and identify what made it publishable in a top journal.

Title: {paper.get('title', 'Unknown')}
Journal: {paper.get('journal_name', 'Unknown')}
Year: {paper.get('year', 'Unknown')}
Citations: {paper.get('citation_count', 0)}

Paper text (truncated):
{analysis_text}

Please analyze:
1. METHODOLOGY: What research methods were used? (experiments, surveys, archival data, etc.)
2. DATA: What data sources and sample sizes were used?
3. ANALYTICAL TECHNIQUES: What statistical/analytical methods were employed?
4. THEORETICAL CONTRIBUTION: What theories were used/extended?
5. KEY NOVELTY: What is genuinely new about this paper?
6. PUBLISHABILITY FACTORS: Why was this paper accepted at a top journal?
7. PRACTICAL IMPLICATIONS: What are the managerial takeaways?
8. LIMITATIONS & FUTURE DIRECTIONS: What gaps remain?

Provide a structured analysis in JSON format."""

        try:
            response = client.chat.completions.create(
                model=config.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You are an expert marketing academic reviewer."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=2000,
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.warning("LLM analysis failed: %s", e)
            return None

    def _merge_llm_analysis(self, analysis: Dict, llm_response: str):
        """Merge LLM analysis results into the analysis dict."""
        # Try to parse JSON from LLM response
        try:
            # Find JSON block in response
            json_match = re.search(r"\{[\s\S]*\}", llm_response)
            if json_match:
                llm_data = json.loads(json_match.group())
                # Merge specific fields if they provide richer data
                if "novelty" in llm_data:
                    analysis["novelty_assessment"] = str(llm_data["novelty"])
                if "practical_implications" in llm_data:
                    analysis["practical_implications"] = str(llm_data["practical_implications"])
        except (json.JSONDecodeError, AttributeError):
            pass  # Keep rule-based analysis
