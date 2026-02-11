import OpenAI from 'openai';
import dotenv from 'dotenv';
import KnowledgeDatabase from './database.js';

dotenv.config();

class IdeaGenerator {
  constructor() {
    this.openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY
    });
    this.model = process.env.ANALYSIS_MODEL || 'gpt-4-turbo-preview';
    this.db = null;
  }

  async initialize() {
    this.db = new KnowledgeDatabase();
    await this.db.initialize();
    console.log('Idea Generator initialized');
  }

  async generateResearchIdeas() {
    console.log('\nGenerating research ideas based on analyzed papers...');

    // Get top papers and success patterns
    const topPapers = await this.db.getTopPapers(50);
    const patterns = await this.db.getSuccessPatterns();

    if (topPapers.length === 0) {
      console.log('No analyzed papers found. Run analysis first.');
      return [];
    }

    const ideas = [];

    // Generate ideas based on different strategies
    const strategies = [
      'extension',      // Extend existing successful research
      'combination',    // Combine multiple successful approaches
      'gap_filling',    // Identify and fill research gaps
      'replication',    // Replicate with new context/data
      'methodological'  // Apply new methods to established questions
    ];

    for (const strategy of strategies) {
      const ideaSet = await this.generateIdeasByStrategy(strategy, topPapers, patterns);
      ideas.push(...ideaSet);

      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    console.log(`✓ Generated ${ideas.length} research ideas`);

    return ideas;
  }

  async generateIdeasByStrategy(strategy, topPapers, patterns) {
    console.log(`\nGenerating ideas using ${strategy} strategy...`);

    const patternsSummary = patterns.map(p => ({
      type: p.pattern_type,
      description: p.pattern_description,
      frequency: p.frequency
    }));

    const papersSummary = topPapers.slice(0, 10).map(p => ({
      title: p.title,
      journal: p.journal,
      successScore: p.success_score,
      methodology: p.methodology,
      dataSources: p.data_sources
    }));

    const strategyPrompts = {
      extension: `Generate research ideas that EXTEND the successful papers listed below.
        Look for:
        - Natural next steps in the research stream
        - Additional contexts to test the findings
        - Temporal extensions (how findings evolve over time)
        - Geographic/cultural replications with extensions`,

      combination: `Generate research ideas that COMBINE insights from multiple successful papers.
        Look for:
        - Synergies between different theoretical frameworks
        - Integration of different methodological approaches
        - Cross-domain applications
        - Multi-method triangulation opportunities`,

      gap_filling: `Generate research ideas that FILL GAPS in the current literature.
        Look for:
        - Understudied contexts or populations
        - Unexplored mediators or moderators
        - Contradictory findings that need reconciliation
        - Missing links in theoretical frameworks`,

      replication: `Generate research ideas that REPLICATE successful studies in new ways.
        Look for:
        - Different industries or markets
        - Different time periods (especially recent trends)
        - Different data sources or measurement approaches
        - Different cultural or geographic contexts`,

      methodological: `Generate research ideas using NOVEL METHODOLOGIES.
        Look for:
        - New data sources (social media, IoT, satellite data, etc.)
        - Advanced analytical techniques (ML, NLP, causal inference)
        - Mixed methods approaches
        - Experimental designs in new contexts`
    };

    const prompt = `${strategyPrompts[strategy]}

SUCCESS PATTERNS IDENTIFIED:
${JSON.stringify(patternsSummary, null, 2)}

TOP PERFORMING PAPERS:
${JSON.stringify(papersSummary, null, 2)}

Generate 3-5 HIGH-QUALITY, PUBLISHABLE research ideas. For each idea provide:

1. Title (concise, academic)
2. Research Question (specific, answerable)
3. Theoretical Background (brief)
4. Proposed Methodology
5. Required Data (be specific about what data is needed)
6. Expected Contribution (why this would be publishable)
7. Feasibility Score (0-100)
8. Novelty Score (0-100)
9. Journals this would fit (from: JM, JMR, Marketing Science, JCR, JAMS, IJRM)
10. Papers this builds on (from the list above)

Make ideas realistic and feasible. Focus on high-impact questions that follow successful patterns.

Provide response as JSON array.`;

    try {
      const response = await this.openai.chat.completions.create({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are a senior marketing professor and journal editor with deep expertise in identifying publishable research opportunities. You understand what makes research publishable in top journals.'
          },
          { role: 'user', content: prompt }
        ],
        response_format: { type: 'json_object' },
        temperature: 0.7
      });

      const result = JSON.parse(response.choices[0].message.content);
      const ideas = result.ideas || result.research_ideas || [];

      // Save ideas to database
      for (const idea of ideas) {
        const ideaRecord = {
          title: idea.title,
          description: idea.theoretical_background || idea.description,
          basedOnPapers: idea.builds_on_papers || idea.basedOnPapers || [],
          researchQuestion: idea.research_question || idea.researchQuestion,
          proposedMethodology: idea.proposed_methodology || idea.methodology,
          requiredData: idea.required_data || idea.data_requirements || [],
          expectedContribution: idea.expected_contribution || idea.contribution,
          feasibilityScore: idea.feasibility_score || idea.feasibility || 70,
          noveltyScore: idea.novelty_score || idea.novelty || 70,
          status: 'generated'
        };

        await this.db.insertResearchIdea(ideaRecord);
      }

      console.log(`✓ Generated ${ideas.length} ideas for ${strategy} strategy`);

      return ideas;

    } catch (error) {
      console.error(`Error generating ideas for ${strategy}:`, error.message);
      return [];
    }
  }

  async refineIdea(ideaId) {
    // Get idea from database
    const ideas = await this.db.getResearchIdeas();
    const idea = ideas.find(i => i.id === ideaId);

    if (!idea) {
      console.error('Idea not found');
      return null;
    }

    console.log(`\nRefining idea: ${idea.title}`);

    const prompt = `Refine and elaborate this research idea to make it more concrete and actionable:

RESEARCH IDEA:
Title: ${idea.title}
Research Question: ${idea.research_question}
Methodology: ${idea.proposed_methodology}
Required Data: ${idea.required_data}

Please provide:
1. Detailed research design
2. Specific hypotheses to test
3. Detailed data requirements with examples of specific datasets
4. Step-by-step analytical plan
5. Expected challenges and solutions
6. Timeline estimate
7. Resource requirements

Provide response as structured JSON.`;

    try {
      const response = await this.openai.chat.completions.create({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are a research methodology consultant helping researchers develop detailed research plans.'
          },
          { role: 'user', content: prompt }
        ],
        response_format: { type: 'json_object' },
        temperature: 0.5
      });

      return JSON.parse(response.choices[0].message.content);

    } catch (error) {
      console.error('Error refining idea:', error.message);
      return null;
    }
  }

  async evaluateIdeas() {
    console.log('\nEvaluating generated ideas...');

    const ideas = await this.db.getResearchIdeas('generated');

    for (const idea of ideas) {
      // Evaluate each idea for publishability
      const evaluation = await this.evaluateIdea(idea);

      if (evaluation) {
        console.log(`\n${idea.title}`);
        console.log(`  Feasibility: ${evaluation.feasibility}/100`);
        console.log(`  Novelty: ${evaluation.novelty}/100`);
        console.log(`  Impact: ${evaluation.impact}/100`);
      }

      await new Promise(resolve => setTimeout(resolve, 1500));
    }
  }

  async evaluateIdea(idea) {
    const prompt = `Evaluate this research idea for publication potential in top marketing journals:

${JSON.stringify(idea, null, 2)}

Rate (0-100) on:
1. Feasibility (can it actually be done?)
2. Novelty (is it original?)
3. Impact (will it matter?)
4. Data Availability (can required data be obtained?)
5. Methodological Soundness

Provide brief justification for each score and overall recommendation.

Response as JSON.`;

    try {
      const response = await this.openai.chat.completions.create({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are a journal editor evaluating research proposals.'
          },
          { role: 'user', content: prompt }
        ],
        response_format: { type: 'json_object' },
        temperature: 0.3
      });

      return JSON.parse(response.choices[0].message.content);

    } catch (error) {
      console.error('Error evaluating idea:', error.message);
      return null;
    }
  }

  async generateIdeaSummaryReport() {
    const ideas = await this.db.getResearchIdeas();

    console.log('\n' + '='.repeat(80));
    console.log('RESEARCH IDEAS SUMMARY REPORT');
    console.log('='.repeat(80));

    console.log(`\nTotal Ideas Generated: ${ideas.length}\n`);

    // Sort by novelty score
    const topIdeas = ideas.sort((a, b) => b.novelty_score - a.novelty_score).slice(0, 10);

    topIdeas.forEach((idea, index) => {
      console.log(`\n${index + 1}. ${idea.title}`);
      console.log(`   Question: ${idea.research_question}`);
      console.log(`   Novelty: ${idea.novelty_score}/100 | Feasibility: ${idea.feasibility_score}/100`);
      console.log(`   Status: ${idea.status}`);
      console.log(`   Generated: ${idea.generation_date}`);
    });

    return ideas;
  }

  async close() {
    if (this.db) {
      await this.db.close();
    }
  }
}

// CLI usage
if (import.meta.url === `file://${process.argv[1]}`) {
  const generator = new IdeaGenerator();

  (async () => {
    try {
      await generator.initialize();
      await generator.generateResearchIdeas();
      await generator.generateIdeaSummaryReport();

      console.log('\n✓ Idea generation complete');
    } catch (error) {
      console.error('Error:', error);
    } finally {
      await generator.close();
    }
  })();
}

export default IdeaGenerator;
