import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Journal configurations
const JOURNALS = {
  JM: {
    name: 'Journal of Marketing',
    url: process.env.JM_URL || 'https://journals.sagepub.com/home/jmx',
    publisher: 'SAGE'
  },
  JMR: {
    name: 'Journal of Marketing Research',
    url: process.env.JMR_URL || 'https://journals.sagepub.com/home/mrj',
    publisher: 'SAGE'
  },
  MS: {
    name: 'Marketing Science',
    url: process.env.MS_URL || 'https://pubsonline.informs.org/journal/mksc',
    publisher: 'INFORMS'
  },
  JCR: {
    name: 'Journal of Consumer Research',
    url: process.env.JCR_URL || 'https://academic.oup.com/jcr',
    publisher: 'OUP'
  },
  JAMS: {
    name: 'Journal of the Academy of Marketing Science',
    url: process.env.JAMS_URL || 'https://link.springer.com/journal/11747',
    publisher: 'Springer'
  },
  IJRM: {
    name: 'International Journal of Research in Marketing',
    url: process.env.IJRM_URL || 'https://www.sciencedirect.com/journal/international-journal-of-research-in-marketing',
    publisher: 'Elsevier'
  }
};

class PaperDownloader {
  constructor() {
    this.browser = null;
    this.page = null;
    this.downloadPath = path.join(process.cwd(), 'downloads');
    this.ensureDownloadDirectory();
  }

  ensureDownloadDirectory() {
    if (!fs.existsSync(this.downloadPath)) {
      fs.mkdirSync(this.downloadPath, { recursive: true });
    }
  }

  async initialize() {
    console.log('Initializing browser...');
    this.browser = await puppeteer.launch({
      headless: process.env.HEADLESS_MODE === 'true',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });
    this.page = await this.browser.newPage();

    // Set download behavior
    const client = await this.page.target().createCDPSession();
    await client.send('Page.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath: this.downloadPath
    });

    console.log('Browser initialized successfully');
  }

  async authenticateLibrary() {
    console.log('Authenticating with library...');

    const institutionUrl = process.env.LIBRARY_INSTITUTION_URL;
    if (!institutionUrl) {
      console.warn('No institution URL provided. Skipping authentication.');
      return;
    }

    try {
      await this.page.goto(institutionUrl, { waitUntil: 'networkidle2' });

      // Wait for username field (adjust selectors based on your institution)
      await this.page.waitForSelector('input[name="username"], input[type="text"]', { timeout: 5000 });

      await this.page.type('input[name="username"], input[type="text"]', process.env.LIBRARY_USERNAME);
      await this.page.type('input[name="password"], input[type="password"]', process.env.LIBRARY_PASSWORD);

      // Click login button (adjust selector as needed)
      await this.page.click('button[type="submit"], input[type="submit"]');

      await this.page.waitForNavigation({ waitUntil: 'networkidle2' });
      console.log('Library authentication successful');
    } catch (error) {
      console.error('Authentication failed:', error.message);
      console.log('Continuing without authentication - some content may be inaccessible');
    }
  }

  async scrapeJournalPapers(journalKey, limit = 50) {
    const journal = JOURNALS[journalKey];
    console.log(`\nScraping papers from ${journal.name}...`);

    const papers = [];

    try {
      await this.page.goto(journal.url, { waitUntil: 'networkidle2' });
      await this.page.waitForTimeout(2000);

      // Different scraping strategies based on publisher
      switch (journal.publisher) {
        case 'SAGE':
          return await this.scrapeSAGE(journal, limit);
        case 'INFORMS':
          return await this.scrapeINFORMS(journal, limit);
        case 'OUP':
          return await this.scrapeOUP(journal, limit);
        case 'Springer':
          return await this.scrapeSpringer(journal, limit);
        case 'Elsevier':
          return await this.scrapeElsevier(journal, limit);
        default:
          console.log(`No scraper implemented for ${journal.publisher}`);
          return [];
      }
    } catch (error) {
      console.error(`Error scraping ${journal.name}:`, error.message);
      return papers;
    }
  }

  async scrapeSAGE(journal, limit) {
    console.log('Using SAGE scraper...');
    const papers = [];

    try {
      // Navigate to current issue or archive
      const articles = await this.page.$$('article.articleItem, .table-of-content .item');

      for (let i = 0; i < Math.min(articles.length, limit); i++) {
        try {
          const article = articles[i];

          const titleElement = await article.$('h5 a, .art_title a');
          const title = titleElement ? await this.page.evaluate(el => el.textContent.trim(), titleElement) : 'Unknown';
          const link = titleElement ? await this.page.evaluate(el => el.href, titleElement) : null;

          const authorElements = await article.$$('.author-name, .contrib-author');
          const authors = await Promise.all(
            authorElements.map(el => this.page.evaluate(e => e.textContent.trim(), el))
          );

          papers.push({
            title,
            authors: authors.join(', '),
            url: link,
            journal: journal.name,
            journalKey: 'JM', // Update based on actual journal
            abstract: '',
            year: new Date().getFullYear(),
            metadata: {}
          });
        } catch (error) {
          console.error(`Error parsing article ${i}:`, error.message);
        }
      }
    } catch (error) {
      console.error('SAGE scraper error:', error.message);
    }

    return papers;
  }

  async scrapeINFORMS(journal, limit) {
    console.log('Using INFORMS scraper...');
    // Implementation for INFORMS journals
    return [];
  }

  async scrapeOUP(journal, limit) {
    console.log('Using OUP scraper...');
    // Implementation for Oxford University Press journals
    return [];
  }

  async scrapeSpringer(journal, limit) {
    console.log('Using Springer scraper...');
    // Implementation for Springer journals
    return [];
  }

  async scrapeElsevier(journal, limit) {
    console.log('Using Elsevier scraper...');
    // Implementation for Elsevier journals
    return [];
  }

  async downloadPDF(paper, outputPath) {
    console.log(`Downloading: ${paper.title}`);

    try {
      if (!paper.url) {
        console.log('No URL available for download');
        return null;
      }

      await this.page.goto(paper.url, { waitUntil: 'networkidle2' });
      await this.page.waitForTimeout(2000);

      // Look for PDF download link (adjust selectors based on publisher)
      const pdfSelectors = [
        'a[href*=".pdf"]',
        'a.pdf-link',
        'a.download-pdf',
        '.article-tools a[href*="pdf"]'
      ];

      for (const selector of pdfSelectors) {
        try {
          const pdfLink = await this.page.$(selector);
          if (pdfLink) {
            await pdfLink.click();
            await this.page.waitForTimeout(5000); // Wait for download
            return outputPath;
          }
        } catch (e) {
          continue;
        }
      }

      console.log('PDF download link not found');
      return null;
    } catch (error) {
      console.error(`Error downloading PDF: ${error.message}`);
      return null;
    }
  }

  async downloadAllJournals() {
    const allPapers = [];
    const limit = parseInt(process.env.DOWNLOAD_LIMIT) || 50;

    for (const journalKey of Object.keys(JOURNALS)) {
      const papers = await this.scrapeJournalPapers(journalKey, limit);
      allPapers.push(...papers);

      // Be respectful with rate limiting
      await this.page.waitForTimeout(3000);
    }

    return allPapers;
  }

  async close() {
    if (this.browser) {
      await this.browser.close();
      console.log('Browser closed');
    }
  }
}

// CLI usage
if (import.meta.url === `file://${process.argv[1]}`) {
  const downloader = new PaperDownloader();

  (async () => {
    try {
      await downloader.initialize();
      await downloader.authenticateLibrary();

      const papers = await downloader.downloadAllJournals();

      // Save metadata
      const metadataPath = path.join(downloader.downloadPath, 'metadata.json');
      fs.writeFileSync(metadataPath, JSON.stringify(papers, null, 2));

      console.log(`\n✓ Downloaded metadata for ${papers.length} papers`);
      console.log(`✓ Metadata saved to: ${metadataPath}`);

    } catch (error) {
      console.error('Error:', error);
    } finally {
      await downloader.close();
    }
  })();
}

export default PaperDownloader;
