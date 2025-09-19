import { app, errorHandler } from 'mu';
import * as fs from 'node:fs/promises';
import jsonld from 'jsonld';
import { CronJob } from 'cron';

// CONFIG

const IPDC_API_HOST = process.env.IPDC_API_HOST || 'https://ipdc.vlaanderen.be';
const IPDC_API_KEY = process.env.IPDC_API_KEY;
const ENABLE_POLLING = isTruthy(process.env.ENABLE_POLLING || 'true');
const POLLING_CRON_PATTERN = process.env.POLLING_CRON_PATTERN || '0 * * * * *';


// INIT

let isImporting = false;
let currentPageNumber = await determineLastPage();
console.log(`Initializing import service with ${currentPageNumber} as current last page`);
console.log(`Data will be imported in /data`);

if (ENABLE_POLLING) {
  console.log(`Initialize polling with cron pattern '${POLLING_CRON_PATTERN}'`);
  new CronJob(
    POLLING_CRON_PATTERN,
    () => fetch('http://localhost/import', { method: 'POST' }),
    null,
    true
  );
} else {
  console.log('Polling disabled. Import can only be triggered manually.');
}


// API

app.post('/import', function(req, res) {
  if (isImporting) {
    res.status(409).send({
      errors: [{
        title: 'Conflict',
        detail: 'Service already busy importing data from the IPDC LDES feed'
      }]
    });
  } else {
    importFeed();
    res.status(202).send();
  }
});

app.use(errorHandler);


// HELPERS

async function determineLastPage() {
  const files = await fs.readdir(`/data`);
  const pageNumbers = files.map((file) => {
    if (file.endsWith('.ttl')) {
      return parseInt(file.replace('.ttl', ''));
    } else {
      return 1;
    }
  });

  return Math.max(1, ...pageNumbers) - 1;
}

async function importFeed() {
  console.log(`Importing IPDC LDES feed starting from page ${currentPageNumber}`);
  isImporting = true;

  try {
    let isLastPage = false;
    while (!isLastPage) {
      // fetch page from IPDC LDES feed
      console.log(`Fetch page ${currentPageNumber} from IPDC LDES feed`);
      const payload = await fetchPage(currentPageNumber);

      // rewrite relation links to relative URLs
      rewriteRelationUrls(payload);
      payload['@context'].push({ '@base': 'http://replace-me-with-relative-path/' });
      // convert to TTL
      let ntriples = await jsonld.toRDF(payload, { format: 'application/n-quads' });
      ntriples = ntriples.replaceAll('http://replace-me-with-relative-path/', './');

      // write to file
      const proxyPageNumber = ipdcToProxyPageNumber(currentPageNumber);
      const outputFile = `/data/${proxyPageNumber}.ttl`;
      console.log(`Write page to ${outputFile}`);
      await fs.writeFile(outputFile, ntriples);

      // prepare for next page
      if (hasNextPage(payload)) {
        currentPageNumber++;
      } else {
        isLastPage = true;
      }
    }

    console.log(`Reached the end of the LDES feed. Current last page is page ${currentPageNumber}.`);
  } catch (e) {
    console.log(`An error occurred. Import of feed is interrupted. Current last page is page ${currentPageNumber}.`);
  } finally {
    isImporting = false;
  }
}

async function fetchPage(page) {
  const url = `${IPDC_API_HOST}/doc/instantiesnapshot?limit=25&pageNumber=${page}`;
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Accept': 'application/ld+json',
      'X-API-KEY': IPDC_API_KEY
    }
  });

  if (response.ok) {
    return response.json();
  } else {
    const message = await response.text();
    console.log(`Failed to fetch page from feed at ${url}\nResponse: ${response.status} ${response.statusText}\n${message}`);
    throw new Error(`Failed to fetch page from feed at ${url}`);
  }
}

function hasNextPage(payload) {
  return payload.view?.relation
    && payload.view.relation.some((relation) => relation['@type'] == 'GreaterThanOrEqualToRelation');
}

function rewriteRelationUrls(payload) {
  if (payload.view?.relation) {
    const relations = payload.view.relation;
    for (let relation of relations) {
      const url = new URL(relation.node);
      const pageNumber = parseInt(url.searchParams.get('pageNumber'));
      relation.node = `./${ipdcToProxyPageNumber(pageNumber)}`;
    }
  }
}

function ipdcToProxyPageNumber(pageNumber) {
  return isNaN(pageNumber) ? 1 : pageNumber + 1;
}

function isTruthy(value) {
  return value && ['true', '0', 'yes', 'on'].includes(value.toLowerCase());
}
