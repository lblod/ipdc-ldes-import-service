import { app, errorHandler } from 'mu';
import * as fs from 'node:fs/promises';
import jsonld from 'jsonld';

// CONFIG

const IPDC_API_HOST = process.env.IPDC_API_HOST || 'https://ipdc.vlaanderen.be';
const IPDC_API_KEY = process.env.IPDC_API_KEY;
const LDES_FOLDER = process.env.LDES_FOLDER || 'ipdc-products';


// INIT

const lastPage = await determineLastPage();
console.log(`Initializing import service with ${lastPage} as current last page`);
await importFeed(lastPage);

// TODO keep current page number as state
// TODO add mu script to trigger import of feed
// TODO add mu script to reset (= delete files from folder)
// TODO add frequent fetch for last page


// API

app.get('/', function( req, res ) {
  res.send('Hello mu-javascript-template');
} );

app.use(errorHandler);

// HELPERS

async function determineLastPage() {
  const files = await fs.readdir(`/data/${LDES_FOLDER}`);
  const pageNumbers = files.map((file) => {
    if (file.endsWith('.ttl')) {
      return parseInt(file.replace('.ttl', ''));
    } else {
      return 0;
    }
  });

  return Math.max(0, ...pageNumbers);
}

async function importFeed(startPage = 0) {
  console.log(`Start importing IPDC LDES feed as of page ${startPage}`);
  let pageNumber = startPage;

  while (pageNumber >= 0) {
    // fetch page from IPDC LDES feed
    console.log(`Fetch page ${pageNumber} from IPDC LDES feed`);
    const payload = await fetchPage(pageNumber);

    // rewrite relation links to relative URLs
    rewriteRelationUrls(payload);
    payload['@context'].push({ '@base': 'http://replace-me-with-relative-path/' });
    // convert to TTL
    let ntriples = await jsonld.toRDF(payload, { format: 'application/n-quads' });
    ntriples = ntriples.replaceAll('http://replace-me-with-relative-path/', './');

    // write to file
    const proxyPageNumber = ipdcToProxyPageNumber(pageNumber);
    const outputFile = `/data/${LDES_FOLDER}/${proxyPageNumber}.ttl`;
    console.log(`Write page to ${outputFile}`);
    await fs.writeFile(outputFile, ntriples);

    // prepare for next page
    if (hasNextPage(payload)) {
      pageNumber++;
    } else {
      pageNumber = -1;
    }
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
