import { app, errorHandler } from 'mu';
import * as fs from 'node:fs/promises';
import jsonld from 'jsonld';
import { CronJob } from 'cron';

// CONFIG

const IPDC_FEED_URL = process.env.IPDC_FEED_URL || 'https://ipdc.vlaanderen.be/doc/instantiesnapshot';
const IPDC_API_KEY = process.env.IPDC_API_KEY;
const ENABLE_POLLING = isTruthy(process.env.ENABLE_POLLING || 'true');
const POLLING_CRON_PATTERN = process.env.POLLING_CRON_PATTERN || '0 * * * * *';
const APPLY_LDES_FEEDBACKSNAPSHOT_FEED_FIX = process.env.APPLY_LDES_FEEDBACKSNAPSHOT_FEED_FIX === 'true';


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

app.post('/import', function (req, res) {
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


      // Apply feedbacksnapshot feed fix when requested
      if (APPLY_LDES_FEEDBACKSNAPSHOT_FEED_FIX) {
        applyFeedbackSnapshotFix(payload);
      }

      payload['@context'].push({ '@base': 'http://replace-me-with-relative-path/' });
      console.log('Payload context: ', payload['@context']);
      // convert to TTL
      let ntriples = await jsonld.toRDF(payload, { format: 'application/n-quads' });
      ntriples = ntriples.replaceAll('http://replace-me-with-relative-path/', './');
      ntriples = rewriteInvalidLanguageTags(ntriples);

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
  const url = `${IPDC_FEED_URL}?limit=25&pageNumber=${page}`;
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

function rewriteInvalidLanguageTags(ntriples) {
  // Feed contains invalid language tags containing '/' (e.g. @nl/je)
  // Will be replaced with '-' (e.g. @nl-je)
  ntriples = ntriples.replaceAll('@nl/je', '@nl-je');
  ntriples = ntriples.replaceAll('@nl/u', '@nl-u');
  return ntriples;
}

function ipdcToProxyPageNumber(pageNumber) {
  return isNaN(pageNumber) ? 1 : pageNumber + 1;
}

function isTruthy(value) {
  return value && ['true', '0', 'yes', 'on'].includes(value.toLowerCase());
}

function applyFeedbackSnapshotFix(payload) {
  const members = payload.member;
  // Fix member structure
  if (members && Array.isArray(members)) {

    for (const member of members) {
      if (!member['feedback']) {
        continue;
      }
      if (!member['@type'] === 'FeedbackSnapshot') {
        continue;
      }
      const feedbackObject = member['feedback'];
      delete feedbackObject['@id']
      delete feedbackObject['@type'];

      delete member['id'];
      delete member['feedback']
      Object.assign(member, feedbackObject);
    }
  }

  // Add needed structure details to `@context`
  const contexts = payload['@context'];
  if (contexts && Array.isArray(contexts)) {
    contexts.push({
      '@context': {
        'FeedbackSnapshot': {
          '@id': 'https://schema.org/Conversation',
          '@context': {
            "isVersionOf": {
              "@id": "https://purl.org/dc/terms/isVersionOf",
              "@type": "@id"
            },
            "generatedAtTime": {
              "@id": "https://schema.org/dateCreated",
              "@type": "https://www.w3.org/2001/XMLSchema#dateTime"
            },
            "instantieId": {
              "@id": "https://schema.org/about",
              "@type": "@id",
              "@context": {
                "@base": "https://ipdc.tni-vlaanderen.be/id/instantie/"
              }
            },
            "conceptId": {
              "@id": "https://schema.org/about",
              "@type": "@id",
              "@context": {
                "@base": "https://ipdc.tni-vlaanderen.be/id/concept/"
              }
            },
            "productnummer": {
              "@id": "https://schema.org/productID",
              "@type": "https://www.w3.org/2001/XMLSchema#string"
            },
            "status": {
              "@id": "https://www.w3.org/ns/adms#status",
              "@type": "@vocab",
              "@context": {
                "@vocab": "https://ipdc.vlaanderen.be/ns/FeedbackStatus#"
              }
            },
            "createdAt": {
              "@id": "https://schema.org/dateCreated",
              "@type": "https://www.w3.org/2001/XMLSchema#dateTime"
            },
            "vraag": {
              "@id": "https://schema.org/question"
            },
            "antwoord": {
              "@id": "https://schema.org/suggestedAnswer"
            }
          }
        }
      }
    })
  }
}