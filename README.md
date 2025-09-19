# ipdc-ldes-import-service

Microservice to import pages from the IPDC LDES feed and prepare them for hosting as LDES feed via the [ldes-serve-feed-service](https://github.com/lblod/ldes-serve-feed-service).

## Getting started
Add the service to your `docker-compose.yml`

``` yaml
services:
  ipdc-ldes-import:
    image: lblod/ipdc-ldes-import-service:0.1.0
    environment:
      IPDC_API_KEY: "my-secret-api-key-for-IPDC"
    volumes:
      - ./data/ldes-feed/ipdc-products:/data
```

Configure the `IPDC_API_KEY` environment variable and boot up the service

``` bash
docker compose up -d
```

## Reference
### Configuration
The following environment variables can be configured on the service
- **`IPDC_API_KEY`** (required): secret API key for the IPDC API
- **`IPDC_API_HOST`** (optional, default: `https://ipdc.vlaanderen.be`): domain the IPDC API is hosted on.
- **`ENABLE_POLLING`** (optional, default: `true`): enable continous polling of the IPDC LDES feed to check for updates. Any value of `true`, `1`, `on`, `yes` is considered truthy.
- **`POLLING_CRON_PATTERN`** (optional, default: `0 * * * * *`): cron frequency to check for updates. Only relevant if `ENABLE_POLLING` is set.

### API
#### POST /import
Trigger import of the IPDC LDES feed.

Returns status 202 Accepted on success.

Returns status 409 Conflict if an import is already running.

### Folder structure
Data fetched from the IPDC LDES feed will be stored in Turtle files containing relative relations to the other pages. The folder is structured as expected by the [ldes-serve-feed-service](https://github.com/lblod/ldes-serve-feed-service).

```
/data
 |-- 1.ttl
 |-- 2.ttl
 |-- 3.ttl
 |-- 4.ttl
  ...
```

Note that the page numbering of the [ldes-serve-feed-service](https://github.com/lblod/ldes-serve-feed-service) starts at 1 while the IPDC LDES feed starts at 0. Therefore page 0 of the IPDC LDES feed maps to the file `1.ttl`, page 1 to the file `2.ttl` etc.
