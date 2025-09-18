# MongoDB Batch Update Example

## Overview
This project demonstrates **batch processing** to efficiently enrich and update MongoDB documents.  
It reads “leads” from a MongoDB database, fetches related `quotations` and `policyrequests` records, builds a consolidated `leadView` object for each document, and updates the database in parallel while respecting a configurable concurrency limit.

## Key Features
- **Batch Update with Concurrency Control**  
  Uses [`p-limit`](https://www.npmjs.com/package/p-limit) to control how many asynchronous update operations run simultaneously.  
  This prevents MongoDB and your Node.js process from being overwhelmed.

- **Lead Data Enrichment**  
  For each lead missing `proposalData`, `quoteData`, and `placa`, the script:
  1. Queries related `quotations` and `policyrequests`.
  2. Combines the data into a new object using `buildLeadView`.
  3. Updates the original lead document in the database.

- **Automated Index Creation**  
  After updates complete, indexes are created on multiple fields to optimize future queries and reporting.

## Environment Variables
| Variable              | Description                                      | Default |
|-----------------------|--------------------------------------------------|---------|
| `MONGODB_URI`         | MongoDB connection string                        | *none* |
| `MONGODB_DB_NAME`     | Name of the database                              | *none* |
| `LIMITER`             | Max number of concurrent update operations        | `1000` |

## Installation
```bash
git clone <repository-url>
cd <project-folder>
npm install
node index.js

