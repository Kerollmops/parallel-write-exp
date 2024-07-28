# Parallel Write Exp

A parallel indexer experiment for Meilisearch.

## How to use it?

First run Meilisearch with the following experimental feature. Setting this parameter to zero will let Meilisearch register update files but will prevent it to process them by itself.

```bash
./meilisearch --experimental-max-number-of-batched-tasks 0
```

Send your documents to Meilisearch and once you are sure the document content has successfully been stored in the _data.ms/update_files_ folder, run this command line with the path to the index you want to update. Note that:
 - We advice to cut Meilisearch so that nothing wrong can happen.
 - It only support update (PUT) and not yet replacement (POST).
 - All the files in this folder will be indexed into the index you'll specified even if it's not dedicated to this index.
 - You must empty this folder to make sure that Meilisearch doesn't try to process them again once it reboots.

```bash
cargo run --release -- --primary-key externalId --operation update --index-path ../meilisearch/data.ms/indexes/{index-uid}
```
