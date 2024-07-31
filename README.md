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

## Features and Goals
- [ ] Integrated in the index scheduler _1 week_
  - [x] Reads content from the update files and write into the index environment
  - [ ] Use a single write transaction for auto batched document updates (PATCH or POST)
  - [ ] Stop auto batching and reordering settings updates with document updates
- [ ] Read the right update files (the one dedicated to the index) _1/2 day_
  - [x] Read them in parallel to avoid copies
- [x] Compute the obkv documents when doing updates (PATCH)
  - [ ] Support nested fields (flattened obkv) _1 week_
- [ ] Support good dump import (no MDB_TXN_FULL)
- [ ] Design it with document compression in mind
- [ ] Support good edit documents by function execution (optional) (stream edited documents into LMDB)
- [ ] Refactor all the extractors _2 weeks_
  - [x] Use a memory LRU cache _2 days_
  - [x] Extract the word docids database
    - [ ] Do it by using charabia (currently a `split_whitespace`)
    - [ ] Merge the sorters from the different threads in parallel and
          send the content to the LMDB writer thread to avoid duplicating content.
          Note that entries will be unordered and `MDB_APPEND` will no longer work.
  - [ ] Extract the word pair proximity docids database
  - [ ] Extract the exact word databases
  - [ ] Extract the facet strings and numbers
    - [ ] Build the facet level tree (in parallel?)
  - [ ] Compute the word prefix docids (optional) (use rotxn in parallel) _1 week_
  - [ ] Extract the vectors (use rotxn in parallel)
  - [ ] Extract the geo points (use rotxn in parallel)
- [ ] Perform a lot of tests on the Cloud with small machines _2 weeks_
- [ ] Optimize charabia (second step)
