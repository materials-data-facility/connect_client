# GPT-5.4 Simple Walkthrough: Publish a Dataset in One Line

This is the shortest clean demo of the new workflow.

All commands are single-line so they paste cleanly.

## Terminal A: start a local backend

```bash
cd /Users/ben/Desktop/git/mdf_client/cs/aws
```

```bash
STORE_BACKEND=sqlite SQLITE_PATH=/tmp/mdf-gpt54-simple.db ASYNC_DISPATCH_MODE=inline STORAGE_BACKEND=local FILE_STORE_PATH=/tmp/mdf-gpt54-simple-files AUTH_MODE=dev ALLOW_ALL_CURATORS=true USE_MOCK_DATACITE=true USE_MOCK_SEARCH=true python -m v2.app.main
```

## Terminal B: install the client and create a tiny dataset

```bash
cd /Users/ben/Desktop/git/mdf_client
```

```bash
python -m pip install -e ".[extractors]"
```

```bash
rm -rf /tmp/mdf-gpt54-simple-demo && mkdir -p /tmp/mdf-gpt54-simple-demo && printf 'x,y\n1,2\n3,4\n' > /tmp/mdf-gpt54-simple-demo/data.csv
```

## One-time setup

```bash
printf 'local\nGPT54 Demo Lab\nGPT54 Demo Publisher\n\nn\n' | mdf setup /tmp/mdf-gpt54-simple-demo
```

## Publish with one line of code

This is the main demo command:

```bash
mdf publish /tmp/mdf-gpt54-simple-demo --title "GPT54 One-Line Dataset" --author "GPT54 Demo User" --submit --service local
```

That one command:

- uploads the local directory
- submits the dataset
- records the source ID
- confirms it reached `pending_curation`

## Get the source ID

```bash
SOURCE_ID=$(mdf status --json --service local | python -c 'import json,sys; print(json.load(sys.stdin)["submission"]["source_id"])') && echo "$SOURCE_ID"
```

## Show the result

```bash
mdf status "$SOURCE_ID" --service local
```

```bash
mdf pending --service local
```

## Approve it and watch it publish

```bash
mdf approve "$SOURCE_ID" --yes --service local
```

```bash
mdf watch "$SOURCE_ID" --service local --interval 1 --timeout 30
```

## Show the published dataset

```bash
mdf show "$SOURCE_ID" --cite --service local
```

```bash
mdf search "GPT54 One-Line Dataset" --service local
```

## Fastest possible recap

```bash
cd /Users/ben/Desktop/git/mdf_client
```

```bash
python -m pip install -e ".[extractors]"
```

```bash
rm -rf /tmp/mdf-gpt54-simple-demo && mkdir -p /tmp/mdf-gpt54-simple-demo && printf 'x,y\n1,2\n3,4\n' > /tmp/mdf-gpt54-simple-demo/data.csv
```

```bash
printf 'local\nGPT54 Demo Lab\nGPT54 Demo Publisher\n\nn\n' | mdf setup /tmp/mdf-gpt54-simple-demo
```

```bash
mdf publish /tmp/mdf-gpt54-simple-demo --title "GPT54 One-Line Dataset" --author "GPT54 Demo User" --submit --service local
```

```bash
SOURCE_ID=$(mdf status --json --service local | python -c 'import json,sys; print(json.load(sys.stdin)["submission"]["source_id"])') && mdf approve "$SOURCE_ID" --yes --service local && mdf watch "$SOURCE_ID" --service local --interval 1 --timeout 30 && mdf show "$SOURCE_ID" --cite --service local
```
