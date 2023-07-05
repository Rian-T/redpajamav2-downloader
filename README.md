# redpajama-downloader

Downloads RedPajama and converts it into MDS files. It's fast because it downloads in parallel.

Warning: developed as a one-off tool and hasn't been extensively tested.

## Step 1: Download `urls.txt`

```
wget 'https://data.together.xyz/redpajama-data-1T/v1.0.0/urls.txt'
```

## Step 2: Download SHA256 checksum files

```
bash download_sha256.sh
```

## Step 3: Download JSONL files (8 hours)

```
python download.py
```

## Step 4: Check SHA256

```
python check_sha256.py
```

If there are some errors, go back to Step 2. It will re-download only incomplete files.

## Step 5: Create MDS files (16 hours)

The `common_crawl` subset is compressed by zstandard.

```
python create_mds_jsonl_zst.py --in-dir /scratch/redpajama-data-1T/common_crawl/ --out-path s3://takiba/data/llm/mds/en/redpajama-data-1T/common_crawl
```

Other subsets are standard jsonl files.

```
python create_mds_jsonl.py --in-dir /scratch/redpajama-data-1T/stackexchange/ --out-path s3://takiba/data/llm/mds/en/redpajama-data-1T/stackexchange
```

