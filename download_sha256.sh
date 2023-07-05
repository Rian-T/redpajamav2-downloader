#!/bin/sh

mkdir -p sha256
cd sha256

wget https://data.together.xyz/redpajama-data-1T/v1.0.0/sha256/arxiv_SHA256SUMS.txt
wget https://data.together.xyz/redpajama-data-1T/v1.0.0/sha256/book_SHA256SUMS.txt
wget https://data.together.xyz/redpajama-data-1T/v1.0.0/sha256/c4_SHA256SUMS.txt
wget https://data.together.xyz/redpajama-data-1T/v1.0.0/sha256/common_crawl_SHA256SUMS.txt
wget https://data.together.xyz/redpajama-data-1T/v1.0.0/sha256/github_SHA256SUMS.txt
wget https://data.together.xyz/redpajama-data-1T/v1.0.0/sha256/stackexchange_SHA256SUMS.txt
wget https://data.together.xyz/redpajama-data-1T/v1.0.0/sha256/wikipedia_SHA256SUMS.txt
