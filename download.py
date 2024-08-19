import logging
import multiprocessing
import os
import time
from multiprocessing.pool import ThreadPool

import requests
from tqdm.auto import tqdm

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("download.log"), logging.StreamHandler()],
)


def download_url(url):
    # Use the last part of the URL as the file name
    url_prefix = "https://data.together.xyz/redpajama-data-v2/v1.0.0/"
    assert url.startswith(url_prefix)
    file_name = url[len(url_prefix) :]

    file_name = os.path.join("/scratch/redpajama-data-1T/", file_name)
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    try:
        # Check for existing files before attempting to download
        if os.path.exists(file_name):
            time.sleep(1)

            # Ensure the file is completely downloaded
            with requests.get(url, stream=True, timeout=10.0) as r:
                r.raise_for_status()
                total_size = int(r.headers.get("content-length", 0))

            if total_size == os.path.getsize(file_name):
                logging.info(
                    f"File '{file_name}' already exists, and it is complete. Skipping download."
                )
                return True
            else:
                logging.info(
                    f"File '{file_name}' already exists, but it's incomplete. Downloading."
                )

        # Retrieve data from the URL
        response = requests.get(url, stream=True)

        # Write the file
        with open(file_name, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)

        return True

    except Exception as e:
        logging.error(
            f"An error occurred while downloading the file '{file_name}'. Error: {e}"
        )
        return False


def download_urls(urls, num_threads):
    pool = ThreadPool(num_threads)
    results = list(tqdm(pool.imap_unordered(download_url, urls, chunksize=1), total=len(urls)))
    pool.close()
    pool.join()
    return results


def main():
    urls = open("urls.txt").readlines()
    urls = [url.strip() for url in urls]

    # Initial download attempt with 10 threads
    results = download_urls(urls, 10)

    # Collect failed downloads
    failed_urls = [url for url, success in zip(urls, results) if not success]

    # Retry failed downloads with fewer threads
    max_retries = 10
    retry_count = 0

    while failed_urls and retry_count < max_retries:
        retry_count += 1
        logging.info(f"Retrying {len(failed_urls)} failed downloads with 1 thread. Attempt {retry_count}/{max_retries}.")
        results = download_urls(failed_urls, 1)

        # Collect remaining failed downloads
        failed_urls = [url for url, success in zip(failed_urls, results) if not success]

    # Log any remaining failed downloads
    if failed_urls:
        logging.error(f"Failed to download {len(failed_urls)} files after {max_retries} retries.")
        with open("failed_urls.txt", "w") as f:
            for url in failed_urls:
                f.write(url + "\n")
    else:
        logging.info("All files downloaded successfully.")


if __name__ == "__main__":
    import fire

    fire.Fire(main)