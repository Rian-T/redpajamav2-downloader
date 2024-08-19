import logging
import multiprocessing
import os
import time
from multiprocessing.pool import ThreadPool
import subprocess

import requests
from tqdm.auto import tqdm

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("download.log"), logging.StreamHandler()],
)

def download_url(url, max_retries=5):
    # Use the last part of the URL as the file name
    url_prefix = "https://data.together.xyz/redpajama-data-v2/v1.0.0/"
    assert url.startswith(url_prefix)
    file_name = url[len(url_prefix) :]

    file_name = os.path.join("../redpajama-data-test/", file_name)
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    retries = 0
    backoff_time = 1  # Initial backoff time in seconds

    while retries < max_retries:
        try:
            # Check for existing files before attempting to download
            if os.path.exists(file_name):
                time.sleep(1)

                # Ensure the file is completely downloaded
                result = subprocess.run(
                    ["wget", "--spider", url],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    total_size = int(result.stderr.split("Length: ")[1].split(" ")[0])

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
            with open("wget.log", "a") as log_file:
                result = subprocess.run(
                    ["wget", "-O", file_name, url],
                    stdout=log_file,
                    stderr=log_file,
                    text=True
                )

            if result.returncode == 0:
                return True
            elif "429 Too Many Requests" in result.stderr:
                raise Exception("429 Too Many Requests")

            raise Exception(result.stderr)

        except Exception as e:
            if "429 Too Many Requests" in str(e):
                retries += 1
                logging.warning(
                    f"Received 429 Too Many Requests. Retrying in {backoff_time} seconds..."
                )
                time.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
            else:
                logging.error(
                    f"An error occurred while downloading the file '{file_name}'. Error: {e}"
                )
                return False

    logging.error(
        f"Failed to download the file '{file_name}' after {max_retries} retries due to 429 Too Many Requests."
    )
    return False

def download_urls(urls, num_threads):
    pool = ThreadPool(num_threads)
    results = list(tqdm(pool.imap_unordered(download_url, urls, chunksize=1), total=len(urls)))
    pool.close()
    pool.join()
    return results

def main():
    urls = open("fr_urls.txt").readlines()
    urls = [url.strip() for url in urls]

    # Initial download attempt with 64 threads
    results = download_urls(urls, 64)

    # Collect failed downloads
    failed_urls = [url for url, success in zip(urls, results) if not success]

    # Retry failed downloads with fewer threads
    max_retries = 10
    retry_count = 0

    while failed_urls and retry_count < max_retries:
        retry_count += 1
        logging.info(f"Retrying {len(failed_urls)} failed downloads with 1 thread. Attempt {retry_count}/{max_retries}.")
        results = download_urls(failed_urls, 10 - retry_count)

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