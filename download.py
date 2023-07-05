import logging
import multiprocessing
import os
import time
from multiprocessing.pool import ThreadPool

import requests
from tqdm.auto import tqdm

# loggingの設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("download.log"), logging.StreamHandler()],
)


def download_url(url):
    # ファイル名はURLの最後の部分を使用します
    url_prefix = "https://data.together.xyz/redpajama-data-1T/v1.0.0/"
    assert url.startswith(url_prefix)
    file_name = url[len(url_prefix) :]

    file_name = os.path.join("/scratch/redpajama-data-1T/", file_name)
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    try:
        # ダウンロードを試みる前に、既存のファイルをチェックします
        if os.path.exists(file_name):
            time.sleep(1)

            # ファイルが完全にダウンロードされていることを確認します
            with requests.get(url, stream=True, timeout=10.0) as r:
                r.raise_for_status()
                total_size = int(r.headers.get("content-length", 0))

            if total_size == os.path.getsize(file_name):
                logging.info(
                    f"File '{file_name}' already exists, and it is complete. Skipping download."
                )
                return
            else:
                logging.info(
                    f"File '{file_name}' already exists, but it's incomplete. Downloading."
                )

        # URLからデータを取得します
        response = requests.get(url, stream=True)

        # ファイルを書き込みます
        with open(file_name, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)

    except Exception as e:
        logging.error(
            f"An error occurred while downloading the file '{file_name}'. Error: {e}"
        )


def main():
    urls = open("urls.txt").readlines()
    urls = [url.strip() for url in urls]
    # urls = [url.strip() for url in urls if "common_crawl" in url]
    # print(urls)
    # return

    pool = ThreadPool(10)

    # URLごとにdownload_url関数を呼び出します
    dummy = pool.imap_unordered(download_url, urls, chunksize=1)
    dummy = list(tqdm(dummy, total=len(urls)))

    # すべてのタスクが終了するまで待ちます
    pool.close()
    pool.join()


if __name__ == "__main__":
    import fire

    fire.Fire(main)
