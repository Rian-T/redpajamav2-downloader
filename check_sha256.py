import hashlib
import multiprocessing
import os

from tqdm import tqdm


def calculate_hash(filename):
    h = hashlib.sha256()
    try:
        with open(filename, "rb") as file:
            while True:
                chunk = file.read(h.block_size)
                if not chunk:
                    break
                h.update(chunk)
    except FileNotFoundError:
        print(f"File does not exist: {filename}")
        return None
    return h.hexdigest()


def check_hash(args):
    input_data, directory = args
    expected_hash, filename = input_data
    filename = os.path.join(directory, filename)
    calculated_hash = calculate_hash(filename)

    if calculated_hash is None:
        return False

    if expected_hash != calculated_hash:
        print(f"Hash does not match: {filename}")
        os.remove(filename)
        return False

    return True


def check_hashes_in_directory(directory, hash_file):
    with open(hash_file, "r") as file:
        lines = [line.strip().split() for line in file]

    with multiprocessing.Pool() as pool:
        results = pool.imap_unordered(
            check_hash, [(line, directory) for line in lines], chunksize=1
        )
        results = list(tqdm(results, total=len(lines)))

    matched = results.count(True)
    not_matched = results.count(False)

    print(f"Matched: {matched}\nNot matched: {not_matched}")

    return matched, not_matched


def check_all_sha256_files(
    data_dir="/scratch/redpajama-data-1T/",
    sha256_dir="./sha256/",
):
    sha256_files = [f for f in os.listdir(sha256_dir) if f.endswith("_SHA256SUMS.txt")]

    for sha256_file in sha256_files:
        print(f"Processing {sha256_file}...")
        matched, not_matched = check_hashes_in_directory(
            data_dir, os.path.join(sha256_dir, sha256_file)
        )
        print(f"Correct  : {matched}")
        print(f"Incorrect: {not_matched}")
        print("----------")


if __name__ == "__main__":
    import fire

    fire.Fire(check_all_sha256_files)
