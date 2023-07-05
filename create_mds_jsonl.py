import glob
import json
import os
import random
import traceback

import streaming
from tqdm.auto import tqdm


def into_row(data):
    # https://huggingface.co/datasets/togethercomputer/RedPajama-Data-1T/blob/main/RedPajama-Data-1T.py

    if "meta" not in data:
        text = data["text"]
        del data["text"]
        return {
            "text": text,
            "meta": json.dumps(data),
        }
    else:
        return {
            "text": data["text"],
            "meta": data["meta"],
        }


def main(in_dir: str, out_path: str):
    in_paths = glob.glob(os.path.join(in_dir, "**/*.jsonl"), recursive=True)
    in_paths = sorted(in_paths)
    random.seed(42)
    random.shuffle(in_paths)

    columns = {
        "text": "str",
        "meta": "json",
        # "red_pajama_subset": "str",  # なんか入ってないｗ 使う予定ないし良いか
    }

    with streaming.MDSWriter(
        out=out_path,
        columns=columns,
        # progress_bar=True,
        compression="zstd",
        hashes=["sha1", "xxh64"],
    ) as out:
        for in_path in tqdm(in_paths):
            with open(in_path, encoding="utf-8") as f:
                for line in f:
                    try:
                        row = into_row(json.loads(line))
                        out.write(row)
                    except Exception as e:
                        print(in_path)
                        traceback.print_exc()


if __name__ == "__main__":
    import fire

    fire.Fire(main)
