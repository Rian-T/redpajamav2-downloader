import glob
import io
import json
import os
import random
import traceback

import streaming
import zstandard
from tqdm.auto import tqdm


def main(in_dir: str, out_path: str):
    in_paths = glob.glob(os.path.join(in_dir, "**/*.jsonl.zst"), recursive=True)
    print("Files: ", len(in_paths))
    assert in_paths
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
            with zstandard.open(open(in_path, "rb"), "rt", encoding="utf-8") as f:
                for line in f:
                    try:
                        # text以外に謎のフィールドがいっぱい入ってる感じ
                        data = json.loads(line)
                        text = data["text"]
                        del data["text"]

                        row = {"text": text, "meta": data}
                        out.write(row)
                    except Exception as e:
                        traceback.print_exc()


if __name__ == "__main__":
    import fire

    fire.Fire(main)
