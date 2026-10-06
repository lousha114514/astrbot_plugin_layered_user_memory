#!/usr/bin/env python3
"""打包脚本：读取 metadata.yaml 的 version，产出带版本号的插件 zip。

用法：
    python build.py            # 输出到插件目录的上一级
    python build.py --out DIR  # 指定输出目录

版本号唯一来源为 metadata.yaml 的 `version` 字段；发版前只需改它（并同步
main.py 的 __version__ 与 CHANGELOG.md），然后运行本脚本。
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import zipfile

ROOT = os.path.dirname(os.path.abspath(__file__))
PLUGIN_NAME = "astrbot_plugin_user_memory"

# 打进 zip 的文件（平铺在压缩包根目录，符合 AstrBot 上传格式）
INCLUDE = [
    "main.py",
    "metadata.yaml",
    "_conf_schema.json",
    "README.md",
    "requirements.txt",
    "CHANGELOG.md",
    "pages/memory/index.html",
    "pages/memory/app.js",
    "pages/memory/style.css",
]


def read_version() -> str:
    path = os.path.join(ROOT, "metadata.yaml")
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = re.match(r"\s*version:\s*(\S+)", line)
            if m:
                return m.group(1).strip()
    raise SystemExit("未在 metadata.yaml 中找到 version 字段")


def main() -> int:
    parser = argparse.ArgumentParser(description="打包 AstrBot 插件 zip")
    parser.add_argument("--out", default=os.path.dirname(ROOT), help="输出目录")
    args = parser.parse_args()

    version = read_version()
    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{PLUGIN_NAME}-{version}.zip")

    missing = [f for f in INCLUDE if not os.path.exists(os.path.join(ROOT, f))]
    if missing:
        print("缺少文件，无法打包：")
        for f in missing:
            print("  -", f)
        return 1

    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as z:
        for f in INCLUDE:
            z.write(os.path.join(ROOT, f), arcname=f)

    size = os.path.getsize(out_path)
    print(f"已打包 {version} -> {out_path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
