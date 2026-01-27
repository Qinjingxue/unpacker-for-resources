import os
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from send2trash import send2trash

MAX_WORKERS = min(6, os.cpu_count() or 6)

# 解压完成顺序队列（FIFO）
EXTRACTED_QUEUE = deque()      # (timestamp, archive_path)
QUEUE_LOCK = threading.Lock()

# 所有识别到的压缩包（最终统一回收）
ALL_ARCHIVES = set()


# ================== 魔数识别 ==================
def detect_archive_type(path):
    try:
        with open(path, "rb") as f:
            sig = f.read(8)
    except Exception:
        return None

    if sig.startswith(b"PK\x03\x04"):
        return "zip"
    if sig.startswith(b"7z\xBC\xAF\x27\x1C"):
        return "7z"
    if sig.startswith(b"Rar!\x1A\x07"):
        return "rar"
    return None


# ================== 释放空间（删最早压缩包） ==================
def free_space_by_oldest():
    with QUEUE_LOCK:
        if not EXTRACTED_QUEUE:
            return False

        _, archive = EXTRACTED_QUEUE.popleft()

    try:
        if os.path.exists(archive):
            os.remove(archive)
            print(f"🗑 删除最早压缩包释放空间：{archive}")
        return True
    except Exception as e:
        print(f"⚠ 删除失败：{archive} -> {e}")
        return False


# ================== 实际解压（单次尝试） ==================
def try_extract_once(path, passwords):
    name = os.path.splitext(os.path.basename(path))[0]
    out_dir = os.path.join(os.path.dirname(path), name)
    os.makedirs(out_dir, exist_ok=True)

    for pwd in passwords:
        cmd = ["7z", "x", "-y", f"-p{pwd}", path, f"-o{out_dir}"]
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        if proc.returncode == 0:
            with QUEUE_LOCK:
                EXTRACTED_QUEUE.append((time.time(), path))
            print(f"✔ 解压成功：{path}")
            return True, out_dir

        if b"No space left" in proc.stderr or b"not enough space" in proc.stderr:
            return "NOSPACE", None

    return False, None


# ================== 带“持续释放空间”的解压 ==================
def extract_with_space_management(path, passwords):
    while True:
        result, out = try_extract_once(path, passwords)

        if result == "NOSPACE":
            print(f"⚠ 空间不足：{path}，尝试释放空间...")
            if free_space_by_oldest():
                continue   # 🔁 删一个 → 再试
            else:
                print(f"✘ 无可删除压缩包，空间仍不足：{path}")
                return False, None

        return result, out


# ================== 扫描压缩包 ==================
def scan_archives(base):
    found = []
    for root, _, files in os.walk(base):
        for f in files:
            full = os.path.join(root, f)
            if detect_archive_type(full):
                found.append(full)
    return found


# ================== 单分支嵌套目录压平 ==================
def flatten_single_chain(base_dir):
    changed = True
    while changed:
        changed = False
        for root, dirs, files in os.walk(base_dir, topdown=False):
            if len(dirs) == 1 and not files:
                child = os.path.join(root, dirs[0])
                for item in os.listdir(child):
                    shutil.move(
                        os.path.join(child, item),
                        os.path.join(root, item)
                    )
                os.rmdir(child)
                changed = True


# ================== 主逻辑 ==================
def main():
    print("请输入密码（每行一个，空行结束）：")
    passwords = []
    while True:
        line = input().strip()
        if not line:
            break
        passwords.append(line)
    passwords.append("")  # 无密码兜底

    base_dir = os.getcwd()
    processed = set()

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = {}

        def submit(path):
            futures[
                executor.submit(extract_with_space_management, path, passwords)
            ] = path

        # 初始扫描（仅当前目录）
        for f in os.listdir(base_dir):
            full = os.path.abspath(f)
            if os.path.isfile(full) and detect_archive_type(full):
                ALL_ARCHIVES.add(full)
                submit(full)

        # 动态并行处理
        while futures:
            for future in as_completed(list(futures)):
                path = futures.pop(future)
                success, out = future.result()

                if success:
                    processed.add(path)
                    for sub in scan_archives(out):
                        if sub not in processed:
                            ALL_ARCHIVES.add(sub)
                            submit(sub)

    # ================== 最终补丁：回收压缩包 ==================
    print("\n🧹 回收所有压缩包到回收站...")
    for arc in ALL_ARCHIVES:
        if os.path.exists(arc):
            try:
                send2trash(arc)
                print(f"🗑 已回收：{arc}")
            except Exception as e:
                print(f"⚠ 回收失败：{arc} -> {e}")

    # ================== 压平目录 ==================
    print("\n🔧 清理纯单分支嵌套目录...")
    flatten_single_chain(base_dir)

    print("\n🎉 全部流程完成")


if __name__ == "__main__":
    main()
