import os
import time
import threading
import subprocess
import shutil
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from send2trash import send2trash
import psutil

# ================== 配置 ==================
SEVEN_ZIP = "7z"  # 确保 7z.exe 在 PATH
MAX_LIMIT = 8
MIN_LIMIT = 1

INITIAL_WORKERS = 2
CHECK_INTERVAL = 5   # 秒
DISK_UTIL_LOW = 60   # %
DISK_UTIL_HIGH = 90  # %

# ==========================================

lock = threading.Lock()
current_workers = INITIAL_WORKERS
executor = None

finished_archives = deque()   # 解压完成的压缩包（按时间顺序）
active_tasks = set()

# ---------------- IO 监控 -----------------

def get_disk_util():
    d1 = psutil.disk_io_counters()
    time.sleep(1)
    d2 = psutil.disk_io_counters()

    busy = (d2.busy_time - d1.busy_time) / 1000.0
    util = min(100, busy * 100)
    return util

def monitor_and_adjust():
    global current_workers, executor

    while True:
        time.sleep(CHECK_INTERVAL)

        util = get_disk_util()
        cpu = psutil.cpu_percent(interval=1)

        with lock:
            if util < DISK_UTIL_LOW and current_workers < MAX_LIMIT:
                current_workers += 1
                print(f"[调度] 磁盘空闲(util={util:.1f}%), 提升并发 → {current_workers}")
                reset_executor()

            elif util > DISK_UTIL_HIGH and current_workers > MIN_LIMIT:
                current_workers -= 1
                print(f"[调度] 磁盘繁忙(util={util:.1f}%), 降低并发 → {current_workers}")
                reset_executor()

# ---------------- 解压逻辑 -----------------

def extract_archive(path):
    out_dir = os.path.splitext(path)[0]
    os.makedirs(out_dir, exist_ok=True)

    cmd = [
        SEVEN_ZIP, "x",
        path,
        f"-o{out_dir}",
        "-y",
        "-mmt=1"
    ]

    try:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            if "No space left" in r.stderr:
                return "NOSPACE"
            return "FAIL"
        return "OK"
    finally:
        with lock:
            active_tasks.discard(path)

def submit_task(path):
    with lock:
        active_tasks.add(path)
        executor.submit(task_wrapper, path)

def task_wrapper(path):
    while True:
        result = extract_archive(path)
        if result == "OK":
            with lock:
                finished_archives.append(path)
            return

        if result == "NOSPACE":
            if not free_space_by_oldest():
                print(f"[失败] 空间不足：{path}")
                return
            continue

        print(f"[失败] 解压失败：{path}")
        return

# ---------------- 空间管理 -----------------

def free_space_by_oldest():
    with lock:
        if not finished_archives:
            return False
        old = finished_archives.popleft()

    try:
        print(f"[空间] 删除最早压缩包：{old}")
        send2trash(old)
        return True
    except Exception:
        return False

# ---------------- Executor 重建 -----------------

def reset_executor():
    global executor
    old = executor
    executor = ThreadPoolExecutor(max_workers=current_workers)
    if old:
        old.shutdown(wait=False)

# ---------------- 扫描 -----------------

def scan_archives(root):
    for d, _, files in os.walk(root):
        for f in files:
            if f.lower().endswith((".zip", ".7z", ".rar")):
                yield os.path.join(d, f)

# ================= 主程序 ==================

def main():
    global executor

    print(f"初始并发：{INITIAL_WORKERS}")
    executor = ThreadPoolExecutor(max_workers=INITIAL_WORKERS)

    threading.Thread(target=monitor_and_adjust, daemon=True).start()

    root = os.getcwd()
    for archive in scan_archives(root):
        submit_task(archive)

    while True:
        with lock:
            if not active_tasks:
                break
        time.sleep(1)

    executor.shutdown(wait=True)

    print("✔ 全部解压完成，开始回收压缩包")

    for a in list(finished_archives):
        try:
            send2trash(a)
        except:
            pass

    print("✔ 完成")

if __name__ == "__main__":
    main()
