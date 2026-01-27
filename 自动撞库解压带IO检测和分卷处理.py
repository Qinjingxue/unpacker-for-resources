import os
import shutil
import subprocess
import threading
import time
import psutil
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from send2trash import send2trash
from collections import deque, defaultdict

# ================== 基础配置 ==================

ROOT = os.getcwd()

PASSWORDS = []
FAILED = []

UNPACKED_ARCHIVES = deque()
PROCESSED = set()          # 已处理的逻辑压缩包
IN_PROGRESS = set()        # 正在解压的逻辑压缩包
LOCK = threading.Lock()

# ================== 并发控制 ==================

MAX_WORKERS = 2
MIN_WORKERS = 1
MAX_WORKERS_LIMIT = max(2, os.cpu_count() or 4)
CHECK_INTERVAL = 3

# 使用信号量控制并发
WORKER_SEM = threading.Semaphore(MAX_WORKERS)

# ================== magic bytes ==================

MAGIC_7Z = b"7z\xBC\xAF\x27\x1C"
MAGIC_RAR = b"Rar!"
MAGIC_ZIP = b"PK"

# ================== 用户输入密码 ==================

def read_passwords():
    print("请输入密码（直接回车结束输入）：")
    while True:
        line = input()
        if not line.strip():
            break
        PASSWORDS.append(line.strip())

# ================== 是否可能是压缩包 ==================

def is_possible_archive(path):
    try:
        with open(path, "rb") as f:
            sig = f.read(8)
        return (
            sig.startswith(MAGIC_7Z) or
            sig.startswith(MAGIC_RAR) or
            sig.startswith(MAGIC_ZIP)
        )
    except:
        return False

# ================== 提取逻辑压缩包名 ==================

def get_logical_name(filename: str) -> str:
    name = filename
    name = re.sub(r"\.part\d+\.rar$", "", name, flags=re.I)
    name = re.sub(r"\.(7z|zip|rar)\.\d+$", "", name, flags=re.I)
    return os.path.splitext(name)[0]

# ================== 扫描并分组 ==================

def scan_archives():
    groups = defaultdict(list)
    for root, _, files in os.walk(ROOT):
        for f in files:
            path = os.path.join(root, f)
            if is_possible_archive(path) or re.search(r"\.(part\d+\.rar|\d+)$", f, re.I):
                lname = get_logical_name(f).lower()
                key = os.path.join(root, lname)
                groups[key].append(path)

    archives = []
    for key, paths in groups.items():
        with LOCK:
            if key in PROCESSED or key in IN_PROGRESS:
                continue
            paths.sort()
            if not any(is_possible_archive(p) for p in paths):
                continue
            main = None
            for p in paths:
                if re.search(r"\.(part0*1\.rar|7z\.001|zip\.001)$", p, re.I):
                    main = p
                    break
            if not main: main = paths[0]
            PROCESSED.add(key)
            archives.append((key, main, paths))
    return archives

# ================== IO 自适应并发 ==================

def adjust_workers():
    global MAX_WORKERS
    last = psutil.disk_io_counters()
    last_bytes = last.read_bytes + last.write_bytes

    LOW = 30 * 1024 * 1024 * CHECK_INTERVAL
    MID = 80 * 1024 * 1024 * CHECK_INTERVAL
    HIGH = 160 * 1024 * 1024 * CHECK_INTERVAL

    while True:
        time.sleep(CHECK_INTERVAL)
        now = psutil.disk_io_counters()
        now_bytes = now.read_bytes + now.write_bytes
        delta = now_bytes - last_bytes
        last_bytes = now_bytes

        if delta < LOW: MAX_WORKERS += 3
        elif delta < MID: MAX_WORKERS += 2
        elif delta < HIGH: MAX_WORKERS += 1
        else: MAX_WORKERS -= 1

        MAX_WORKERS = max(MIN_WORKERS, min(MAX_WORKERS, MAX_WORKERS_LIMIT))
        # 修正信号量值
        WORKER_SEM._value = MAX_WORKERS
        # print(f"[IO] Δ={delta/1024/1024:.1f}MB workers={MAX_WORKERS}")

# ================== 空间不足处理 ==================

def ensure_space(required_gb=5):
    required_bytes = required_gb * 1024 ** 3
    while True:
        usage = shutil.disk_usage(ROOT)
        if usage.free > required_bytes:
            break

        with LOCK:
            if not UNPACKED_ARCHIVES:
                print("[CRITICAL] 磁盘已满，无可删除的压缩包！")
                return False

            archive_files = UNPACKED_ARCHIVES.popleft()
            for file_path in archive_files:
                if os.path.exists(file_path):
                    print(f"[SPACE] 释放空间：正在删除分卷 {os.path.basename(file_path)}")
                    try:
                        send2trash(file_path)
                    except:
                        try: os.remove(file_path)
                        except: pass
    return True

# ================== 解压函数 ==================

def extract(task):
    key, archive, all_parts = task

    if not ensure_space(5):
        print(f"[SKIP] 空间不足，跳过: {archive}")
        return False

    with WORKER_SEM:
        with LOCK:
            IN_PROGRESS.add(key)

        try:
            print(f"\n[EXTRACT] 开始: {archive} (共 {len(all_parts)} 分卷)")
            base = os.path.basename(key)
            out_dir = os.path.join(os.path.dirname(archive), base)
            os.makedirs(out_dir, exist_ok=True)

            success = False
            for pwd in PASSWORDS + [""]:
                pwd_show = pwd if pwd else "<无密码>"
                # print(f"[EXTRACT] 尝试密码 {pwd_show}")

                cmd = ["7z", "x", archive, f"-o{out_dir}", "-y"]
                if pwd: cmd.append(f"-p{pwd}")

                # 核心执行部分，定义 r
                r = subprocess.run(
                    cmd, stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                )

                if r.returncode == 0:
                    print(f"[EXTRACT] 成功: {archive}")
                    with LOCK:
                        UNPACKED_ARCHIVES.append(all_parts)
                    success = True
                    break

                # 检查是否因为空间不足失败
                err = r.stderr.lower()
                if "no space" in err or "write error" in err or r.returncode == 8:
                    print(f"[SPACE] 磁盘空间不足，尝试清理重试...")
                    if ensure_space(10):
                        return extract(task) # 递归重试

                if "wrong password" not in err:
                    break

            if not success:
                FAILED.append(archive)
                print(f"[EXTRACT] 失败: {archive}")
            return success

        finally:
            with LOCK:
                IN_PROGRESS.discard(key)

# ================== 压平目录 ==================

def flatten_dirs(base):
    print("\n[CLEAN] 压平单分支目录...")
    for root, dirs, files in os.walk(base, topdown=False):
        if len(dirs) == 1 and not files:
            child = os.path.join(root, dirs[0])
            for item in os.listdir(child):
                src, dst = os.path.join(child, item), os.path.join(root, item)
                try: os.rename(src, dst)
                except OSError:
                    if os.path.exists(dst): os.remove(dst)
                    os.rename(src, dst)
            try: os.rmdir(child)
            except: pass

# ================== 主流程 ==================

def main():
    read_passwords()
    start_time = time.time()

    threading.Thread(target=adjust_workers, daemon=True).start()
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS_LIMIT)

    while True:
        tasks = scan_archives()
        if not tasks: break
        futures = [executor.submit(extract, t) for t in tasks]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"[ERROR] 线程异常: {e}")

    executor.shutdown(wait=True)

    print("\n[CLEAN] 正在删除已成功解压的分卷...")
    with LOCK:
        while UNPACKED_ARCHIVES:
            file_list = UNPACKED_ARCHIVES.popleft()
            for f in file_list:
                if os.path.exists(f):
                    try: send2trash(f)
                    except: pass

    flatten_dirs(ROOT)

    print("\n" + "="*20 + " 处理结果汇总 " + "="*20)
    print(f"总计耗时: {(time.time()-start_time)/60:.2f} 分钟")
    if FAILED:
        print(f"失败任务数: {len(FAILED)}")
        for f in FAILED: print(f" [×] {f}")
        with open("failed_log.txt", "w", encoding="utf-8") as log:
            for f in FAILED: log.write(f + "\n")
        print(f"详细列表已保存至: failed_log.txt")
    else:
        print(" [√] 全部解压成功！")
    print("="*54)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
    finally:
        print("\n" + "="*40)
        input("程序运行结束，按回车退出窗口...")