import os
import sys
import shutil
import subprocess
import threading
import time
import psutil
import re
import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from send2trash import send2trash
from collections import deque, defaultdict

# ================== Core Logic Engine ==================

class DecompressionEngine:
    def __init__(self, root_dir, passwords, log_callback, completion_callback):
        self.root_dir = root_dir
        self.passwords = passwords
        self.log_callback = log_callback
        self.completion_callback = completion_callback
        
        # State
        self.is_running = False
        self.failed_tasks = []
        self.unpacked_archives = deque()
        self.processed = set()
        self.in_progress = set()
        self.lock = threading.Lock()
        
        # Concurrency Control
        self.io_history = deque(maxlen=5)
        self.max_retries = 3
        self.min_workers = 1
        self.max_workers_limit = self.detect_max_workers()
        self.active_workers = 0
        self.current_concurrency_limit = min(2, self.max_workers_limit)
        self.concurrency_cond = threading.Condition(self.lock)
        
        # 7z Path
        self.seven_z_path = self.get_resource_path("7z.exe")
        if not os.path.exists(self.seven_z_path):
            self.seven_z_path = "7z"

    def get_resource_path(self, relative_path):
        base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_path, relative_path)

    def detect_max_workers(self):
        cpu_count = os.cpu_count() or 4
        try:
            cmd = "powershell -Command \"Get-PhysicalDisk | Select-Object -Property MediaType\""
            res = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            if "SSD" in res.stdout.upper():
                return max(2, cpu_count)
        except:
            pass
        return 2

    def log(self, message):
        if self.log_callback:
            self.log_callback(message)

    def is_possible_archive(self, path):
        MAGIC_7Z, MAGIC_RAR, MAGIC_ZIP = b"7z\xbc\xaf'\x1c", b"Rar!", b"PK"
        try:
            with open(path, "rb") as f:
                sig = f.read(8)
            return any(sig.startswith(m) for m in [MAGIC_7Z, MAGIC_RAR, MAGIC_ZIP])
        except:
            return False

    def get_logical_name(self, filename: str) -> str:
        name = re.sub(r"\.part\d+\.rar$", "", filename, flags=re.I)
        name = re.sub(r"\.(7z|zip|rar)\.\d+$", "", name, flags=re.I)
        return os.path.splitext(name)[0].strip().rstrip('.')

    def scan_archives(self, target_dir=None):
        target_dir = target_dir or self.root_dir
        groups = defaultdict(list)
        for root, _, files in os.walk(target_dir):
            for f in files:
                path = os.path.join(root, f)
                if self.is_possible_archive(path) or re.search(r"\.(part\d+\.rar|[rz]?\d+)$", f, re.I):
                    lname = self.get_logical_name(f).lower()
                    key = os.path.join(root, lname)
                    groups[key].append(path)

        archives = []
        for key, paths in groups.items():
            with self.lock:
                if key in self.processed or key in self.in_progress: continue
                paths.sort()
                if not any(self.is_possible_archive(p) for p in paths): continue
                main = next((p for p in paths if re.search(r"\.(part0*1\.rar|7z\.001|zip\.001|7z|zip|rar)$", p, re.I)), paths[0])
                self.processed.add(key)
                archives.append((key, main, paths))
        return archives

    def adjust_workers(self):
        last = psutil.disk_io_counters()
        last_bytes = (last.read_bytes + last.write_bytes) if last else 0
        while self.is_running:
            time.sleep(1)
            now = psutil.disk_io_counters()
            if not now: continue
            now_bytes = now.read_bytes + now.write_bytes
            delta = now_bytes - last_bytes
            last_bytes = now_bytes
            self.io_history.append(delta)
            avg_delta = sum(self.io_history) / len(self.io_history)
            
            with self.concurrency_cond:
                old_limit = self.current_concurrency_limit
                if avg_delta < 10*1024*1024: self.current_concurrency_limit += 1
                elif avg_delta > 60*1024*1024: self.current_concurrency_limit -= 1
                self.current_concurrency_limit = max(self.min_workers, min(self.current_concurrency_limit, self.max_workers_limit))
                if old_limit != self.current_concurrency_limit:
                    self.concurrency_cond.notify_all()

    def ensure_space(self, required_gb=5):
        required_bytes = required_gb * 1024 ** 3
        while True:
            try:
                if shutil.disk_usage(self.root_dir).free > required_bytes: break
            except:
                return False
            with self.lock:
                if not self.unpacked_archives:
                    self.log("[CRITICAL] 磁盘已满，无可删除的压缩包！")
                    return False
                for f in self.unpacked_archives.popleft():
                    if os.path.exists(f):
                        self.log(f"[SPACE] 释放空间：正在删除 {os.path.basename(f)}")
                        try: send2trash(f)
                        except:
                            try: os.remove(f)
                            except: pass
        return True

    def extract(self, task):
        key, archive, all_parts = task
        retry_count = 0
        with self.concurrency_cond:
            while self.active_workers >= self.current_concurrency_limit:
                self.concurrency_cond.wait()
            self.active_workers += 1
        try:
            with self.lock: self.in_progress.add(key)
            out_dir = os.path.join(os.path.dirname(archive), os.path.basename(key))
            
            while retry_count < self.max_retries:
                if not self.ensure_space(5): return None
                self.log(f"\n[EXTRACT] 开始: {archive}")
                os.makedirs(out_dir, exist_ok=True)
                correct_pwd, r, err = None, None, ""
                si = subprocess.STARTUPINFO() if os.name == 'nt' else None
                if si: si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                
                # 1. Password Test
                for pwd in self.passwords + [""]:
                    cmd = [self.seven_z_path, "t", archive, "-y"]
                    if pwd: cmd.append(f"-p{pwd}")
                    rt = subprocess.run(cmd, capture_output=True, text=True, startupinfo=si)
                    if rt.returncode == 0:
                        correct_pwd = pwd
                        break
                    err = rt.stderr.lower()
                    if "wrong password" not in err: break

                # 2. Extract
                if correct_pwd is not None or not self.passwords:
                    cmd = [self.seven_z_path, "x", archive, f"-o{out_dir}", "-y"]
                    if correct_pwd: cmd.append(f"-p{correct_pwd}")
                    r = subprocess.run(cmd, capture_output=True, text=True, startupinfo=si)
                    if r.returncode == 0:
                        self.log(f"[EXTRACT] 成功: {archive}")
                        with self.lock: self.unpacked_archives.append(all_parts)
                        return out_dir
                    err = r.stderr.lower()

                # 3. Retry on space error
                if r and ("no space" in err or "write error" in err or r.returncode == 8):
                    if self.ensure_space(10):
                        retry_count += 1
                        continue
                
                # 4. Final Failure Handling
                error_msg = "原因未知"
                if r:
                    code = r.returncode
                    if code == 1: error_msg = "警告 (文件被占用或部分失败)"
                    elif code == 2: error_msg = "致命错误 (文件损坏或格式不支持)"
                    elif code == 7: error_msg = "命令行参数错误"
                    elif code == 8: error_msg = "内存/磁盘空间不足"
                    elif code == 255: error_msg = "用户中断"
                if "wrong password" in err: error_msg = "密码错误"
                
                # Cleanup failed directory
                if os.path.exists(out_dir):
                    try: shutil.rmtree(out_dir)
                    except: pass
                
                self.failed_tasks.append(f"{os.path.basename(archive)} [{error_msg}]")
                self.log(f"[EXTRACT] 失败: {archive} (错误: {error_msg})")
                return None
        finally:
            with self.lock: self.in_progress.discard(key)
            with self.concurrency_cond:
                self.active_workers -= 1
                self.concurrency_cond.notify_all()

    def flatten_dirs(self, base):
        self.log("\n[CLEAN] 压平单分支目录...")
        for root, dirs, files in os.walk(base, topdown=False):
            if len(dirs) == 1 and not files:
                child_path = os.path.join(root, dirs[0])
                if os.path.exists(child_path):
                    for item in os.listdir(child_path):
                        src, dst = os.path.join(child_path, item), os.path.join(root, item)
                        final_dst = dst
                        if os.path.exists(dst) and os.path.abspath(src).lower() != os.path.abspath(dst).lower():
                            b, e = os.path.splitext(item)
                            c = 1
                            while os.path.exists(final_dst):
                                final_dst = os.path.join(root, f"{b} ({c}){e}")
                                c += 1
                        try: shutil.move(src, final_dst)
                        except: pass
                    try: os.rmdir(child_path)
                    except: pass

    def start(self):
        self.is_running = True
        threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        start_time = time.time()
        threading.Thread(target=self.adjust_workers, daemon=True).start()
        executor = ThreadPoolExecutor(max_workers=self.max_workers_limit)
        pending = deque(self.scan_archives())
        futures = {}
        success_count = 0
        try:
            while pending or futures or self.in_progress:
                while pending and len(futures) < self.max_workers_limit * 2:
                    t = pending.popleft()
                    futures[executor.submit(self.extract, t)] = t
                if not futures:
                    time.sleep(0.5)
                    continue
                done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED, timeout=1)
                for f in done:
                    if f in futures:
                        futures.pop(f)
                        try:
                            res = f.result()
                            if res and os.path.exists(res):
                                success_count += 1
                                new = self.scan_archives(res)
                                if new: pending.extend(new)
                        except: pass
            executor.shutdown(wait=True)
            self.log("\n[CLEAN] 删除已成功解压的分卷...")
            with self.lock:
                while self.unpacked_archives:
                    for f in self.unpacked_archives.popleft():
                        if os.path.exists(f):
                            try: send2trash(f)
                            except: pass
            self.flatten_dirs(self.root_dir)
            
            # --- Final Summary Report ---
            self.log("\n" + "="*20 + " 处理结果汇总 " + "="*20)
            self.log(f"总计耗时: {(time.time()-start_time)/60:.2f} 分钟")
            self.log(f"成功解压: {success_count} 个")
            if self.failed_tasks:
                self.log(f"失败任务: {len(self.failed_tasks)} 个")
                log_path = os.path.join(self.root_dir, "failed_log.txt")
                try:
                    with open(log_path, "w", encoding="utf-8") as f_log:
                        for ft in self.failed_tasks:
                            self.log(f" [×] {ft}")
                            f_log.write(f"{ft}\n")
                    self.log(f"详细失败列表已保存至: {log_path}")
                except:
                    self.log("[ERROR] 无法保存失败日志文件。")
            else:
                self.log(" [√] 全部任务已成功处理！")
            self.log("="*54)
        finally:
            self.is_running = False
            if self.completion_callback: self.completion_callback()

# ================== GUI Wrapper ==================

class ArchiveUnpackerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("智能解压工具 (GUI版)")
        self.root.geometry("700x500")
        self.setup_ui()

    def setup_ui(self):
        f1 = tk.Frame(self.root); f1.pack(pady=5, fill=tk.X, padx=10)
        tk.Label(f1, text="工作目录:").pack(side=tk.LEFT)
        self.ent_dir = tk.Entry(f1); self.ent_dir.insert(0, os.getcwd()); self.ent_dir.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        tk.Button(f1, text="选择...", command=lambda: self.ent_dir.insert(0, filedialog.askdirectory() or self.ent_dir.get())).pack(side=tk.LEFT)

        f2 = tk.Frame(self.root); f2.pack(pady=5, fill=tk.X, padx=10)
        tk.Label(f2, text="解压密码 (一行一个):").pack(anchor="w")
        self.txt_pwd = tk.Text(f2, height=4); self.txt_pwd.pack(fill=tk.X)

        self.btn_start = tk.Button(self.root, text="开始处理", command=self.start, bg="#DDDDDD", width=15); self.btn_start.pack(pady=5)
        self.txt_log = scrolledtext.ScrolledText(self.root, state='disabled'); self.txt_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

    def log(self, msg):
        self.root.after(0, lambda: (self.txt_log.config(state='normal'), self.txt_log.insert(tk.END, f"{msg}\n"), self.txt_log.see(tk.END), self.txt_log.config(state='disabled')))

    def start(self):
        pwds = [l.strip() for l in self.txt_pwd.get("1.0", tk.END).split('\n') if l.strip()]
        self.btn_start.config(state='disabled', text="正在运行...")
        engine = DecompressionEngine(self.ent_dir.get(), pwds, self.log, lambda: self.root.after(0, lambda: self.btn_start.config(state='normal', text="开始处理")))
        engine.start()

if __name__ == "__main__":
    root = tk.Tk()
    ArchiveUnpackerApp(root)
    root.mainloop()
