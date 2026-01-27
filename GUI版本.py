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
from concurrent.futures import ThreadPoolExecutor, as_completed
from send2trash import send2trash
from collections import deque, defaultdict

# ================== GUI Application Wrapper ==================

class ArchiveUnpackerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("智能解压工具 (GUI版)")
        self.root.geometry("700x500")

        # Configuration & State
        self.root_dir = os.getcwd()
        self.passwords = []
        self.is_running = False
        self.failed_tasks = []
        self.unpacked_archives = deque()
        self.processed = set()          # 已处理的逻辑压缩包
        self.in_progress = set()        # 正在解压的逻辑压缩包
        self.lock = threading.Lock()
        
        # Concurrency Control
        self.max_workers = 2
        self.min_workers = 1
        self.max_workers_limit = max(2, os.cpu_count() or 4)
        self.worker_sem = threading.Semaphore(self.max_workers)
        
        # Determine 7z path (Compatible with PyInstaller)
        self.seven_z_path = self.get_resource_path("7z.exe")
        if not os.path.exists(self.seven_z_path):
             self.seven_z_path = "7z" # Fallback to system path

        self.setup_ui()

    def get_resource_path(self, relative_path):
        """ Get absolute path to resource, works for dev and for PyInstaller """
        try:
            # PyInstaller creates a temp folder and stores path in _MEIPASS
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.dirname(os.path.abspath(__file__))

        return os.path.join(base_path, relative_path)

    def setup_ui(self):
        # 1. Directory Selection
        frame_dir = tk.Frame(self.root)
        frame_dir.pack(pady=5, fill=tk.X, padx=10)
        
        tk.Label(frame_dir, text="工作目录:").pack(side=tk.LEFT)
        self.lbl_dir = tk.Entry(frame_dir)
        self.lbl_dir.insert(0, self.root_dir)
        self.lbl_dir.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        tk.Button(frame_dir, text="选择...", command=self.select_directory).pack(side=tk.LEFT)

        # 2. Password Input
        frame_pwd = tk.Frame(self.root)
        frame_pwd.pack(pady=5, fill=tk.X, padx=10)
        tk.Label(frame_pwd, text="解压密码 (一行一个):").pack(anchor="w")
        self.txt_pwd = tk.Text(frame_pwd, height=4)
        self.txt_pwd.pack(fill=tk.X)

        # 3. Control Buttons
        frame_btn = tk.Frame(self.root)
        frame_btn.pack(pady=5)
        self.btn_start = tk.Button(frame_btn, text="开始处理", command=self.start_thread, bg="#DDDDDD", width=15)
        self.btn_start.pack()

        # 4. Log Output
        tk.Label(self.root, text="运行日志:").pack(anchor="w", padx=10)
        self.txt_log = scrolledtext.ScrolledText(self.root, state='disabled')
        self.txt_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

    def log(self, message):
        def _log():
            self.txt_log.config(state='normal')
            self.txt_log.insert(tk.END, str(message) + "\n")
            self.txt_log.see(tk.END)
            self.txt_log.config(state='disabled')
        self.root.after(0, _log)

    def select_directory(self):
        d = filedialog.askdirectory(initialdir=self.root_dir)
        if d:
            self.root_dir = d
            self.lbl_dir.delete(0, tk.END)
            self.lbl_dir.insert(0, d)

    def start_thread(self):
        if self.is_running:
            return
        
        # Refresh config from UI
        self.root_dir = self.lbl_dir.get()
        pwd_text = self.txt_pwd.get("1.0", tk.END).strip()
        self.passwords = [line.strip() for line in pwd_text.split('\n') if line.strip()]
        
        self.btn_start.config(state='disabled', text="正在运行...")
        self.is_running = True
        
        # Reset state
        self.failed_tasks = []
        self.unpacked_archives = deque()
        self.processed = set()
        self.in_progress = set()
        
        threading.Thread(target=self.run_logic, daemon=True).start()

    # ================== Logic Methods (Adapted) ==================

    def is_possible_archive(self, path):
        MAGIC_7Z = b"7z\xbc\xaf\x27\x1c"
        MAGIC_RAR = b"Rar!"
        MAGIC_ZIP = b"PK"
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

    def get_logical_name(self, filename: str) -> str:
        name = filename
        name = re.sub(r"\.part\d+\.rar$", "", name, flags=re.I)
        name = re.sub(r"\.(7z|zip|rar)\.\d+$", "", name, flags=re.I)
        return os.path.splitext(name)[0]

    def scan_archives(self):
        groups = defaultdict(list)
        for root, _, files in os.walk(self.root_dir):
            for f in files:
                path = os.path.join(root, f)
                if self.is_possible_archive(path) or re.search(r"\.(part\d+\.rar|\d+)", f, re.I):
                    lname = self.get_logical_name(f).lower()
                    key = os.path.join(root, lname)
                    groups[key].append(path)

        archives = []
        for key, paths in groups.items():
            with self.lock:
                if key in self.processed or key in self.in_progress:
                    continue
                paths.sort()
                if not any(self.is_possible_archive(p) for p in paths):
                    continue
                main = None
                for p in paths:
                    if re.search(r"\.(part0*1\.rar|7z\.001|zip\.001)$", p, re.I):
                        main = p
                        break
                if not main: main = paths[0]
                self.processed.add(key)
                archives.append((key, main, paths))
        return archives

    def adjust_workers(self):
        last = psutil.disk_io_counters()
        last_bytes = last.read_bytes + last.write_bytes
        CHECK_INTERVAL = 3

        LOW = 30 * 1024 * 1024 * CHECK_INTERVAL
        MID = 80 * 1024 * 1024 * CHECK_INTERVAL
        HIGH = 160 * 1024 * 1024 * CHECK_INTERVAL

        while self.is_running:
            time.sleep(CHECK_INTERVAL)
            now = psutil.disk_io_counters()
            now_bytes = now.read_bytes + now.write_bytes
            delta = now_bytes - last_bytes
            last_bytes = now_bytes

            if delta < LOW: self.max_workers += 3
            elif delta < MID: self.max_workers += 2
            elif delta < HIGH: self.max_workers += 1
            else: self.max_workers -= 1

            self.max_workers = max(self.min_workers, min(self.max_workers, self.max_workers_limit))
            # Update semaphore directly (risky but consistent with original logic)
            self.worker_sem._value = self.max_workers

    def ensure_space(self, required_gb=5):
        required_bytes = required_gb * 1024 ** 3
        while True:
            try:
                usage = shutil.disk_usage(self.root_dir)
            except FileNotFoundError:
                # Root dir might have been moved/deleted or is invalid
                return False
                
            if usage.free > required_bytes:
                break

            with self.lock:
                if not self.unpacked_archives:
                    self.log("[CRITICAL] 磁盘已满，无可删除的压缩包！")
                    return False

                archive_files = self.unpacked_archives.popleft()
                for file_path in archive_files:
                    if os.path.exists(file_path):
                        self.log(f"[SPACE] 释放空间：正在删除分卷 {os.path.basename(file_path)}")
                        try:
                            send2trash(file_path)
                        except:
                            try: os.remove(file_path)
                            except: pass
        return True

    def extract(self, task):
        key, archive, all_parts = task

        if not self.ensure_space(5):
            self.log(f"[SKIP] 空间不足，跳过: {archive}")
            return False

        with self.worker_sem:
            with self.lock:
                self.in_progress.add(key)

            try:
                self.log(f"\n[EXTRACT] 开始: {archive} (共 {len(all_parts)} 分卷)")
                base = os.path.basename(key)
                out_dir = os.path.join(os.path.dirname(archive), base)
                os.makedirs(out_dir, exist_ok=True)

                success = False
                for pwd in self.passwords + [""]:
                    # cmd = ["7z", "x", archive, f"-o{out_dir}", "-y"]
                    cmd = [self.seven_z_path, "x", archive, f"-o{out_dir}", "-y"]
                    if pwd: cmd.append(f"-p{pwd}")

                    # Hide window on Windows
                    startupinfo = None
                    if os.name == 'nt':
                        startupinfo = subprocess.STARTUPINFO()
                        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

                    r = subprocess.run(
                        cmd, stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                        startupinfo=startupinfo
                    )

                    if r.returncode == 0:
                        self.log(f"[EXTRACT] 成功: {archive}")
                        with self.lock:
                            self.unpacked_archives.append(all_parts)
                        success = True
                        break

                    err = r.stderr.lower()
                    if "no space" in err or "write error" in err or r.returncode == 8:
                        self.log(f"[SPACE] 磁盘空间不足，尝试清理重试...")
                        if self.ensure_space(10):
                            return self.extract(task) # 递归重试

                    if "wrong password" not in err:
                        break

                if not success:
                    self.failed_tasks.append(archive)
                    self.log(f"[EXTRACT] 失败: {archive}")
                return success

            finally:
                with self.lock:
                    self.in_progress.discard(key)

    def flatten_dirs(self, base):
        self.log("\n[CLEAN] 压平单分支目录...")
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

    def run_logic(self):
        start_time = time.time()
        self.log(f"开始扫描目录: {self.root_dir}")
        self.log(f"使用 7z 路径: {self.seven_z_path}")

        # Start IO monitor
        monitor_thread = threading.Thread(target=self.adjust_workers, daemon=True)
        monitor_thread.start()

        executor = ThreadPoolExecutor(max_workers=self.max_workers_limit)

        try:
            while True:
                tasks = self.scan_archives()
                if not tasks: break
                
                futures = [executor.submit(self.extract, t) for t in tasks]
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        self.log(f"[ERROR] 线程异常: {e}")
            
            executor.shutdown(wait=True)

            self.log("\n[CLEAN] 正在删除已成功解压的分卷...")
            with self.lock:
                while self.unpacked_archives:
                    file_list = self.unpacked_archives.popleft()
                    for f in file_list:
                        if os.path.exists(f):
                            try: send2trash(f)
                            except: pass

            self.flatten_dirs(self.root_dir)

            self.log("\n" + "="*20 + " 处理结果汇总 " + "="*20)
            self.log(f"总计耗时: {(time.time()-start_time)/60:.2f} 分钟")
            
            if self.failed_tasks:
                self.log(f"失败任务数: {len(self.failed_tasks)}")
                for f in self.failed_tasks: self.log(f" [×] {f}")
                
                log_path = os.path.join(self.root_dir, "failed_log.txt")
                with open(log_path, "w", encoding="utf-8") as log:
                    for f in self.failed_tasks: log.write(f + "\n")
                self.log(f"详细列表已保存至: {log_path}")
            else:
                self.log(" [√] 全部解压成功！")
            self.log("="*54)

        except Exception as e:
            self.log(f"Fatal Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.is_running = False
            self.root.after(0, lambda: self.btn_start.config(state='normal', text="开始处理"))

# ================== Main Entry ==================

if __name__ == "__main__":
    root = tk.Tk()
    app = ArchiveUnpackerApp(root)
    root.mainloop()
