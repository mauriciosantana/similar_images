import os
import subprocess
import threading

class FastExifTool:
    """ExifToolを常駐させて高速に処理するクラス"""
    def __init__(self, executable):
        self.executable = executable
        self.process = None
        self.lock = threading.Lock()

    def start(self):
        if not self.executable or not os.path.exists(self.executable):
            return
        # Windows環境で裏コマンド実行時の黒窓ポップアップを防ぐ
        creationflags = 0x08000000 if os.name == 'nt' else 0
        self.process = subprocess.Popen(
            [self.executable, "-stay_open", "True", "-@", "-"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", bufsize=1, creationflags=creationflags
        )

    def execute(self, *args):
        with self.lock:
            if not self.process or self.process.poll() is not None:
                self.start()
            if not self.process:
                return False, "ExifToolが見つかりません"
            
            try:
                for arg in args:
                    self.process.stdin.write(arg + "\n")
                self.process.stdin.write("-execute\n")
                self.process.stdin.flush()
                
                output = ""
                while True:
                    line = self.process.stdout.readline()
                    if not line: break
                    if line.strip() == "{ready}": break
                    output += line
                
                is_success = "files updated" in output or "files created" in output or "image files read" in output
                return is_success, output
            except Exception as e:
                return False, str(e)

    def stop(self):
        if self.process:
            try:
                self.process.stdin.write("-stay_open\nFalse\n")
                self.process.stdin.flush()
                self.process.wait(timeout=3)
            except Exception:
                self.process.kill()
            self.process = None