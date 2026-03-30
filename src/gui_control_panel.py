# -*- coding: utf-8 -*-
"""
ProjectZZZ - Панель Управления (GUI)
Версия: 2.0 - ЧИСТЫЙ КОД БЕЗ ОШИБОК
Запуск: python src\gui_control_panel.py
"""
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import subprocess
import threading
import os
import sys
from datetime import datetime
from pathlib import Path
import yaml

try:
    import psycopg2
except ImportError:
    psycopg2 = None

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"
SRC_DIR = PROJECT_ROOT / "src"
MODELS_DIR = PROJECT_ROOT / "models"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==============================================================================
# КЛАСС ПАНЕЛИ УПРАВЛЕНИЯ
# ==============================================================================
class ProjectZZZControlPanel:
    def __init__(self, root):
        self.root = root
        self.root.title("ProjectZZZ - Панель Управления v2.0")
        self.root.geometry("1400x900")
        
        self.db_connected = False
        self.auto_mode = tk.BooleanVar(value=False)
        self.running_scripts = {}
        
        self.setup_styles()
        self.create_header()
        self.create_db_status_panel()
        self.create_auto_mode_checkbox()
        self.create_control_buttons()
        self.create_log_panel()
        self.create_status_bar()
        
        self.check_database_connection()
    
    def setup_styles(self):
        """Настройка стилей интерфейса"""
        style = ttk.Style()
        style.theme_use('clam')
        
        style.configure('Header.TLabel', font=('Segoe UI', 16, 'bold'), foreground='#2c3e50')
        style.configure('Status.TLabel', font=('Segoe UI', 10), foreground='#34495e')
        style.configure('Success.TLabel', foreground='#27ae60', font=('Segoe UI', 10, 'bold'))
        style.configure('Error.TLabel', foreground='#e74c3c', font=('Segoe UI', 10, 'bold'))
        style.configure('Warning.TLabel', foreground='#f39c12', font=('Segoe UI', 10, 'bold'))
        style.configure('Primary.TButton', font=('Segoe UI', 11, 'bold'), padding=12)
        style.configure('Secondary.TButton', font=('Segoe UI', 10), padding=8)
        style.configure('Danger.TButton', font=('Segoe UI', 10, 'bold'), foreground='#c0392b', padding=10)
    
    def create_header(self):
        """Создаёт заголовок окна"""
        header_frame = ttk.Frame(self.root, padding="10")
        header_frame.pack(fill=tk.X)
        
        title_label = ttk.Label(
            header_frame, 
            text="ProjectZZZ - Система Рекомендаций SKU", 
            style='Header.TLabel'
        )
        title_label.pack(side=tk.LEFT)
        
        version_label = ttk.Label(
            header_frame, 
            text="v3.1 | " + datetime.now().strftime("%Y-%m-%d %H:%M"),
            style='Status.TLabel'
        )
        version_label.pack(side=tk.RIGHT)
        
        ttk.Separator(self.root, orient='horizontal').pack(fill=tk.X)
    
    def create_db_status_panel(self):
        """Панель статуса БД"""
        db_frame = ttk.LabelFrame(self.root, text="Статус Базы Данных", padding="10")
        db_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.db_status_label = ttk.Label(db_frame, text="Не подключено", style='Error.TLabel')
        self.db_status_label.pack(side=tk.LEFT, padx=5)
        
        self.db_info_label = ttk.Label(db_frame, text="", style='Status.TLabel')
        self.db_info_label.pack(side=tk.LEFT, padx=20)
        
        refresh_btn = ttk.Button(
            db_frame, 
            text="Обновить", 
            command=self.check_database_connection,
            style='Secondary.TButton'
        )
        refresh_btn.pack(side=tk.RIGHT)
    
    def create_auto_mode_checkbox(self):
        """Чекбокс автоматического режима"""
        auto_frame = ttk.Frame(self.root, padding="5")
        auto_frame.pack(fill=tk.X, padx=10, pady=2)
        
        chk = ttk.Checkbutton(
            auto_frame, 
            text="АВТОМАТИЧЕСКИЙ РЕЖИМ (ежедневно 05:00/06:00)", 
            variable=self.auto_mode
        )
        chk.pack(side=tk.LEFT, padx=10)
        
        self.mode_label = ttk.Label(
            auto_frame, 
            text="Ручной режим", 
            foreground='#e74c3c'
        )
        self.mode_label.pack(side=tk.LEFT, padx=20)
        
        self.auto_mode.trace('w', self.update_mode_label)
    
    def update_mode_label(self, *args):
        """Обновляет метку режима"""
        if self.auto_mode.get():
            self.mode_label.config(text="Автоматический режим", foreground='#27ae60')
        else:
            self.mode_label.config(text="Ручной режим", foreground='#e74c3c')
    
    def create_control_buttons(self):
        """Панель кнопок управления"""
        control_frame = ttk.LabelFrame(self.root, text="Управление Процессами", padding="20")
        control_frame.pack(fill=tk.X, padx=10, pady=10)
        
        btn_width = 45
        btn_padx = 15
        btn_pady = 15
        
        # Первая строка - Загрузка данных
        ttk.Label(
            control_frame, 
            text="Загрузка данных:", 
            font=('Segoe UI', 10, 'bold')
        ).grid(row=0, column=0, sticky=tk.W, pady=5, padx=5)
        
        self.btn_load_refs = ttk.Button(
            control_frame, 
            text="1. Справочники (умная загрузка)", 
            command=lambda: self.run_script("load_references.py"),
            style='Primary.TButton',
            width=btn_width
        )
        self.btn_load_refs.grid(row=0, column=1, padx=btn_padx, pady=btn_pady, sticky=tk.EW)
        
        self.btn_load_sales = ttk.Button(
            control_frame, 
            text="2. Продажи (умная загрузка)", 
            command=lambda: self.run_script("load_sales.py"),
            style='Primary.TButton',
            width=btn_width
        )
        self.btn_load_sales.grid(row=0, column=2, padx=btn_padx, pady=btn_pady, sticky=tk.EW)
        
        # Вторая строка - Принудительная загрузка
        ttk.Label(
            control_frame, 
            text="Принудительная:", 
            font=('Segoe UI', 10, 'bold')
        ).grid(row=1, column=0, sticky=tk.W, pady=5, padx=5)
        
        self.btn_load_sales_force = ttk.Button(
            control_frame, 
            text="2. Продажи [FORCE]", 
            command=self.run_force_load_sales,
            style='Danger.TButton',
            width=btn_width
        )
        self.btn_load_sales_force.grid(row=1, column=1, padx=btn_padx, pady=btn_pady, sticky=tk.EW)
        
        ttk.Label(
            control_frame, 
            text="ML Процессы:", 
            font=('Segoe UI', 10, 'bold')
        ).grid(row=1, column=2, sticky=tk.E, pady=5, padx=5)
        
        # Третья строка - ML процессы
        self.btn_backtest = ttk.Button(
            control_frame, 
            text="3. Backtesting", 
            command=lambda: self.run_script("backtest_engine.py"),
            style='Primary.TButton',
            width=btn_width
        )
        self.btn_backtest.grid(row=2, column=1, padx=btn_padx, pady=btn_pady, sticky=tk.EW)
        
        self.btn_generate = ttk.Button(
            control_frame, 
            text="4. Генерация Рекомендаций", 
            command=lambda: self.run_script("generate_recommendations.py"),
            style='Primary.TButton',
            width=btn_width
        )
        self.btn_generate.grid(row=2, column=2, padx=btn_padx, pady=btn_pady, sticky=tk.EW)
        
        # Четвёртая строка - Утилиты
        ttk.Label(
            control_frame, 
            text="Утилиты:", 
            font=('Segoe UI', 10, 'bold')
        ).grid(row=3, column=0, sticky=tk.W, pady=5, padx=5)
        
        self.btn_view_db = ttk.Button(
            control_frame, 
            text="Просмотр БД (pgAdmin)", 
            command=self.open_db_viewer,
            width=btn_width
        )
        self.btn_view_db.grid(row=3, column=1, padx=btn_padx, pady=btn_pady, sticky=tk.EW)
        
        self.btn_clear_logs = ttk.Button(
            control_frame, 
            text="Очистить логи", 
            command=self.clear_logs,
            width=btn_width
        )
        self.btn_clear_logs.grid(row=3, column=2, padx=btn_padx, pady=btn_pady, sticky=tk.EW)
        
        # Пятая строка - GitHub
        self.btn_github = ttk.Button(
            control_frame, 
            text="Sync GitHub (Pull/Push)", 
            command=self.sync_github,
            style='Secondary.TButton',
            width=btn_width*2 + 3
        )
        self.btn_github.grid(row=4, column=1, columnspan=2, padx=btn_padx, pady=20, sticky=tk.EW)
        
        control_frame.grid_columnconfigure(1, weight=1)
        control_frame.grid_columnconfigure(2, weight=1)
    
    def create_log_panel(self):
        """Панель логов"""
        log_frame = ttk.LabelFrame(self.root, text="Журнал Событий (Logs)", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            wrap=tk.WORD, 
            width=140, 
            height=30,
            font=('Consolas', 9), 
            bg='#f8f9fa'
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        self.log_text.tag_configure('INFO', foreground='#2c3e50')
        self.log_text.tag_configure('SUCCESS', foreground='#27ae60', font=('Consolas', 9, 'bold'))
        self.log_text.tag_configure('ERROR', foreground='#e74c3c', font=('Consolas', 9, 'bold'))
        self.log_text.tag_configure('WARNING', foreground='#f39c12', font=('Consolas', 9, 'bold'))
        self.log_text.tag_configure('SYSTEM', foreground='#3498db', font=('Consolas', 9, 'bold'))
        
        btn_frame = ttk.Frame(log_frame)
        btn_frame.pack(fill=tk.X, pady=(5, 0))
        
        ttk.Button(
            btn_frame, 
            text="Открыть папку логов", 
            command=lambda: os.startfile(LOG_DIR)
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame, 
            text="Сохранить лог", 
            command=self.save_log
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame, 
            text="Открыть output", 
            command=lambda: os.startfile(OUTPUT_DIR)
        ).pack(side=tk.LEFT, padx=5)
    
    def create_status_bar(self):
        """Строка статуса"""
        self.status_bar = ttk.Label(
            self.root, 
            text="Готов к работе", 
            relief=tk.SUNKEN, 
            anchor=tk.W, 
            padding=5
        )
        self.status_bar.pack(fill=tk.X, side=tk.BOTTOM)
    
    def check_database_connection(self):
        """Проверяет подключение к БД"""
        self.log("Проверка подключения к БД...", "SYSTEM")
        
        if psycopg2 is None:
            self.db_status_label.config(text="psycopg2 не установлен", style='Error.TLabel')
            self.log("Библиотека psycopg2 не установлена!", "ERROR")
            return
        
        try:
            if not CONFIG_PATH.exists():
                self.db_status_label.config(text="config.yaml не найден", style='Error.TLabel')
                self.db_info_label.config(text="")
                self.log("Файл config.yaml не найден!", "ERROR")
                return
            
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            db = config['database']
            conn = psycopg2.connect(
                host=db['host'],
                port=db['port'],
                database=db['name'],
                user=db['user'],
                password=db['password']
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
            table_count = cursor.fetchone()[0]
            
            conn.close()
            
            self.db_connected = True
            self.db_status_label.config(text="Подключено", style='Success.TLabel')
            self.db_info_label.config(text=f"БД: {db['name']} | Таблиц: {table_count}")
            self.log(f"Подключение к БД успешно! Таблиц: {table_count}", "SUCCESS")
            
        except Exception as e:
            self.db_connected = False
            self.db_status_label.config(text="Ошибка подключения", style='Error.TLabel')
            self.db_info_label.config(text="")
            self.log(f"Ошибка подключения к БД: {e}", "ERROR")
    
    def run_script(self, script_name):
        """Запускает скрипт"""
        self._run_script_internal(script_name, force=False)
    
    def run_force_load_sales(self):
        """Принудительная загрузка продаж"""
        if messagebox.askyesno(
            "ПОДТВЕРДИТЕ FORCE ЗАГРУЗКУ", 
            "Вы собираетесь запустить ПРИНУДИТЕЛЬНУЮ загрузку продаж!\n\n"
            "Это:\n"
            "  - Удалит ВСЕ существующие данные из таблицы purchases\n"
            "  - Загрузит ВСЕ файлы заново (даже неизменённые)\n"
            "  - Займёт около 2-3 минут\n\n"
            "Обычная загрузка пропускает неизменённые файлы.\n\n"
            "Продолжить?"
        ):
            self._run_script_internal("load_sales.py", force=True)
    
    def _run_script_internal(self, script_name, force=False):
        """Внутренний метод запуска скрипта"""
        script_path = SRC_DIR / script_name
        
        if not script_path.exists():
            messagebox.showerror("Ошибка", f"Скрипт не найден:\n{script_path}")
            self.log(f"Скрипт {script_name} не найден!", "ERROR")
            return
        
        if script_name in self.running_scripts and self.running_scripts[script_name]:
            messagebox.showwarning("Внимание", f"Скрипт {script_name} уже выполняется!")
            return
        
        mode_str = " [FORCE]" if force else ""
        auto_str = " [AUTO]" if self.auto_mode.get() else ""
        
        self.log(f"\n{'='*60}", "SYSTEM")
        self.log(f"Запуск: {script_name}{mode_str}{auto_str}", "SYSTEM")
        self.log(f"{'='*60}\n", "SYSTEM")
        self.status_bar.config(text=f"Выполняется: {script_name}{mode_str}{auto_str}...")
        
        btn_map = {
            "load_references.py": self.btn_load_refs,
            "load_sales.py": self.btn_load_sales,
            "backtest_engine.py": self.btn_backtest,
            "generate_recommendations.py": self.btn_generate
        }
        
        btn = btn_map.get(script_name)
        if btn:
            btn.config(state='disabled')
        
        self.running_scripts[script_name] = True
        
        cmd = [sys.executable, str(script_path)]
        if force:
            cmd.append("--force")
        
        thread = threading.Thread(target=self._execute_script, args=(cmd, btn, script_name))
        thread.daemon = True
        thread.start()
    
    def _execute_script(self, cmd, btn, script_name):
        """Выполняет скрипт в отдельном потоке"""
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                encoding='utf-8',
                errors='replace'
            )
            
            for line in process.stdout:
                self.root.after(0, self._log_line, line.strip())
            
            process.wait()
            
            if process.returncode == 0:
                self.root.after(0, lambda: self.log(f"\nСкрипт завершён успешно!\n", "SUCCESS"))
                self.root.after(0, lambda: self.status_bar.config(text="Готов к работе"))
            else:
                self.root.after(0, lambda: self.log(f"\nОшибка выполнения (код {process.returncode})\n", "ERROR"))
                self.root.after(0, lambda: self.status_bar.config(text="Ошибка выполнения"))
            
        except Exception as e:
            self.root.after(0, lambda: self.log(f"Исключение: {e}", "ERROR"))
            self.root.after(0, lambda: self.status_bar.config(text="Исключение"))
        
        finally:
            self.running_scripts[script_name] = False
            if btn:
                self.root.after(0, lambda: btn.config(state='normal'))
    
    def _log_line(self, line):
        """Добавляет строку в лог"""
        if not line:
            return
        
        if 'SUCCESS' in line.upper() or 'успешно' in line.lower() or '✅' in line:
            tag = 'SUCCESS'
        elif 'ERROR' in line.upper() or 'ошибка' in line.lower() or '❌' in line:
            tag = 'ERROR'
        elif 'WARNING' in line.upper() or 'предупреждение' in line.lower() or '⚠️' in line:
            tag = 'WARNING'
        elif 'INFO' in line.upper() or '📊' in line or '🎯' in line:
            tag = 'INFO'
        else:
            tag = 'SYSTEM'
        
        self.log_text.insert(tk.END, line + '\n', tag)
        self.log_text.see(tk.END)
    
    def log(self, message, level='INFO'):
        """Добавляет сообщение в лог"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        full_message = f"[{timestamp}] {message}"
        
        if level == 'SUCCESS':
            tag = 'SUCCESS'
        elif level == 'ERROR':
            tag = 'ERROR'
        elif level == 'WARNING':
            tag = 'WARNING'
        elif level == 'SYSTEM':
            tag = 'SYSTEM'
        else:
            tag = 'INFO'
        
        self.log_text.insert(tk.END, full_message + '\n', tag)
        self.log_text.see(tk.END)
    
    def open_db_viewer(self):
        """Открывает pgAdmin"""
        self.log("Открытие pgAdmin 4...", "SYSTEM")
        try:
            os.startfile("pgAdmin 4")
        except:
            messagebox.showinfo("Инфо", "pgAdmin 4 не найден в PATH.\nОткройте его вручную.")
    
    def clear_logs(self):
        """Очищает логи"""
        if messagebox.askyesno("Подтверждение", "Очистить панель логов?"):
            self.log_text.delete(1.0, tk.END)
            self.log("Логи очищены", "SYSTEM")
    
    def save_log(self):
        """Сохраняет лог в файл"""
        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=f"project_zzz_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(self.log_text.get(1.0, tk.END))
                self.log(f"Лог сохранён: {file_path}", "SUCCESS")
            except Exception as e:
                self.log(f"Ошибка сохранения: {e}", "ERROR")
    
    def sync_github(self):
        """Синхронизация с GitHub"""
        self.log("Синхронизация с GitHub...", "SYSTEM")
        
        try:
            self.log("Git pull...", "INFO")
            result = subprocess.run(
                ['git', 'pull'], 
                cwd=PROJECT_ROOT, 
                capture_output=True, 
                text=True, 
                encoding='utf-8', 
                errors='replace'
            )
            if result.returncode == 0:
                self.log(result.stdout, "INFO")
            else:
                self.log(result.stderr, "ERROR")
            
            if messagebox.askyesno("GitHub", "Отправить изменения на GitHub?"):
                self.log("Git add...", "INFO")
                subprocess.run(['git', 'add', '.'], cwd=PROJECT_ROOT)
                
                commit_msg = f"Auto-commit {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                self.log("Git commit...", "INFO")
                subprocess.run(['git', 'commit', '-m', commit_msg], cwd=PROJECT_ROOT)
                
                self.log("Git push...", "INFO")
                result = subprocess.run(
                    ['git', 'push'], 
                    cwd=PROJECT_ROOT,
                    capture_output=True, 
                    text=True, 
                    encoding='utf-8', 
                    errors='replace'
                )
                if result.returncode == 0:
                    self.log("GitHub sync completed!", "SUCCESS")
                else:
                    self.log(result.stderr, "ERROR")
        
        except Exception as e:
            self.log(f"Ошибка Git: {e}", "ERROR")
            messagebox.showerror("Git Error", f"Ошибка синхронизации:\n{e}")


# ==============================================================================
# ЗАПУСК ПРИЛОЖЕНИЯ
# ==============================================================================
def main():
    root = tk.Tk()
    
    # Иконка (если есть)
    icon_path = PROJECT_ROOT / "docs" / "icon.ico"
    if icon_path.exists():
        root.iconbitmap(str(icon_path))
    
    app = ProjectZZZControlPanel(root)
    root.mainloop()


if __name__ == "__main__":
    main()