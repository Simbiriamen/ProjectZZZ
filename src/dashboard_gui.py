# -*- coding: utf-8 -*-
"""
dashboard_gui.py v2.1
🔧 ИСПРАВЛЕНИЯ:
  1. Обработка таймаута subprocess
  2. Корректное завершение потоков (thread.join())
  3. Логирование ошибок
"""
import tkinter as tk
from tkinter import scrolledtext, messagebox
import subprocess
import threading
import os
import datetime

# Путь к проекту
PROJECT_PATH = r"D:\ProjectZZZ"
SCRIPT_TIMEOUT = 3600  # 60 минут


class ProjectDashboard:
    def __init__(self, root):
        self.root = root
        self.root.title("ProjectZZZ - Панель Управления")
        self.root.geometry("800x600")

        # Заголовок
        self.label_title = tk.Label(root, text="ProjectZZZ Control Panel", font=("Arial", 16, "bold"))
        self.label_title.pack(pady=10)

        # Фрейм для кнопок
        self.frame_buttons = tk.Frame(root)
        self.frame_buttons.pack(pady=10)

        # Кнопки действий
        self.btn_check_db = tk.Button(self.frame_buttons, text="Проверить БД", command=lambda: self.run_script("python src/check_db.py"), width=20)
        self.btn_check_db.grid(row=0, column=0, padx=5, pady=5)

        self.btn_load_data = tk.Button(self.frame_buttons, text="Загрузить Данные (ETL)", command=lambda: self.run_script("python src/load_sales.py"), width=20)
        self.btn_load_data.grid(row=0, column=1, padx=5, pady=5)

        self.btn_generate = tk.Button(self.frame_buttons, text="Генерация Рекомендаций", command=lambda: self.run_script("python src/generate_recommendations.py"), width=20)
        self.btn_generate.grid(row=1, column=0, padx=5, pady=5)

        self.btn_sync_git = tk.Button(self.frame_buttons, text="Sync GitHub", command=self.sync_github, width=20, bg="#ddddff")
        self.btn_sync_git.grid(row=1, column=1, padx=5, pady=5)

        # Поле логов
        self.label_logs = tk.Label(root, text="Журнал выполнения:", anchor="w")
        self.label_logs.pack(padx=10, pady=(10, 0))

        self.text_logs = scrolledtext.ScrolledText(root, height=20, state='disabled')
        self.text_logs.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

        # Статус бар
        self.status_var = tk.StringVar()
        self.status_var.set("Готов к работе")
        self.label_status = tk.Label(root, textvariable=self.status_var, bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.label_status.pack(side=tk.BOTTOM, fill=tk.X)

        # 🔧 Хранение ссылок на потоки
        self.running_threads = {}

        self.log_message("Система запущена. Ожидание команд.")

    def log_message(self, message):
        """Добавление сообщения в окно логов"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"[{timestamp}] {message}\n"
        self.text_logs.config(state='normal')
        self.text_logs.insert(tk.END, full_message)
        self.text_logs.see(tk.END)
        self.text_logs.config(state='disabled')

    def run_script(self, command):
        """Запуск скрипта в отдельном потоке с таймаутом"""
        self.status_var.set("Выполнение...")
        self.log_message(f"Запуск: {command}")

        def target():
            process = None
            try:
                # Запуск команды в папке проекта
                process = subprocess.Popen(
                    command,
                    shell=True,
                    cwd=PROJECT_PATH,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

                try:
                    # 🔧 Ожидание с таймаутом
                    stdout, stderr = process.communicate(timeout=SCRIPT_TIMEOUT)

                    if process.returncode == 0:
                        self.log_message("Успешно завершено.")
                        self.status_var.set("Готов к работе")
                    else:
                        self.log_message(f"Ошибка выполнения: {stderr}")
                        self.status_var.set("Ошибка выполнения скрипта")
                        self.root.after(0, lambda: messagebox.showerror("Ошибка", f"Скрипт вернул ошибку:\n{stderr}"))

                except subprocess.TimeoutExpired:
                    # 🔧 Принудительное завершение при таймауте
                    process.kill()
                    process.wait()
                    self.log_message(f"ТАЙМАУТ ({SCRIPT_TIMEOUT} сек) - процесс завершён принудительно")
                    self.status_var.set("Таймаут выполнения")
                    self.root.after(0, lambda: messagebox.showerror("Таймаут", f"Скрипт не завершился за {SCRIPT_TIMEOUT} сек"))

            except Exception as e:
                self.log_message(f"Критическая ошибка: {str(e)}")
                self.status_var.set("Критическая ошибка")
                self.root.after(0, lambda: messagebox.showerror("Ошибка", str(e)))
            
            finally:
                # 🔧 Освобождение ресурсов
                if process and process.poll() is None:
                    process.kill()

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        
        # 🔧 Сохранение ссылки на поток для контроля
        self.running_threads[command] = thread

    def sync_github(self):
        """Синхронизация с GitHub"""
        self.log_message("Начало синхронизации с GitHub...")
        # Команды git
        commands = [
            "git add .",
            "git commit -m 'Auto commit from Dashboard'",
            "git push origin main"
        ]
        # Для упрощения просто делаем pull и push
        self.run_script("git pull && git push")

    def wait_all_threads(self, timeout=10):
        """🔧 Ожидание завершения всех потоков при закрытии"""
        for cmd, thread in self.running_threads.items():
            if thread.is_alive():
                thread.join(timeout=timeout)
        self.running_threads.clear()

    def on_closing(self):
        """🔧 Обработчик закрытия окна"""
        if self.running_threads:
            if messagebox.askokcancel("Выход", "Есть выполняющиеся процессы. Завершить?"):
                self.wait_all_threads(timeout=5)
                self.root.destroy()
        else:
            self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    root.protocol("WM_DELETE_WINDOW", ProjectDashboard.on_closing)  # 🔧 Перехват закрытия
    app = ProjectDashboard(root)
    root.mainloop()