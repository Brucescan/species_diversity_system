import subprocess
import sys
import time
from multiprocessing import Process, Queue
import os
import schedule

# 强制设置工作目录为 data_pipeline/
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from fetch_aqi import FetchAQI
from fetch_bird import FetchBird
from save_to_database import consumer



def run_update_script(script_name):
    """Executes a python script in a subprocess."""
    print(f"运行更新脚本{script_name}")
    try:
        # Use sys.executable to ensure using the same python environment
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,  # Raise exception if script returns non-zero exit code
            capture_output=True,  # Capture stdout and stderr
            text=True,  # 解码输出文本
            encoding='utf-8',
            errors='replace',
        )
        print(f"成功运行{script_name}脚本.")
        if result.stdout:
            print("Output:\n", result.stdout)
        if result.stderr:
            print("Error Output:\n", result.stderr)  # Should be empty if check=True and no error
    except FileNotFoundError:
        print(f"Error: Script '{script_name}' not found in {os.getcwd()}")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: Script returned non-zero exit code {e.returncode}")
        if e.stdout:
            print("Output:\n", e.stdout)
        if e.stderr:
            print("Error Output:\n", e.stderr)
    except Exception as e:
        print(f"An unexpected error occurred while running {script_name}: {e}")
    print(f"--- Finished running update script: {script_name} ---")


if __name__ == '__main__':
    q = Queue()
    fetch_aqi = FetchAQI()
    fetch_bird = FetchBird()
    p1 = Process(target=fetch_aqi.get_data, args=(q,))
    p2 = Process(target=fetch_bird.get_all_data, args=(q,))
    # 启动第一个生产者进程
    p1.start()
    # 启动第二个生产者进程
    p2.start()

    # 启动消费者
    c = Process(target=consumer, args=(q,))
    c.start()
    # 等待所有的进程完成
    p1.join()
    print("AQI fetch process joined.")
    p2.join()
    print("Bird fetch process joined.")
    c.join()
    print("Consumer process joined.")

    print("Phase 1: 初始化数据抓取成功并且保存到数据库中.")
    print("-" * 30)

    # Schedule the jobs
    schedule.every().hour.do(run_update_script,"update_aqi.py").tag('aqi-update')
    # Example: Run daily at 2:30 AM. Adjust time as needed.
    schedule.every().day.at("02:30").do(run_update_script,"update_bird.py").tag('bird-update')

    # Print upcoming jobs
    print("Scheduled jobs:")
    for job in schedule.get_jobs():
        print(f"- {job}")

    # Run the scheduler loop
    print("Phase 2: 周期运行中. 等待调度工作...")
    print("-" * 30)
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every 60 seconds if a job is due


    


