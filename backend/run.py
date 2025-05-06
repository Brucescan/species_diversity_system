import subprocess
import sys
import time
import multiprocessing


def run_command(command, cwd=None):
    """运行指定的命令并打印其输出"""
    print(f"Running command: {command}")
    process = subprocess.Popen(
        command,
        shell=True,
        cwd=cwd,
        stdout=subprocess.PIPE,
        encoding='utf-8',  # 强制使用 UTF-8 编码
        stderr=subprocess.STDOUT,  # 将 stderr 合并到 stdout
        universal_newlines=True,
    )

    # 实时打印输出
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())

    # 检查返回值
    return_code = process.poll()
    if return_code != 0:
        print(f"Command failed with return code {return_code}")
        sys.exit(return_code)


def run_pipeline():
    """运行pipeline命令"""
    try:
        run_command("python data_pipeline/run_pipeline.py")
    except Exception as e:
        print(f"run_pipeline 出错: {e}", flush=True)  # 捕获所有异常


def run_server():
    """运行开发服务器"""
    run_command("python manage.py runserver")

if __name__ == "__main__":
    pipeline_process = multiprocessing.Process(target=run_pipeline)
    server_process = multiprocessing.Process(target=run_server)

    pipeline_process.start()

    time.sleep(5)
    server_process.start()

    pipeline_process.join()
    server_process.join()
