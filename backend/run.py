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
    run_command("python data_pipeline/run_pipeline.py")


def run_server():
    """运行开发服务器"""
    run_command("python manage.py runserver")


if __name__ == "__main__":
    # 使用多进程分别运行pipeline和server
    pipeline_process = multiprocessing.Process(target=run_pipeline)
    server_process = multiprocessing.Process(target=run_server)

    # 启动进程
    pipeline_process.start()
    time.sleep(5)  # 给pipeline一些初始化时间
    server_process.start()

    # 等待进程结束(正常情况下server_process不会结束)
    pipeline_process.join()
    server_process.join()