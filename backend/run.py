import subprocess
import sys
import time


def run_command(command, cwd=None):
    """运行指定的命令并打印其输出"""
    print(f"Running command: {command}")
    process = subprocess.Popen(
        command,
        shell=True,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
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


if __name__ == "__main__":
    # 第一个命令：运行 pipeline
    # run_command("python manage.py run_pipeline")

    # 等待一下确保 pipeline 完成
    time.sleep(1)

    # 第二个命令：启动开发服务器
    run_command("python manage.py runserver")
