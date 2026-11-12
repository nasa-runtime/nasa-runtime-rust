"""确认源码工作区不会混入同名组件的 registry 实现。"""

import argparse
import json
from pathlib import Path
import subprocess


def verify(metadata):
    """业务作用：要求工作区成员之间共享当前源码身份，避免同名类型来自不同实现。
    参数说明：
    - metadata：Cargo 对完整 feature 图解析得到的包与成员信息。
    返回：全部同名依赖来自当前成员时通过；registry 副本或重复来源会抛出错误。
    """
    members = set(metadata["workspace_members"])
    packages = metadata["packages"]
    names = {package["name"] for package in packages if package["id"] in members}
    conflicts = [
        package["id"]
        for package in packages
        if package["name"] in names and package["id"] not in members
    ]
    if conflicts:
        raise ValueError(
            "workspace components resolve to multiple sources: " + ", ".join(conflicts)
        )
    print("Workspace component sources verified:", len(members))


def main():
    """业务作用：以锁文件核对本地工作区完整依赖图，不修改版本或访问发布入口。
    参数说明: 无。
    返回：依赖解析及来源一致性均满足后退出；锁文件漂移或来源冲突时失败。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--all-features",
        "--format-version",
        "1",
    ]
    if args.offline:
        command.append("--offline")
    root = Path(__file__).resolve().parents[2]
    verify(json.loads(subprocess.check_output(command, cwd=root)))


if __name__ == "__main__":
    main()
