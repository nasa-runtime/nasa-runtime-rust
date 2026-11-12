"""按发布顺序核对工作区组件的 registry 依赖与本地源码身份。"""

import argparse
import json
from pathlib import Path
import subprocess

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


def registry_versions(root, packages):
    """业务作用：只允许完整前置批次解除源码覆盖，固定线上组件的版本边界。
    参数说明：
    - root：包含发布顺序与组件来源映射的仓库根目录。
    - packages：需要核对来源的工作区成员。
    返回：已解除覆盖的前置组件及其版本；批次不完整、顺序异常或源码路径不符时拒绝。
    """
    mapping = tomllib.loads((root / "Cargo.toml").read_text())["patch"]["crates-io"]
    published = {}
    for package in packages:
        name = package["name"]
        if name not in mapping:
            published[name] = package["version"]
            continue
        path = (root / mapping[name]["path"]).resolve()
        path.relative_to(root)
        if path / "Cargo.toml" != Path(package["manifest_path"]).resolve():
            raise ValueError("workspace source differs: " + name)
    plan = subprocess.check_output(
        [str(root / ".github/scripts/release-crates.sh"), "--plan"], text=True
    )
    prefixes = [set()]
    for line in plan.splitlines():
        _, _, names = line.split("\t")
        prefixes.append(prefixes[-1] | set(names.split()))
    # 只能按完整批次依次采用线上依赖，不能以零散删除 patch 绕过未发布组件的来源约束。
    if set(published) not in prefixes:
        raise ValueError("registry components must form complete preceding batches")
    return published


def verify(metadata, published=None):
    """业务作用：让已发布依赖统一使用锁定的线上实现，其余组件统一使用当前源码。
    参数说明：
    - metadata：Cargo 对完整 feature 图解析得到的包与成员信息。
    - published：允许作为依赖解析的线上组件及精确版本；为空时所有组件仍要求本地来源。
    返回：所有组件的来源与发布阶段一致时通过；版本漂移、非 crates.io 来源或本地回指均拒绝。
    """
    published = published or {}
    members = set(metadata["workspace_members"])
    packages = metadata["packages"]
    names = {package["name"] for package in packages if package["id"] in members}
    conflicts = [
        package["id"]
        for package in packages
        if package["name"] in names and package["id"] not in members
        and (
            published.get(package["name"]) != package["version"]
            or package.get("source") != "registry+https://github.com/rust-lang/crates.io-index"
        )
    ]
    if conflicts:
        raise ValueError(
            "workspace components resolve to multiple sources: " + ", ".join(conflicts)
        )
    local_published = {
        package["id"] for package in packages
        if package["id"] in members and package["name"] in published
    }
    # 工作区仍保留已发布组件的源码入口，但其它组件不得再借依赖边回指这些本地副本。
    for node in metadata.get("resolve", {}).get("nodes", []):
        if local_published.intersection(node["dependencies"]):
            raise ValueError("published dependency still resolves locally: " + node["id"])
    print("Workspace component sources verified:", len(members), "registry dependencies:", len(published))


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
    metadata = json.loads(subprocess.check_output(command, cwd=root))
    members = set(metadata["workspace_members"])
    packages = [p for p in metadata["packages"] if p["id"] in members]
    verify(metadata, registry_versions(root, packages))


if __name__ == "__main__":
    main()
