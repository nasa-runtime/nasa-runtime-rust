"""以当前配套组件源码生成并编译工作区归档。"""

import argparse
import json
from pathlib import Path
import subprocess
import tarfile

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


def source_arguments(root, packages):
    """业务作用：为仍采用源码装配的组件保留覆盖，已发布依赖继续使用 registry。
    参数说明：
    - root：拥有统一依赖映射的工作区根目录。
    - packages：Cargo 解析出的工作区成员元数据。
    返回：现有映射对应的外部 patch 配置；已解除覆盖的成员不再补回，路径越界或身份不符时拒绝。
    """
    mapping = tomllib.loads((root / "Cargo.toml").read_text())["patch"]["crates-io"]
    arguments = []
    for package in packages:
        name = package["name"]
        if name not in mapping:
            continue
        path = (root / mapping[name]["path"]).resolve()
        path.relative_to(root)
        if path / "Cargo.toml" != Path(package["manifest_path"]).resolve():
            raise ValueError("workspace source differs: " + name)
        arguments += [
            "--config",
            "patch.crates-io." + name + ".path=" + json.dumps(str(path)),
        ]
    return arguments


def inspect_archive(package, target):
    """业务作用：核对实际 crate 的源码、公开元数据、说明、许可正文与名称声明。
    参数说明：
    - package：声明版本、许可证与文件位置的成员元数据。
    - target：Cargo 实际写入归档的构建目录。
    返回：必要公开资源与源码一致时成功；本地内容泄漏或清单不一致时失败。
    """
    source = Path(package["manifest_path"]).parent
    filename = package["name"] + "-" + package["version"] + ".crate"
    with tarfile.open(target / "package" / filename) as archive:
        contents = {}
        for member in archive.getmembers():
            relative = Path(*Path(member.name).parts[1:])
            if {"tests", "target", ".git", "__pycache__"}.intersection(relative.parts):
                raise ValueError("non-product archive entry: " + member.name)
            if not member.isfile() or str(relative) in contents:
                raise ValueError("unique ordinary file required: " + member.name)
            contents[str(relative)] = archive.extractfile(member).read()
    normalized = tomllib.loads(contents["Cargo.toml"].decode())["package"]
    for key in ("name", "version", "license", "description"):
        if normalized.get(key) != package[key]:
            raise ValueError("archive metadata differs: " + filename + ":" + key)
    licenses = {
        "MIT OR Apache-2.0": ("LICENSE-MIT", "LICENSE-APACHE"),
        "Apache-2.0": ("LICENSE",),
    }
    readme = Path(package["readme"])
    if readme.is_absolute():
        readme = readme.relative_to(source)
    for name in (str(readme), "NOTICE", *licenses[package["license"]]):
        if contents[name] != (source / name).read_bytes():
            raise ValueError("archive resource differs: " + filename + ":" + name)
    if normalized["readme"] != str(readme):
        raise ValueError("archive README differs: " + filename)
    # Cargo 生成的依赖锁与规范化清单单独处理，其余内容必须对应当前公开源码。
    for name, content in contents.items():
        if name in {"Cargo.toml", "Cargo.lock", ".cargo_vcs_info.json"}:
            continue
        path = source / ("Cargo.toml" if name == "Cargo.toml.orig" else name)
        if not path.is_file() or content != path.read_bytes():
            raise ValueError("archive source differs: " + filename + ":" + name)
    print("Paired-source archive verified:", filename, "files:", len(contents))


def main():
    """业务作用：逐包形成可编译归档，保留已发布依赖的 registry 校验和身份。
    参数说明: 无。
    返回：全部选定成员归档编译及资源核对成功后退出；任一失败立即停止。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-dir", type=Path, default=Path("target"))
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--package", action="append")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    target = args.target_dir.resolve()
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"],
            cwd=root,
        )
    )
    members = set(metadata["workspace_members"])
    packages = [p for p in metadata["packages"] if p["id"] in members]
    command = [
        "cargo",
        "package",
        "--target-dir",
        str(target),
        "--all-features",
        "--locked",
        "--allow-dirty",
        *source_arguments(root, packages),
    ]
    if args.offline:
        command.append("--offline")
    selected = packages
    if args.package:
        requested = set(args.package)
        if requested - {p["name"] for p in packages}:
            raise ValueError("unknown workspace package selection")
        selected = [p for p in packages if p["name"] in requested]
    # 整个工作区一起打包会用临时归档替代同名线上组件；VCS 信息变化即可造成校验和冲突。
    # 逐包验证保留线上前置依赖，未发布组件仍由显式 patch 装配，不把临时归档伪装成 registry 内容。
    for package in selected:
        print("Packaging workspace member:", package["name"], flush=True)
        subprocess.run([*command, "-p", package["name"]], cwd=root, check=True)
        inspect_archive(package, target)
    print("Registry-only dependency availability is not established by this build.")


if __name__ == "__main__":
    main()
