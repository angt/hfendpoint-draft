import sys
import stat
import urllib.request
import gzip
import shutil
import platform
from pathlib import Path
from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install_scripts import install_scripts

NAME = "hfendpoint"
VERSION = "0.2.0"
URL = f"https://github.com/angt/hfendpoint-draft/releases/download/v{VERSION}"

def get_binary_url():
    system = platform.system()
    if system == "Linux":
        system = "linux"
    elif system == "Darwin":
        system = "macos"
    else:
        sys.exit(f"Unsupported system: {system}")

    machine = platform.machine()
    if machine == "aarch64" or machine == "arm64":
        machine = "aarch64"
    elif machine == "x86_64" or machine == "amd64":
        machine = "x86_64"
    else:
        sys.exit(f"Unsupported machine: {machine}")

    return f"{NAME}-{machine}-{system}.gz"

class CustomInstallCommands(install_scripts):
    def run(self):
        self.mkpath(self.install_dir)
        gz_file = get_binary_url()
        gz_path = Path(self.install_dir) / gz_file
        bin_path = Path(self.install_dir) / NAME

        gz_url = f"{URL}/{gz_file}"
        print(f"Downloading {gz_url}...")
        urllib.request.urlretrieve(gz_url, gz_path)

        print(f"Installing to {bin_path}...")
        with gzip.open(gz_path, "rb") as f_in:
            with open(bin_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        gz_path.unlink()
        bin_path.chmod(bin_path.stat().st_mode | stat.S_IEXEC)

        super().run()

setup(
    name="hfendpoint",
    version=VERSION,
    description="hfendpoint lib",
    author="angt",
    author_email="angt@hf.co",
    python_requires=">=3.12",
    install_requires=["msgpack"],
    py_modules=["hfendpoint"],
    cmdclass={
        "develop": develop,
        "install_scripts": CustomInstallCommands,
    },
    platforms=[
        "Linux_x86_64",
        "Linux_aarch64",
        "macOS_x86_64",
        "macOS_aarch64",
    ],
)
