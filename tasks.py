from __future__ import annotations

import os
import shutil
from shutil import copytree, rmtree

import tomli
from invoke import task

try:
    with open("metron/plugin.toml", "rb") as f:
        plugin_info = tomli.load(f)
        plugin_name = plugin_info["plugin"]["details"]["name"]
        plugin_version = plugin_info["plugin"]["details"]["version"]
except Exception as e:
    plugin_name = "metron-talker-plugin"
    plugin_version = ""
    print(f"Failed to load plugin.toml: {e}")  # noqa: T201

# For install TODO test
"""if sys.platform == "win32":
    INSTALL_DIR = os.environ["localappdata"] + "\\ComicTagger\\plugins"  # Not correct?
elif sys.platform == "darwin":
    INSTALL_DIR = os.path.realpath(os.path.expanduser("~/Library/Application Support/ComicTagger/plugins"))
elif sys.platform == "linux":
    INSTALL_DIR = os.path.realpath("/usr/local/share/comictagger/plugins")"""


@task
def build(c, output="build", ziparchive=None):
    output = os.path.join(output, plugin_name)
    if os.path.exists(output):
        rmtree(output)

    # Install requirements in specified dir
    args = ["pip", "install", "-r", "requirements.txt", f'--target "{output}"', "--no-compile"]
    c.run(" ".join(args), echo=True)

    copytree("metron", output, dirs_exist_ok=True)

    if ziparchive is not None:
        shutil.make_archive(ziparchive, "zip", "build", plugin_name)


@task
def test(c):
    c.run("pytest")


@task
def install(c):
    """TODO"""
    # build(c, output=INSTALL_DIR)


@task
def pack(c):
    zipname = plugin_name + "-" + plugin_version
    build(c, ziparchive=zipname)
    rmtree(os.path.join("build", plugin_name))


@task
def clean(c):
    root = "build"
    rmtree(os.path.join(root, plugin_name))
