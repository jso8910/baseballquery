#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
from glob import glob
from setuptools import find_packages, setup  # type: ignore


def get_version(package: str):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    init_py = open(os.path.join(package, "__init__.py")).read()
    res = re.search("__version__ = ['\"]([^'\"]+)['\"]", init_py)
    if res:
        return res.group(1)
    else:
        raise ValueError("Could not find __version__ in __init__.py")


version = get_version("baseballquery")


with open("README.md") as readme_file:
    readme = readme_file.read()

# with open('HISTORY.md') as history_file:
#     history = history_file.read()

requirements = ["requests", "tqdm", "pandas", "typing-extensions", "numpy", "pyarrow", "sqlalchemy", "msgspec", "aiohttp[speedups]"]

test_requirements = []

_ = setup(
    name="baseballquery",
    version=version,
    description="A library to query MLB stats including on a game level",
    long_description_content_type="text/markdown",
    long_description=readme,  # + '\n\n' + history,
    author="Jason R",
    author_email="mail4jasonr@gmail.com",
    url="https://github.com/jso8910/baseballquery",
    packages=find_packages(
        exclude=[
            "tests",
            "tests.*",
            "baseballquery/chadwick",
            "baseballquery/chadwick.hdf5",
            "baseballquery/downloads",
        ]
    ),
    package_dir={
        "package": "package",
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT",
    zip_safe=False,
    keywords="python",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.12",
    ],
    test_suite="tests",
    tests_require=test_requirements,
    python_requires=">=3.9",
)
