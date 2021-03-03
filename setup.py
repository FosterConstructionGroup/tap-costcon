#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-costcon",
    version="1.0.0",
    description="Singer.io tap for Costcon exports",
    author="Sam Woolerton",
    url="https://samwoolerton.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_costcon"],
    install_requires=["singer-python==5.9.0", "pytz==2018.4",],
    extras_require={"dev": ["pylint", "ipdb", "nose",]},
    entry_points="""
          [console_scripts]
          tap-costcon=tap_costcon:main
      """,
    packages=["tap_costcon"],
    package_data={"tap_costcon": ["tap_costcon/schemas/*.json"]},
    include_package_data=True,
)
