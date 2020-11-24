#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = ["asyncio", "aioredis", "websockets","tenacity"]

classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Financial and Insurance Industry",
    "Topic :: Office/Business :: Financial",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
]


setup(
    name="okws",
    version="0.1.1",
    description="通过 redis 提供 okex websocket 服务数据",
    url="https://github.com/oscnet/okws",
    author="oscnet",
    author_email="oscnet@163.com",
    license="MIT",
    classifiers=classifiers,
    keywords="exchange websockets api",
    packages=find_packages(exclude=["tests"]),
    install_requires=install_requires,
    tests_require=['pytest','pytest-asyncio'],
    test_suite='tests',
    # include_package_data=True,
)
