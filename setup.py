import setuptools
from setuptools import find_packages

reqs = [
    "setuptools",
    "requests",
    "urllib3",
    "overrides",
    "Deprecated",
    'dbacademy-gems@git+https://github.com/databricks-academy/dbacademy-gems',
]

setuptools.setup(
    name="dbacademy-rest",
    version="0.1",
    install_requires=reqs,
    package_dir={"dbacademy": "src/dbacademy"},
    packages=find_packages("src")
)
