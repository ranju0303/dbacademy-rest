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

def find_dbacademy_packages():
    packages = find_packages(where="src")
    if "dbacademy" in packages:
        del packages[packages.index("dbacademy")]
    # print("-"*80)
    # print(packages)
    # print("-"*80)
    return packages


setuptools.setup(
    name="dbacademy-rest",
    version="0.1",
    install_requires=reqs,
    package_dir={"dbacademy": "src/dbacademy"},
    packages=find_dbacademy_packages()
)
