from setuptools import find_packages, setup

REQUIRES = ["PyInquirer",
            "prettytable"]

setup(
    name="dht",
    version="1.0.0",
    description="Distributed Hash Table Implementation",
    packages=find_packages(),
    install_requires=REQUIRES,
    python_requires=">=3.6"
)