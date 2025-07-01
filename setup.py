from setuptools import find_packages, setup

setup(
    name="dagster_london_walks",
    packages=find_packages(exclude=["dagster_london_walks_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
