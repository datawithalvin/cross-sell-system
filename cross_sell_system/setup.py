from setuptools import find_packages, setup

setup(
    name="cross_sell_system",
    packages=find_packages(exclude=["cross_sell_system_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
