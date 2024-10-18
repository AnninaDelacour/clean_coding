from setuptools import find_packages, setup

setup(
    name="elt_processing",
    packages=find_packages(exclude=["elt_processing_tests"]),
    install_requires=[
        "dagster",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
