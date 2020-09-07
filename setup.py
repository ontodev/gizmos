from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(here + "/README.md", "r") as fh:
    long_description = fh.read()

install_requires = []

test_requires = ["html5lib", "pyRdfa3", "pytest", "rdflib"]

setup(
    name="ontodev-gizmos",
    description="Gizmos for ontology development",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="0.1.1",
    author="James A Overton",
    author_email="james@overton.ca",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: BSD License",
    ],
    install_requires=install_requires,
    setup_requires=["pytest-runner"],
    tests_require=test_requires,
    test_suite="pytest",
    packages=find_packages(exclude="tests"),
)
