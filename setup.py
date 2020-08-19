from setuptools import setup, find_packages

install_requires = []

setup(
    name="ontodev-gizmos",
    description="Gizmos for ontology development",
    version="0.0.1",
    author="James A Overton",
    author_email="james@overton.ca",
    install_requires=install_requires,
    packages=find_packages(exclude="tests"),
)
