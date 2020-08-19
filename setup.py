from setuptools import setup, find_packages

install_requires = []

test_requires = ["html5lib", "pyRdfa3", "pytest", "rdflib"]

setup(
    name="ontodev-gizmos",
    description="Gizmos for ontology development",
    version="0.0.1",
    author="James A Overton",
    author_email="james@overton.ca",
    install_requires=install_requires,
    setup_requires=["pytest-runner"],
    tests_require=test_requires,
    test_suite="pytest",
    packages=find_packages(exclude="tests"),
)
