from setuptools import setup, find_namespace_packages

setup(  # pragma: no cover
    # ...
    packages=find_namespace_packages(
        where="src",
    ),
    package_dir={"": "src"},
)
