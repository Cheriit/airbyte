#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.1",
    "pandas==1.3.4",
    "pyarrow",
    "numpy",
    "smart-open[all]==4.1.2",
    "genson==1.2.2",

]

TEST_REQUIREMENTS = [
    "pytest~=6.1",
    "source-acceptance-test",
]

setup(
    name="source_apache_arrow",
    description="Source implementation for Apache Arrow.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)


MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.1",
]

TEST_REQUIREMENTS = [
    "pytest~=6.1",
    "source-acceptance-test",
]

