#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["airbyte-cdk", "pyarrow", "genson==1.2.2", "pandas==1.3.4", "click==8.0.1"]

TEST_REQUIREMENTS = ["pytest~=6.1"]

setup(
    name="destination_apache_arrow",
    description="Destination implementation for Apache Arrow.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
