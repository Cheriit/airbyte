#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_apache_arrow import SourceApacheArrow

if __name__ == "__main__":
    source = SourceApacheArrow()
    launch(source, sys.argv[1:])
