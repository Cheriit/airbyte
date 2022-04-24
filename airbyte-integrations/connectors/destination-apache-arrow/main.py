#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from destination_apache_arrow import DestinationApacheArrow

if __name__ == "__main__":
    DestinationApacheArrow().run(sys.argv[1:])
