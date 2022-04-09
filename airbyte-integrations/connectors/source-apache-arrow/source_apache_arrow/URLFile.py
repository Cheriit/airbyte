from urllib.parse import urlparse
import smart_open
from airbyte_cdk.entrypoint import logger


class URLFile:

    def __init__(self, url: str, provider: dict):
        self._url = url
        self._provider = provider
        self._file = None

    def __enter__(self):
        return self._file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def full_url(self):
        return f"{self.storage_scheme}{self.url}"

    def close(self):
        if self._file:
            self._file.close()
            self._file = None

    def open(self, binary=False):
        self.close()
        self._file = self._open(binary=binary)
        return self

    def _open(self, binary):
        mode = "rb" if binary else "r"
        storage = self.storage_scheme
        url = self.url

        return smart_open.open(self.full_url, mode=mode)

    @property
    def url(self) -> str:
        """Convert URL to remove the URL prefix (scheme)
        :return: the corresponding URL without URL prefix / scheme
        """
        parse_result = urlparse(self._url)
        if parse_result.scheme:
            return self._url.split("://")[-1]
        else:
            return self._url

    @property
    def storage_scheme(self) -> str:
        """Convert Storage Names to the proper URL Prefix
        :return: the corresponding URL prefix / scheme
        """
        storage_name = self._provider["storage"].upper()
        parse_result = urlparse(self._url)

        if storage_name == "LOCAL":
            return "file://"

        logger.error(f"Unknown Storage provider in: {self.full_url}")
        return ""
