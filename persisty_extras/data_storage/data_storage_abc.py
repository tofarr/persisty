from abc import ABC, abstractmethod
from typing import Optional, Iterator, Tuple

from servey.cache_control.cache_control_abc import CacheControlABC

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.data_storage.data_item import DataItem
from persisty.data_storage.data_meta import DataMeta, DataMetaFilter
from persisty.storage.result_set import ResultSet


class DataStorageABC(ABC):
    """
    Storage for binary data rather than objects. Typically backed by a directory or S3
    WE SHOULD BE ABLE TO SPECIFY A MAX SIZE FOR OBJECTS!
    API SHOULD BE ABLE TO TRANSFER A RANGE OF AN OBJECT IN A BUFFER.
    BUFFERS SHOULD HAVE A MAX SIZE
    """

    @abstractmethod
    def get_name(self) -> str:
        """ Get the name of this storage """

    @abstractmethod
    def get_access_control(self) -> AccessControlABC:
        """ Get the access control for this storage """

    @abstractmethod
    def get_cache_control(self) -> CacheControlABC:
        """ Get the cache control for this storage """

    @abstractmethod
    def get_max_chunk_size(self) -> int:
        """ Get the maximum buffer size for multipart upload / downloads """

    @abstractmethod
    def get_max_upload_age(self) -> int:
        """
        Get the maximum time for multipart uploads. At the end of this period, if the upload has not been
        completed it will be automatically abandoned
        """

    @abstractmethod
    def url_for_create(self, key: Optional[str] = None) -> str:
        """ Create a url which will be the target for multipart uploads. """

    @abstractmethod
    def begin_upload(self, key: Optional[str] = None) -> str:
        """ Begin a new multipart upload. Return the key for the upload. """

    @abstractmethod
    def upload_chunk(self, upload_id: str, part_number: int, data: bytes):
        """ Upload part of a data stream. """

    @abstractmethod
    def complete_upload(self, upload_id, part_number: Optional[int] = None, data: Optional[bytes] = None) -> str:
        """ Finish uploading part of a data stream, and return the key for the upload """

    @abstractmethod
    def abort_upload(self, upload_id: str) -> bool:
        """ Abort an upload. """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """ Delete a resource """

    @abstractmethod
    def url_for_read(self, key: Optional[str] = None) -> str:
        """ Create a Url for downloads """

    @abstractmethod
    def read_meta(self, key: str) -> DataMeta:
        """ Read meta for a key """

    @abstractmethod
    def read(self, key: str, data_range: Optional[Tuple[int, int]] = None) -> DataItem:
        """ Read data / a range of data for a key """

    def search_all(self, search_filter: Optional[DataMetaFilter] = None) -> Iterator[DataMeta]:
        """
        Search the data storage and return all results
        """
        kwargs = {'search_filter': search_filter}
        while True:
            result_set = self.search(**kwargs)
            yield from result_set.results
            if result_set.next_page_key:
                kwargs['page_key'] = result_set.next_page_key
            else:
                return

    @abstractmethod
    def search(
        self,
        search_filter: Optional[DataMetaFilter] = None,
        page_key: Optional[str] = None,
        limit: int = 100
    ) -> ResultSet[DataMeta]:
        """ Search the data storage and return a page of results """
