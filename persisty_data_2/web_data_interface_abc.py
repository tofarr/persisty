from abc import abstractmethod, ABC
from typing import Optional

from servey.security.authorization import Authorization

from persisty_data_2.upload_form import UploadForm


class WebDataInterfaceABC(ABC):

    @abstractmethod
    def get_upload_form(self, key: str, authorization: Optional[Authorization]) -> UploadForm:
        """
        Get the upload form parameters
        """

    @abstractmethod
    def get_download_url(self, key: str, authorization: Optional[Authorization]) -> str:
        """
        Get the download url
        """