from datetime import date
from typing import Final, Generator, Sequence, cast, Iterable, Mapping, Optional, Union, List
from json import loads

from requests import Response, Session
from tenacity import retry, stop_after_attempt
from pandas import DataFrame

from ._model import (
    EpiRangeLike,
    AEpiDataCall,
    EpiDataFormatType,
    EpiDataResponse,
    EpiRange,
    EpidataFieldInfo,
    OnlySupportsClassicFormatException,
    add_endpoint_to_url,
)
from ._endpoints import AEpiDataEndpoints
from ._constants import HTTP_HEADERS, BASE_URL
from ._covidcast import CovidcastDataSources, define_covidcast_fields


@retry(reraise=True, stop=stop_after_attempt(2))
def _request_with_retry(
    url: str, params: Mapping[str, str], api_key: str, session: Optional[Session] = None, stream: bool = False
) -> Response:
    """Make request with a retry if an exception is thrown."""

    def call_impl(s: Session) -> Response:
        res = s.get(url, params=params, headers=HTTP_HEADERS, stream=stream, auth=("epidata", api_key))
        if res.status_code == 414:
            return s.post(url, params=params, headers=HTTP_HEADERS, stream=stream, auth=("epidata", api_key))
        return res

    if session:
        return call_impl(session)

    with Session() as s:
        return call_impl(s)


class EpiDataCall(AEpiDataCall):
    """
    epidata call representation
    """

    _session: Final[Optional[Session]]

    def __init__(
        self,
        base_url: str,
        api_key: str,
        session: Optional[Session],
        endpoint: str,
        params: Mapping[str, Union[None, EpiRangeLike, Iterable[EpiRangeLike]]],
        meta: Optional[Sequence[EpidataFieldInfo]] = None,
        only_supports_classic: bool = False,
    ) -> None:
        super().__init__(base_url, api_key, endpoint, params, meta, only_supports_classic)
        self._session = session

    def with_base_url(self, base_url: str) -> "EpiDataCall":
        return EpiDataCall(base_url, self._api_key, self._session, self._endpoint, self._params)

    def with_session(self, session: Session) -> "EpiDataCall":
        return EpiDataCall(self._base_url, self._api_key, session, self._endpoint, self._params)

    def with_api_key(self, api_key: str) -> "EpiDataCall":
        return EpiDataCall(self._base_url, api_key, self._session, self._endpoint, self._params)

    def _call(
        self,
        format_type: Optional[EpiDataFormatType] = None,
        fields: Optional[Iterable[str]] = None,
        stream: bool = False,
    ) -> Response:
        url, params = self.request_arguments(format_type, fields)
        return _request_with_retry(url, params, self._api_key, self._session, stream)

    def classic(
        self, fields: Optional[Iterable[str]] = None, disable_date_parsing: Optional[bool] = False
    ) -> EpiDataResponse:
        """Request and parse epidata in CLASSIC message format."""
        self._verify_parameters()
        try:
            response = self._call(None, fields)
            r = cast(EpiDataResponse, response.json())
            epidata = r.get("epidata")
            if epidata and isinstance(epidata, list) and len(epidata) > 0 and isinstance(epidata[0], dict):
                r["epidata"] = [self._parse_row(row, disable_date_parsing=disable_date_parsing) for row in epidata]
            return r
        except Exception as e:  # pylint: disable=broad-except
            return {"result": 0, "message": f"error: {e}", "epidata": []}

    def __call__(
        self, fields: Optional[Iterable[str]] = None, disable_date_parsing: Optional[bool] = False
    ) -> EpiDataResponse:
        """Request and parse epidata in CLASSIC message format."""
        return self.classic(fields, disable_date_parsing=disable_date_parsing)

    def json(
        self, fields: Optional[Iterable[str]] = None, disable_date_parsing: Optional[bool] = False
    ) -> List[Mapping[str, Union[str, int, float, date, None]]]:
        """Request and parse epidata in JSON format"""
        if self.only_supports_classic:
            raise OnlySupportsClassicFormatException()
        self._verify_parameters()
        response = self._call(EpiDataFormatType.json, fields)
        response.raise_for_status()
        return [
            self._parse_row(row, disable_date_parsing=disable_date_parsing)
            for row in cast(List[Mapping[str, Union[str, int, float, None]]], response.json())
        ]

    def df(self, fields: Optional[Iterable[str]] = None, disable_date_parsing: Optional[bool] = False) -> DataFrame:
        """Request and parse epidata as a pandas data frame"""
        if self.only_supports_classic:
            raise OnlySupportsClassicFormatException()
        self._verify_parameters()
        r = self.json(fields, disable_date_parsing=disable_date_parsing)
        return self._as_df(r, fields, disable_date_parsing=disable_date_parsing)

    def csv(self, fields: Optional[Iterable[str]] = None) -> str:
        """Request and parse epidata in CSV format"""
        if self.only_supports_classic:
            raise OnlySupportsClassicFormatException()
        self._verify_parameters()
        response = self._call(EpiDataFormatType.csv, fields)
        response.raise_for_status()
        return response.text

    def iter(
        self, fields: Optional[Iterable[str]] = None, disable_date_parsing: Optional[bool] = False
    ) -> Generator[Mapping[str, Union[str, int, float, date, None]], None, Response]:
        """Request and streams epidata rows"""
        if self.only_supports_classic:
            raise OnlySupportsClassicFormatException()
        self._verify_parameters()
        response = self._call(EpiDataFormatType.jsonl, fields, stream=True)
        response.raise_for_status()
        for line in response.iter_lines():
            yield self._parse_row(loads(line), disable_date_parsing=disable_date_parsing)
        return response

    def __iter__(self) -> Generator[Mapping[str, Union[str, int, float, date, None]], None, Response]:
        return self.iter()


class Epidata(AEpiDataEndpoints[EpiDataCall]):
    """
    sync epidata call class
    """

    _base_url: Final[str]
    _api_key: Final[str]
    _session: Final[Optional[Session]]

    def __init__(self, api_key: str, base_url: str = BASE_URL, session: Optional[Session] = None) -> None:
        super().__init__()
        self._api_key = api_key
        self._base_url = base_url
        self._session = session

    def with_base_url(self, base_url: str) -> "Epidata":
        return Epidata(self._api_key, base_url, self._session)

    def with_session(self, session: Session) -> "Epidata":
        return Epidata(self._api_key, self._base_url, session)

    def with_api_key(self, api_key: str) -> "Epidata":
        return Epidata(api_key, self._base_url, self._session)

    def _create_call(
        self,
        endpoint: str,
        params: Mapping[str, Union[None, EpiRangeLike, Iterable[EpiRangeLike]]],
        meta: Optional[Sequence[EpidataFieldInfo]] = None,
        only_supports_classic: bool = False,
    ) -> EpiDataCall:
        return EpiDataCall(self._base_url, self._api_key, self._session, endpoint, params, meta, only_supports_classic)


def CovidcastEpidata(
    api_key: str, base_url: str = BASE_URL, session: Optional[Session] = None
) -> CovidcastDataSources[EpiDataCall]:
    url = add_endpoint_to_url(base_url, "covidcast/meta")
    meta_data_res = _request_with_retry(url, {}, api_key, session, False)
    meta_data_res.raise_for_status()
    meta_data = meta_data_res.json()

    def create_call(params: Mapping[str, Union[None, EpiRangeLike, Iterable[EpiRangeLike]]]) -> EpiDataCall:
        return EpiDataCall(base_url, api_key, session, "covidcast", params, define_covidcast_fields())

    return CovidcastDataSources.create(meta_data, create_call)


__all__ = ["Epidata", "EpiDataCall", "EpiRange", "CovidcastEpidata"]
