import datetime
import logging
import zoneinfo
from typing import TypedDict

import requests
from bs4 import BeautifulSoup, ResultSet

URL_PAGE = "https://www.twse.com.tw/zh/trading/historical/stock-day.html"
URL_FETCH_LISTINGS = "https://isin.twse.com.tw/isin/single_main.jsp?"
URL_FETCH_DAILY = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"

USER_AGENT = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36"
HEADERS_LISTING = {"user-agent": USER_AGENT}
HEADERS_DAILY = {
    "user-agent": USER_AGENT,
    "Referer": URL_PAGE,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "X-Requested-With": "XMLHttpRequest",
}

TABLE_CLASS = "h4"
MARKET_FILTER = ["上市"]
SECURITY_TYPE_FILTER = ["ETF", "股票"]

TIMEZONE = zoneinfo.ZoneInfo("Asia/Taipei")

COLUMN_MAPPING_LISTINGS = {
    "國際證券編碼": "isin_code",
    "有價證券代號": "security_code",
    "有價證券名稱": "security_name",
    "市場別": "market_type",
    "有價證券別": "security_type",
    "產業別": "industry_category",
    "公開發行/上市(櫃)/發行日": "listing_date",
    "CFICode": "cfi_code",
    "備 註": "note",
}
REQUIRED_FIELDS = {
    "日期",
    "開盤價",
    "收盤價",
    "最低價",
    "最高價",
    "成交筆數",
    "成交股數",
    "成交金額",
    "漲跌價差",
    "註記",
}


logger = logging.getLogger(__name__)


class VisitPageError(Exception):
    """Raised when visiting the referer page to initialize the session fails."""


class TWSEHTTPError(Exception):
    """Raised when a HTTP error."""


class ContentError(Exception):
    """Raised when the response content fails validation checks."""


class ResponseError(Exception):
    """Raised when the response content fails schema/data validation."""


class Listing(TypedDict):
    isin_code: str
    security_code: str
    security_name: str
    market_type: str
    security_type: str
    industry_category: str
    listing_date: datetime.date
    cfi_code: str
    note: str


class SecurityCrawler:
    def __init__(
        self,
        market_filter: list[str] = MARKET_FILTER,
        security_type_filter: list[str] = SECURITY_TYPE_FILTER,
    ):
        self.market_filter = market_filter
        self.security_type_filter = security_type_filter

    @staticmethod
    def _parse_date_string(date_string: str):
        return (
            datetime.datetime.strptime(
                date_string,
                "%Y/%m/%d",
            )
            .replace(tzinfo=TIMEZONE)
            .date()
        )

    def _check_is_target(self, listing: Listing) -> bool:
        cond1 = not listing["security_code"][-1].isalpha()
        cond2 = not listing["security_code"][0].isalpha()
        cond3 = listing["market_type"] in self.market_filter
        cond4 = listing["security_type"] in self.security_type_filter
        return all([cond1, cond2, cond3, cond4])

    def _assemble_listings(
        self, columns_twse: list[str], rows: ResultSet
    ) -> list[Listing]:
        listings = []
        for row in rows:
            elements = row.text.split("\n")

            listing = {}
            for column_twse, element in zip(columns_twse, elements):
                if column_twse in COLUMN_MAPPING_LISTINGS:
                    column_db = COLUMN_MAPPING_LISTINGS[column_twse]
                    if column_db == "listing_date":
                        element = self._parse_date_string(element)

                    listing[column_db] = element

            if self._check_is_target(listing):
                listings.append(listing)
        return listings

    def fetch_listings(self) -> list[Listing]:
        logger.info("Fetching security listings table from TWSE...")
        response = requests.get(URL_FETCH_LISTINGS, headers=HEADERS_LISTING)
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", class_=TABLE_CLASS)
        first_row = table.find("tr")
        columns_twse = first_row.text.split("\n")
        other_rows = first_row.find_next_siblings("tr")
        return self._assemble_listings(columns_twse, other_rows)

    @staticmethod
    def _send_request(code: str, date_tgt: datetime.date) -> requests.Response:
        payload = {
            "response": "json",
            "date": str(date_tgt).replace("-", ""),
            "stockNo": code,
        }

        with requests.Session() as session:
            try:
                session.get(url=URL_PAGE, headers=HEADERS_DAILY)
            except Exception as e:
                raise VisitPageError(f"Initialize session failed: {e}") from e

            try:
                response = session.get(
                    url=URL_FETCH_DAILY, params=payload, headers=HEADERS_DAILY
                )
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise TWSEHTTPError(f"HTTPError: {e}") from e
        return response

    @staticmethod
    def _examine_response_content(content: dict, date_tgt: datetime.date) -> None:
        if content == {"stat": "查詢日期大於今日，請重新查詢!", "total": 0}:
            raise ContentError("Request may be blocked by the server.")

        if content == {"stat": "查詢日期小於99年1月4日，請重新查詢!", "total": 0}:
            raise ContentError("Request may be blocked by the server.")

        if content.get("stat") != "OK":
            raise ContentError("Response stat is not OK.")

        fields = content.get("fields")
        if not fields:
            raise ContentError("No fields response.")

        missing_fields = REQUIRED_FIELDS - set(fields)
        if missing_fields:
            raise ContentError(f"Missing required fields: {missing_fields}.")

        data = content.get("data")
        if not data:
            raise ContentError("No data response.")

        for row in data:
            for idx, ele in enumerate(row):
                if ele is None:
                    raise ContentError("Data containing NULL.")
                if idx == 0:
                    y, m, _ = ele.split("/")
                    if (int(y) + 1911, int(m)) != (date_tgt.year, date_tgt.month):
                        raise ContentError("Out of target date.")

    def fetch_daily_prices_by_month(
        self, code: str, date_tgt: datetime.date
    ) -> list[dict]:
        response = self._send_request(code, date_tgt)

        # Example request URL:
        # https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20160101&stockNo=0050
        url: str = response.url

        try:
            content: dict = response.json()
        except ValueError as e:
            raise ResponseError(
                f"Invalid JSON response: {e}\nURL: {url}\nBody: {response.text!r}"
            )

        # The API returns this response when there's no data for the requested month.
        # e.g. stock 1213 has no data from 2019-05 to 2019-09, then resumes trading.
        # Treat this as a valid "no data" case and return an empty list
        # instead of raising an error.
        if content == {"stat": "很抱歉，沒有符合條件的資料!", "total": 0}:
            return []

        try:
            self._examine_response_content(content, date_tgt)
        except ContentError as e:
            raise ResponseError(f"{e}\nURL: {url}\nContent: {content}")

        fields = content["fields"]
        return [{"code": code, **dict(zip(fields, row))} for row in content["data"]]
