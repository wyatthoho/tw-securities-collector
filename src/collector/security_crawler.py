import datetime
import functools
import logging
import time
import zoneinfo
from collections.abc import Callable
from typing import TypedDict

import requests
from bs4 import BeautifulSoup, ResultSet

URL_PAGE = "https://www.twse.com.tw/zh/trading/historical/stock-day.html"
URL_FETCH_SECURITIES = "https://isin.twse.com.tw/isin/single_main.jsp?"
URL_FETCH_DAILY_BARS = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
URL_INDEX_REDIRECT = "https://www.twse.com.tw/zh/index.html"
USER_AGENT = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36"
HEADERS_SECURITIES = {"user-agent": USER_AGENT}
HEADERS_DAILY_BARS = {
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

COLUMN_MAPPING_SECURITIES = {
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
COLUMN_MAPPING_DAILY_BARS = {
    "日期": "trade_date",
    "成交股數": "trade_shares",
    "成交金額": "trade_value",
    "開盤價": "opening_price",
    "最高價": "highest_price",
    "最低價": "lowest_price",
    "收盤價": "closing_price",
    "漲跌價差": "price_change",
    "成交筆數": "trade_count",
    "註記": "note",
}

MAX_ATTEMPTS = 5
BACKOFF_SECONDS = 360

logger = logging.getLogger(__name__)


def with_retry[T](func: Callable[..., T]) -> Callable[..., T]:
    @functools.wraps(func)
    def wrapper(self: "SecurityCrawler", *args, **kwargs) -> T:
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return func(self, *args, **kwargs)
            except ResponseError as e:
                last_error = e
                logger.warning(f"Got invalid daily response: {e}")
            except (TWSEHTTPError, VisitPageError) as e:
                last_error = e
                logger.warning(f"Send daily request failed: {e}")

            if attempt == MAX_ATTEMPTS:
                break

            logger.info(f"Sleeping for {BACKOFF_SECONDS} seconds...")
            time.sleep(BACKOFF_SECONDS)
            logger.info(f"Retrying (attempt {attempt}/{MAX_ATTEMPTS})...")

        raise FetchDailyBarsError(f"{last_error}") from last_error

    return wrapper


class VisitPageError(Exception):
    """Raised when visiting the referer page to initialize the session fails."""


class TWSEHTTPError(Exception):
    """Raised when a HTTP error."""


class ResponseError(Exception):
    """Raised when the response content fails schema/data validation."""


class FetchDailyBarsError(Exception):
    """Raised when fetching daily bars fails after exhausting all retry attempts."""


class Security(TypedDict):
    isin_code: str
    security_code: str
    security_name: str
    market_type: str
    security_type: str
    industry_category: str
    listing_date: datetime.date
    cfi_code: str
    note: str


class RawBar(TypedDict, total=False):
    trade_date: str
    trade_shares: str
    trade_value: str
    opening_price: str
    highest_price: str
    lowest_price: str
    closing_price: str
    price_change: str
    trade_count: str
    note: str


class DailyBar(TypedDict):
    security_code: str
    trade_date: datetime.date
    trade_shares: int
    trade_value: int
    opening_price: float
    highest_price: float
    lowest_price: float
    closing_price: float
    price_change: str
    trade_count: int
    note: str


class SecurityCrawler:
    def __init__(
        self,
        market_filter: list[str] = MARKET_FILTER,
        security_type_filter: list[str] = SECURITY_TYPE_FILTER,
    ):
        self.market_filter = market_filter
        self.security_type_filter = security_type_filter
        logger.info("Initialized SecurityCrawler")

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

    def _check_is_target(self, security: Security) -> bool:
        cond1 = not security["security_code"][-1].isalpha()
        cond2 = not security["security_code"][0].isalpha()
        cond3 = security["market_type"] in self.market_filter
        cond4 = security["security_type"] in self.security_type_filter
        return all([cond1, cond2, cond3, cond4])

    def _assemble_securities(
        self, columns_twse: list[str], rows: ResultSet
    ) -> list[Security]:
        securities = []
        for row in rows:
            elements = row.text.split("\n")

            security = {}
            for column_twse, element in zip(columns_twse, elements):
                if column_twse in COLUMN_MAPPING_SECURITIES:
                    column_db = COLUMN_MAPPING_SECURITIES[column_twse]
                    if column_db == "listing_date":
                        element = self._parse_date_string(element)

                    security[column_db] = element

            if self._check_is_target(security):
                securities.append(security)
        return securities

    def fetch_securities(self) -> list[Security]:
        logger.info("Fetching securities from TWSE...")
        response = requests.get(URL_FETCH_SECURITIES, headers=HEADERS_SECURITIES)
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", class_=TABLE_CLASS)
        first_row = table.find("tr")
        columns_twse = first_row.text.split("\n")
        other_rows = first_row.find_next_siblings("tr")
        return self._assemble_securities(columns_twse, other_rows)

    @staticmethod
    def _send_daily_request(
        security_code: str, date_tgt: datetime.date
    ) -> requests.Response:
        payload = {
            "response": "json",
            "date": str(date_tgt).replace("-", ""),
            "stockNo": security_code,
        }

        with requests.Session() as session:
            try:
                session.get(url=URL_PAGE, headers=HEADERS_DAILY_BARS)
            except Exception as e:
                raise VisitPageError(f"Initialize session failed: {e}") from e

            try:
                response = session.get(
                    url=URL_FETCH_DAILY_BARS, params=payload, headers=HEADERS_DAILY_BARS
                )
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise TWSEHTTPError(f"HTTPError: {e}") from e
        return response

    @staticmethod
    def _examine_response_content(content: dict, date_tgt: datetime.date) -> None:
        if content == {"stat": "查詢日期大於今日，請重新查詢!", "total": 0}:
            raise ResponseError("The query date is later than today.")

        if content == {"stat": "查詢日期小於99年1月4日，請重新查詢!", "total": 0}:
            raise ResponseError("The query date is earlier than 1910/1/4.")

        if content.get("stat") != "OK":
            raise ResponseError("Response stat is not OK.")

        fields = content.get("fields")
        if not fields:
            raise ResponseError("No fields response.")

        missing_fields = COLUMN_MAPPING_DAILY_BARS.keys() - set(fields)
        if missing_fields:
            raise ResponseError(f"Missing required fields: {missing_fields}.")

        data = content.get("data")
        if not data:
            raise ResponseError("No data response.")

        for row in data:
            for idx, ele in enumerate(row):
                if ele is None:
                    raise ResponseError("Data containing NULL.")
                if idx == 0:
                    y, m, _ = ele.split("/")
                    if (int(y) + 1911, int(m)) != (date_tgt.year, date_tgt.month):
                        raise ResponseError("Out of target date.")

    @staticmethod
    def _remove_separator(value: str) -> str:
        return value.replace(",", "")

    @staticmethod
    def _roc_date_to_date(roc_date: str) -> datetime.date:
        year, month, day = map(int, roc_date.split("/"))
        return datetime.datetime(year + 1911, month, day, tzinfo=TIMEZONE).date()

    def _assemble_daily_bars(
        self, security_code: str, columns_twse: list[str], rows: list[list[str]]
    ) -> list[DailyBar]:
        """
        Converts TWSE raw rows into daily bars keyed by DB column names.

        Price change is kept as the raw string rather than parsed to a
        number, since it can contain non-numeric indicators (+, -, X);
        'X' denotes ex-dividend/ex-rights days. Derive from 'closing_price'
        if historical changes are required.

        Non-trading days (e.g. 0051), where all price fields are "--",
        are skipped.
        """

        daily_bars = []
        for elements in rows:
            raw_bar: RawBar = {}
            for column_twse, element in zip(columns_twse, elements):
                column_db = COLUMN_MAPPING_DAILY_BARS[column_twse]
                raw_bar[column_db] = element

            trade_date = self._roc_date_to_date(raw_bar["trade_date"])

            if (
                raw_bar["opening_price"]
                == raw_bar["highest_price"]
                == raw_bar["lowest_price"]
                == raw_bar["closing_price"]
                == "--"
            ):
                msg = f"Skipping non-trading day for {security_code} on {trade_date}"
                logger.warning(msg)
                continue

            opening_price = float(self._remove_separator(raw_bar["opening_price"]))
            highest_price = float(self._remove_separator(raw_bar["highest_price"]))
            lowest_price = float(self._remove_separator(raw_bar["lowest_price"]))
            closing_price = float(self._remove_separator(raw_bar["closing_price"]))
            trade_count = int(self._remove_separator(raw_bar["trade_count"]))
            trade_shares = int(self._remove_separator(raw_bar["trade_shares"]))
            trade_value = int(self._remove_separator(raw_bar["trade_value"]))

            daily_bar: DailyBar = {
                "security_code": security_code,
                "trade_date": trade_date,
                "trade_shares": trade_shares,
                "trade_value": trade_value,
                "opening_price": opening_price,
                "highest_price": highest_price,
                "lowest_price": lowest_price,
                "closing_price": closing_price,
                "price_change": raw_bar["price_change"],
                "trade_count": trade_count,
                "note": raw_bar["note"],
            }
            daily_bars.append(daily_bar)
        return daily_bars

    @with_retry
    def fetch_daily_bars(
        self, security_code: str, date_tgt: datetime.date
    ) -> list[DailyBar]:
        """Fetch one month of daily bars for a security from TWSE.

        Example request URL:
        https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20160101&stockNo=0050

        The API returns {"stat": "很抱歉，沒有符合條件的資料!", "total": 0} when there's no
        data for the requested month (e.g. stock 1213 has no data from 2019-05 to 2019-09,
        then resumes trading). This is treated as a valid "no data" case, returning an empty
        list instead of raising an error.
        """
        response = self._send_daily_request(security_code, date_tgt)

        if response.url == URL_INDEX_REDIRECT:
            raise ResponseError("Redirected to homepage.")

        try:
            content: dict = response.json()
        except ValueError as e:
            raise ResponseError(f"Invalid JSON response: {e}\nBody: {response.text!r}")

        if content == {"stat": "很抱歉，沒有符合條件的資料!", "total": 0}:
            return []

        self._examine_response_content(content, date_tgt)

        columns_twse = content["fields"]
        rows = content["data"]
        return self._assemble_daily_bars(security_code, columns_twse, rows)
