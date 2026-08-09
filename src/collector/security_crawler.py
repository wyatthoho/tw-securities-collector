import datetime
import logging

import pandas as pd
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
COLUMNS_SKIP = ["", "頁面編號"]
MARKET_FILTER = ["上市"]
SECURITY_TYPE_FILTER = ["ETF", "股票"]
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


class SecurityCrawler:
    def __init__(
        self,
        market_filter: list[str] = MARKET_FILTER,
        security_type_filter: list[str] = SECURITY_TYPE_FILTER,
    ):
        self.market_filter = market_filter
        self.security_type_filter = security_type_filter

    def _filter_security(self, data: dict[str, str]) -> bool:
        cond1 = not data["有價證券代號"][-1].isalpha()
        cond2 = not data["有價證券代號"][0].isalpha()
        cond3 = data["市場別"] in self.market_filter
        cond4 = data["有價證券別"] in self.security_type_filter
        return all([cond1, cond2, cond3, cond4])

    def _collect_securities(self, columns: list[str], rows: ResultSet) -> pd.DataFrame:
        df = pd.DataFrame()
        for row in rows:
            data_dirty = row.text.split("\n")
            data_cleaned = {
                column: content
                for column, content in zip(columns, data_dirty)
                if column not in COLUMNS_SKIP
            }
            if self._filter_security(data_cleaned):
                df_data = pd.DataFrame([data_cleaned])
                df = pd.concat([df, df_data], ignore_index=True)
        return df

    def fetch_listings(self) -> pd.DataFrame:
        logger.info("Fetching security listings table from TWSE...")
        response = requests.get(URL_FETCH_LISTINGS, headers=HEADERS_LISTING)
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", class_=TABLE_CLASS)
        first_row = table.find("tr")
        columns = first_row.text.split("\n")
        other_rows = first_row.find_next_siblings("tr")
        return self._collect_securities(columns, other_rows)

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
    ) -> pd.DataFrame:
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
        # Treat this as a valid "no data" case and return an empty DataFrame
        # instead of raising an error.
        if content == {"stat": "很抱歉，沒有符合條件的資料!", "total": 0}:
            return pd.DataFrame()

        try:
            self._examine_response_content(content, date_tgt)
        except ContentError as e:
            raise ResponseError(f"{e}\nURL: {url}\nContent: {content}")

        df = pd.DataFrame(content["data"], columns=content["fields"])
        df.insert(0, "code", code)
        return df
