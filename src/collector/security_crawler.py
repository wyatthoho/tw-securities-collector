import datetime
import logging
import sys
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup, ResultSet


USER_AGENT = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36"
COLUMNS_SKIP = ["", "頁面編號"]
URL_LISTINGS = "https://isin.twse.com.tw/isin/single_main.jsp?"
URL_PRICES = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TABLE_CLASS = "h4"
MARKET_FILTER = ["上市"]
SECURITY_TYPE_FILTER = ["ETF", "股票"]
FETCH_RETRY_BACKOFF = [120, 240, 480, 720, 960]


logger = logging.getLogger(__name__)


class SecurityCrawler:
    def __init__(
        self,
        market_filter: list[str] = MARKET_FILTER,
        security_type_filter: list[str] = SECURITY_TYPE_FILTER,
    ):
        self.market_filter = market_filter
        self.security_type_filter = security_type_filter
        self._headers = {"user-agent": USER_AGENT}

    def _security_filter(self, data: dict[str, str]) -> bool:
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
            if self._security_filter(data_cleaned):
                df_data = pd.DataFrame([data_cleaned])
                df = pd.concat([df, df_data], ignore_index=True)
        return df

    def fetch_listings(self) -> pd.DataFrame:
        logger.info("Fetching security listings table from TWSE...")
        response = requests.get(URL_LISTINGS, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", class_=TABLE_CLASS)
        first_row = table.find("tr")
        columns = first_row.text.split("\n")
        other_rows = first_row.find_next_siblings("tr")
        return self._collect_securities(columns, other_rows)

    def search_listed_date(self, code: str) -> datetime.date | None:
        payload = {"owncode": code, "stockname": ""}
        response = requests.get(URL_LISTINGS, params=payload, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        td_all = soup.find_all("td")
        for element in td_all:
            text = element.get_text()
            cond1 = len(text.split("/")) == 3
            cond2 = text.replace("/", "").isdigit()
            if cond1 and cond2:
                year, month, day = [int(digit) for digit in text.split("/")]
                return datetime.date(year, month, day)

    def fetch_monthly_prices(self, code: str, date_tgt: datetime.date) -> pd.DataFrame:
        payload = {
            "response": "json",
            "date": str(date_tgt).replace("-", ""),
            "stockNo": code,
        }

        for backoff in FETCH_RETRY_BACKOFF:
            session = requests.Session()
            try:
                # https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20160101&stockNo=0050
                response = session.get(URL_PRICES, params=payload, headers=self._headers)
                response.raise_for_status()
                content = response.json()
            finally:
                session.close()

            if content["stat"] == "OK":
                df = pd.DataFrame(content["data"], columns=content["fields"])
                df.insert(0, "code", code)
                return df

            logger.warning(f"{content}")
            time.sleep(backoff)

        raise Exception(f"Exhausted retries for {code} {date_tgt.strftime('%Y-%m')}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    crawler = SecurityCrawler()
    securities = crawler.fetch_listings()
    date_listed = crawler.search_listed_date("00639")
    security_prices = crawler.fetch_monthly_prices(
        code="00639", date_tgt=datetime.date(2015, 12, 1)
    )
    logger.info("Prices collected successfully.")
