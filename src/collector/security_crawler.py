import datetime
import logging
import sys

import pandas as pd
import requests
from bs4 import BeautifulSoup, ResultSet


USER_AGENT = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36"
COLUMNS_SKIP = ["", "頁面編號"]
URL_SECURITY_TABLE = "https://isin.twse.com.tw/isin/single_main.jsp?"
URL_MONTHLY_PRICES = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TABLE_CLASS = "h4"
MARKET_FILTER = ["上市"]
SECURITY_TYPE_FILTER = ["ETF", "股票"]


logger = logging.getLogger(__name__)


def security_filter(data: dict[str, str]) -> bool:
    cond1 = not data["有價證券代號"][-1].isalpha()
    cond2 = not data["有價證券代號"][0].isalpha()
    cond3 = data["市場別"] in MARKET_FILTER
    cond4 = data["有價證券別"] in SECURITY_TYPE_FILTER
    return all([cond1, cond2, cond3, cond4])


def collect_securities(columns: list[str], rows: ResultSet) -> pd.DataFrame:
    df = pd.DataFrame()
    for row in rows:
        data_dirty = row.text.split("\n")
        data_cleaned = {
            column: content
            for column, content in zip(columns, data_dirty)
            if column not in COLUMNS_SKIP
        }
        if security_filter(data_cleaned):
            df_data = pd.DataFrame([data_cleaned])
            df = pd.concat([df, df_data], ignore_index=True)
    return df


def fetch_security_table() -> pd.DataFrame:
    """
    Collect the table of securities from Taiwan Stock Exchange.
    """
    logger.info("Fetching securities data from Taiwan Stock Exchange website..")
    headers = {"user-agent": USER_AGENT}
    response = requests.get(URL_SECURITY_TABLE, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    logger.info("Cleaning and filtering data..")
    table = soup.find("table", class_=TABLE_CLASS)
    first_row = table.find("tr")
    columns = first_row.text.split("\n")
    other_rows = first_row.find_next_siblings("tr")
    return collect_securities(columns, other_rows)


def search_listed_date(security_code: str) -> datetime.date | None:
    payload = {"owncode": security_code, "stockname": ""}
    headers = {"user-agent": USER_AGENT}
    response = requests.get(URL_SECURITY_TABLE, params=payload, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    td_all = soup.find_all("td")
    for element in td_all:
        text = element.get_text()
        cond1 = len(text.split("/")) == 3
        cond2 = text.replace("/", "").isdigit()
        if cond1 and cond2:
            year, month, day = [int(digit) for digit in text.split("/")]
            return datetime.date(year, month, day)


def fetch_monthly_prices(security_code: str, date_tgt: datetime.date) -> pd.DataFrame:
    date_show = date_tgt.strftime("%Y-%m")
    logger.info(f"Collecting the prices of {security_code} in {date_show}..")

    payload = {
        "response": "json",
        "date": str(date_tgt).replace("-", ""),
        "stockNo": security_code,
    }
    headers = {"user-agent": USER_AGENT}
    response = requests.get(URL_MONTHLY_PRICES, params=payload, headers=headers)
    content = eval(response.text)
    if content["stat"] != "OK":
        raise Exception(f"Fetch failed for {date_tgt}")
    else:
        return pd.DataFrame(content["data"], columns=content["fields"])


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    securities = fetch_security_table()
    date_listed = search_listed_date("00639")
    security_prices = fetch_monthly_prices(
        security_code="00639", date_tgt=datetime.date(2015, 12, 1)
    )
    logger.info(f"Prices collected:\n\n{security_prices}")
