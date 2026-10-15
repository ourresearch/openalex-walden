"""Load the exchange-rate reference tables for awards `amount_usd` (oxjob #689 rule: rate at the award's start date, USD only).

Sources, all open and widely used:
  ECB euro foreign exchange reference rates, daily since 1999-01-04 (https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip):
      41 currencies quoted per EUR; usd_per_unit = (USD per EUR) / (currency per EUR). Also the annual mean of each daily series.
  World Bank PA.NUS.FCRF, official exchange rate, LCU per USD, period average, annual since 1960 (api.worldbank.org, CC BY 4.0):
      every member country's currency; no Taiwan.
  Federal Reserve H.10 via FRED (DEXTAUS daily, AEXTAUS annual): the Taiwan dollar, which neither of the others carries.
Tables (one row per currency and day / year, usd_per_unit > 0, `source` says which series):
  <schema>.exchange_rates        rate_date DATE, currency STRING, usd_per_unit DOUBLE, source STRING
  <schema>.exchange_rates_annual rate_year INT,  currency STRING, usd_per_unit DOUBLE, source STRING
Precedence when two sources give the same (day | year, currency): ecb, then fed_h10, then worldbank, then the mean of the
Fed daily series (the current, unfinished year only). The nightly
(notebooks/awards/nightly_grants/lib/award_usd.py) reads both tables and never rescales `amount`.

Usage: exchange_rates_load.py --schema openalex_dev.rohan_lab --prefix af1570_   (dev)
       exchange_rates_load.py --schema openalex.common                            (prod, Rohan)
Options: --profile <databricks profile>, --cache <dir> (downloaded files are kept there and reused with --offline).
"""
import argparse, collections, csv, io, json, os, statistics, urllib.request, zipfile

ECB_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
WB_URL = "https://api.worldbank.org/v2/country/all/indicator/PA.NUS.FCRF?format=json&per_page=20000&date=1960:2030"
FRED_DAILY = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXTAUS"
FRED_ANNUAL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=AEXTAUS"
PRECEDENCE = {"ecb": 0, "ecb_daily_mean": 0, "fed_h10": 1, "worldbank_pa_nus_fcrf": 2, "fed_h10_daily_mean": 3}
# World Bank reports by country (ISO3); the currency each one's LCU is. Only currencies that can appear on an award are listed.
ISO3_CURRENCY = {
    "CHL": "CLP", "ARG": "ARS", "COL": "COP", "URY": "UYU", "TZA": "TZS", "SAU": "SAR", "UKR": "UAH", "BTN": "BTN", "IDN": "IDR",
    "DZA": "DZD", "ARM": "AMD", "JPN": "JPY", "CAN": "CAD", "GBR": "GBP", "CHN": "CNY", "CHE": "CHF", "MEX": "MXN", "SWE": "SEK",
    "AUS": "AUD", "NOR": "NOK", "CZE": "CZK", "POL": "PLN", "HKG": "HKD", "DNK": "DKK", "BRA": "BRL", "ISR": "ILS", "NZL": "NZD",
    "PHL": "PHP", "IND": "INR", "KOR": "KRW", "ZAF": "ZAR", "SGP": "SGD", "THA": "THB", "MYS": "MYR", "TUR": "TRY", "HUN": "HUF",
    "ROU": "RON", "BGR": "BGN", "ISL": "ISK", "RUS": "RUB", "EMU": "EUR", "KEN": "KES", "NGA": "NGN", "EGY": "EGP", "GHA": "GHS",
    "UGA": "UGX", "ETH": "ETB", "PAK": "PKR", "BGD": "BDT", "LKA": "LKR", "VNM": "VND", "PER": "PEN", "MAR": "MAD", "TUN": "TND",
    "ZMB": "ZMW", "RWA": "RWF", "SEN": "XOF", "CMR": "XAF", "MWI": "MWK", "MOZ": "MZN", "ZWE": "ZWL", "BWA": "BWP", "NAM": "NAD",
    "ARE": "AED", "QAT": "QAR", "KWT": "KWD", "JOR": "JOD", "LBN": "LBP", "IRN": "IRR", "IRQ": "IQD", "KAZ": "KZT", "GEO": "GEL",
    "AZE": "AZN", "UZB": "UZS", "NPL": "NPR", "MMR": "MMK", "KHM": "KHR", "LAO": "LAK", "MNG": "MNT", "TWN": None,
}


def fetch(url, cache, name, offline):
    path = os.path.join(cache, name)
    if offline or os.path.exists(path):
        return open(path, "rb").read()
    data = urllib.request.urlopen(url, timeout=120).read()
    os.makedirs(cache, exist_ok=True)
    open(path, "wb").write(data)
    return data


def parse(cache, offline):
    daily, annual = {}, {}
    def put(table, key, value, source):
        if value <= 0:
            return
        if key not in table or PRECEDENCE[source] < PRECEDENCE[table[key][1]]:
            table[key] = (value, source)
    # ECB
    z = zipfile.ZipFile(io.BytesIO(fetch(ECB_URL, cache, "eurofxref-hist.zip", offline)))
    rows = list(csv.reader(io.TextIOWrapper(z.open(z.namelist()[0]), encoding="utf-8")))
    header, body = rows[0], rows[1:]
    currencies = [h for h in header[1:] if h]
    by_year = collections.defaultdict(list)
    for row in body:
        if not row or not row[0]:
            continue
        vals = dict(zip(header, row))
        usd_eur = vals.get("USD")
        if not usd_eur or usd_eur == "N/A":
            continue
        put(daily, (row[0], "EUR"), float(usd_eur), "ecb"); by_year[(row[0][:4], "EUR")].append(float(usd_eur))
        for ccy in currencies:
            v = vals.get(ccy)
            if ccy == "USD" or not v or v == "N/A":
                continue
            rate = float(usd_eur) / float(v)
            put(daily, (row[0], ccy), rate, "ecb"); by_year[(row[0][:4], ccy)].append(rate)
    for (year, ccy), values in by_year.items():
        put(annual, (int(year), ccy), statistics.fmean(values), "ecb_daily_mean")
    # Fed H.10 (TWD per USD)
    fed_by_year = collections.defaultdict(list)
    for row in csv.DictReader(io.StringIO(fetch(FRED_DAILY, cache, "DEXTAUS.csv", offline).decode())):
        v = row["DEXTAUS"]
        if v not in ("", "."):
            put(daily, (row["observation_date"], "TWD"), 1.0 / float(v), "fed_h10"); fed_by_year[int(row["observation_date"][:4])].append(1.0 / float(v))
    for year, values in fed_by_year.items():                       # the current year, before H.10 publishes its annual figure
        put(annual, (year, "TWD"), statistics.fmean(values), "fed_h10_daily_mean")
    for row in csv.DictReader(io.StringIO(fetch(FRED_ANNUAL, cache, "AEXTAUS.csv", offline).decode())):
        v = row["AEXTAUS"]
        if v not in ("", "."):
            put(annual, (int(row["observation_date"][:4]), "TWD"), 1.0 / float(v), "fed_h10")
    # World Bank (LCU per USD, annual average)
    for row in json.loads(fetch(WB_URL, cache, "wb_fcrf.json", offline))[1]:
        ccy = ISO3_CURRENCY.get(row.get("countryiso3code"))
        if ccy and row.get("value"):
            put(annual, (int(row["date"]), ccy), 1.0 / float(row["value"]), "worldbank_pa_nus_fcrf")
    return daily, annual


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", required=True)
    ap.add_argument("--prefix", default="")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--cache", default=os.path.expanduser("~/.cache/openalex-exchange-rates"))
    ap.add_argument("--offline", action="store_true")
    args = ap.parse_args()
    daily, annual = parse(args.cache, args.offline)
    print("daily rows", len(daily), "annual rows", len(annual), "currencies", len({k[1] for k in daily} | {k[1] for k in annual}))
    from databricks.connect import DatabricksSession
    b = DatabricksSession.builder
    b = b.profile(args.profile) if args.profile else b
    spark = b.serverless(True).getOrCreate()
    import datetime
    d = spark.createDataFrame([(datetime.date.fromisoformat(k[0]), k[1], float(v[0]), v[1]) for k, v in sorted(daily.items())],
                              "rate_date DATE, currency STRING, usd_per_unit DOUBLE, source STRING")
    a = spark.createDataFrame([(int(k[0]), k[1], float(v[0]), v[1]) for k, v in sorted(annual.items())],
                              "rate_year INT, currency STRING, usd_per_unit DOUBLE, source STRING")
    for name, df in (("exchange_rates", d), ("exchange_rates_annual", a)):
        table = f"{args.schema}.{args.prefix}{name}"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        print("wrote", table, spark.table(table).count())


if __name__ == "__main__":
    main()
