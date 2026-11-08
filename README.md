# Money Master

Who owns the Nasdaq-100 and S&P 500, and what insiders buy and sell.

**Live page:** https://bighigg.github.io/money-master/

## Data

All ownership data comes from SEC EDGAR (public domain):

| Part | SEC source | Delay |
|---|---|---|
| Institutions | Form 13F data sets + every 13F-HR filed after them | up to 45 days after quarter end |
| Insider trades and stakes | Insider transactions data sets + every Form 4 filed after them | 2 business days after the trade |
| Shares outstanding | XBRL company facts and 10-Q/10-K cover pages | last filing |

Prices are the last close from Yahoo Finance. A few share-class counts also come from Yahoo.

Not investment advice. The data can contain errors; check the linked SEC filings.

## Refresh

```
pip install pandas yfinance lxml
SEC_CONTACT=you@example.com python fetch.py
python serve.py 8562
```

SEC requires a contact email in the User-Agent of automated requests.
The first run takes about 20 minutes; a rerun from `edgar_cache/` takes about 4.
