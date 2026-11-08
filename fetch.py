"""Money Master data pull from SEC EDGAR (public domain).

Institutions (13F):
  - SEC "Form 13F data sets" (bulk, quarterly): the two newest sets.
  - Every 13F-HR filed after the newest set: one full-submission file each.
  - Each filer's newest report wins; the report before it gives the change.
Insiders (Form 4):
  - SEC "insider transactions data sets" (bulk, quarterly) for the window.
  - Every Form 4 filed after the newest set, for our issuers only.
CUSIP -> ticker: SEC fails-to-deliver files.  Shares outstanding: SEC XBRL.
Price: Yahoo (yfinance), last close - EDGAR has no prices.

Usage:
  python money_master_public/fetch.py            # Nasdaq-100 + S&P 500 (~520 names)
  python money_master_public/fetch.py AAPL MSFT  # only these tickers

SEC wants a contact email in the User-Agent. Set the SEC_CONTACT
environment variable, or put it in sec_contact.txt (one line, git-ignored).
"""
import io
import json
import re
import sys
import threading
import time
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
EC = HERE / "edgar_cache"
OLD_LISTS = HERE.parent / "stage_screener" / "cache"
WEB_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")
SEC = "https://www.sec.gov"
INDEX_SOURCES = {  # first source with enough rows wins
    "SP500": (400, ["https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                    "https://www.slickcharts.com/sp500"]),
    "NDX": (90, ["https://www.slickcharts.com/nasdaq100",
                 "https://stockanalysis.com/list/nasdaq-100-stocks/"]),
}
TOP_HOLDERS = 25
INSIDER_DAYS = 548   # about 18 months
STALE_DAYS = 200     # a 13F filer whose newest report is this much older is gone
SEC_RATE = 8         # requests per second (SEC limit is 10)
WORKERS = 8
FTD_FILES = 6        # fails-to-deliver files for the CUSIP map (~3 months)


# ---------------------------------------------------------------- http
def sec_ua():
    import os
    f = HERE / "sec_contact.txt"
    who = os.environ.get("SEC_CONTACT") or (f.read_text().strip() if f.exists() else "")
    if not who:
        raise SystemExit("Set SEC_CONTACT or put a contact email in sec_contact.txt (SEC rule).")
    return "MoneyMaster " + who


_lock, _next = threading.Lock(), [0.0]


def _wait():
    with _lock:
        now = time.time()
        t = max(now, _next[0])
        _next[0] = t + 1.0 / SEC_RATE
    if t > now:
        time.sleep(t - now)


def get(url, binary=False, ua=None):
    """GET with SEC rate limit and retry. Returns bytes or text."""
    sec = "sec.gov" in url
    req = urllib.request.Request(url, headers={"User-Agent": ua or (sec_ua() if sec else WEB_UA),
                                               "Accept-Encoding": "identity"})
    for attempt in range(4):
        if sec:
            _wait()
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                b = r.read()
            return b if binary else b.decode("utf-8", "replace")
        except Exception as e:
            code = getattr(e, "code", None)
            if code == 404 or attempt == 3:
                raise
            time.sleep(3 + 5 * attempt + (20 if code in (403, 429) else 0))


def cached(path, url, max_age_h=None, binary=True):
    """Download once into edgar_cache; refresh if older than max_age_h."""
    p = EC / path
    if p.exists() and (max_age_h is None or time.time() - p.stat().st_mtime < max_age_h * 3600):
        return p
    p.parent.mkdir(parents=True, exist_ok=True)
    b = get(url, binary=True)
    tmp = p.with_suffix(p.suffix + ".part")
    tmp.write_bytes(b)
    tmp.replace(p)
    return p


def num(s):
    try:
        return float(str(s).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def sec_date(s):
    """'31-JUL-2026' or '2026-07-31' or '07-31-2026' -> datetime (None if bad)."""
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(str(s).strip().title(), fmt)
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------- universe
def index_members():
    """Return {ticker: ["SP500", "NDX"]}. Fall back to stage_screener's lists."""
    out = {}
    for key, (need, urls) in INDEX_SOURCES.items():
        got = set()
        for url in urls:
            try:
                for t in pd.read_html(io.StringIO(get(url))):
                    col = next((c for c in ("Symbol", "Ticker") if c in t.columns), None)
                    if col and len(t) >= need:
                        got = {str(x).strip() for x in t[col] if str(x).strip()}
                        break
            except Exception as e:
                print(f"  ! {key}: {url.split('/')[2]} failed ({e})")
            if got:
                print(f"  {key}: {len(got)} names from {url.split('/')[2]}")
                break
        if not got:
            f = OLD_LISTS / ("universe_sp500.json" if key == "SP500" else "universe_ndx.json")
            got = set(json.loads(f.read_text()))
            print(f"  ! {key}: using old list {f.name} ({len(got)} names)")
        for t in got:
            out.setdefault(t.replace("-", "."), []).append(key)
    return out


def norm_holder(name):
    n = name.lower().replace(",", "").replace(".", "")
    for key, label in [("vanguard", "Vanguard Group"), ("blackrock", "BlackRock"),
                       ("state street", "State Street"), ("fmr", "Fidelity (FMR)"),
                       ("geode", "Geode Capital"), ("jpmorgan", "JPMorgan Chase"),
                       ("morgan stanley", "Morgan Stanley"), ("norges", "Norges Bank"),
                       ("t rowe", "T. Rowe Price"), ("capital research", "Capital Research"),
                       ("capital world", "Capital World"), ("northern trust", "Northern Trust"),
                       ("goldman", "Goldman Sachs"), ("bank of america", "Bank of America"),
                       ("ubs", "UBS"), ("berkshire", "Berkshire Hathaway")]:
        if key in n:
            return label
    return pretty(name)


def pretty(name):
    """'VANGUARD GROUP INC' -> 'Vanguard Group Inc'; keep mixed-case names."""
    name = " ".join(str(name).split())
    if name.upper() != name:
        return name
    t = name.title()
    for w in ("Llc", "Lp", "Llp", "Plc", "Ag", "Sa", "Nv", "Ii", "Iii", "Usa", "Na", "Ny"):
        t = re.sub(rf"\b{w}\b", w.upper(), t)
    return t


# ---------------------------------------------------------------- SEC lists
def dataset_links(page, pat):
    html = get(f"{SEC}/data-research/sec-markets-data/{page}")
    return sorted(set(re.findall(rf'href="([^"]*{pat})"', html)))


def company_tickers():
    p = cached("company_tickers.json", f"{SEC}/files/company_tickers.json", max_age_h=24)
    d = json.loads(p.read_text())
    order, cik = [], {}
    for v in d.values():
        t = v["ticker"].replace("-", ".").upper()
        if t not in cik:
            cik[t] = (int(v["cik_str"]), v["title"])
            order.append(t)
    return cik, order


def cusip_map(tickers):
    """{cusip: ticker} for our tickers, from the newest fails-to-deliver files."""
    want = {t.replace(".", ""): t for t in tickers}
    links = dataset_links("fails-deliver-data", r"cnsfails\d+[ab]\.zip")
    links = sorted(links, key=lambda u: re.search(r"cnsfails(\d+[ab])", u).group(1))[-FTD_FILES:]
    out = {}
    for u in links:
        p = cached("ftd/" + u.split("/")[-1], SEC + u)
        with zipfile.ZipFile(p) as z:
            txt = z.read(z.namelist()[0]).decode("latin-1")
        for line in txt.splitlines()[1:]:
            f = line.split("|")
            if len(f) > 2 and f[2] in want:
                out[f[1]] = want[f[2]]
    return out


def form_idx(year, q):
    """Rows (form, name, cik, date, path) from EDGAR full-index form.idx."""
    cur = (datetime.now().year, (datetime.now().month - 1) // 3 + 1) == (year, q)
    p = cached(f"idx/{year}Q{q}_form.idx", f"{SEC}/Archives/edgar/full-index/{year}/QTR{q}/form.idx",
               max_age_h=12 if cur else None)
    rows = []
    for line in p.read_text(encoding="latin-1").splitlines():
        m = re.match(r"^(\S+(?: \S+)?)\s{2,}(.+?)\s{2,}(\d+)\s+(\d{4}-\d{2}-\d{2})\s+(edgar/\S+)", line)
        if m:
            rows.append(m.groups())
    return rows


def quarters_since(d):
    y, q = d.year, (d.month - 1) // 3 + 1
    now = datetime.now()
    while (y, q) <= (now.year, (now.month - 1) // 3 + 1):
        yield y, q
        y, q = (y + 1, 1) if q == 4 else (y, q + 1)


# ---------------------------------------------------------------- 13F
def load_13f_bulk(url, cusips):
    """One 13F data set -> list of reports {cik, name, period, filed, pos{cusip: [shares, value]}}."""
    p = cached("13f/" + url.split("/")[-1], SEC + url)
    with zipfile.ZipFile(p) as z:
        sub = pd.read_csv(z.open("SUBMISSION.tsv"), sep="\t", dtype=str)
        cov = pd.read_csv(z.open("COVERPAGE.tsv"), sep="\t", dtype=str,
                          usecols=["ACCESSION_NUMBER", "FILINGMANAGER_NAME"])
        parts = []
        for ch in pd.read_csv(z.open("INFOTABLE.tsv"), sep="\t", dtype=str, chunksize=500_000,
                              usecols=["ACCESSION_NUMBER", "CUSIP", "VALUE", "SSHPRNAMT", "SSHPRNAMTTYPE", "PUTCALL"]):
            ch = ch[ch.CUSIP.str.upper().isin(cusips) & ch.PUTCALL.isna() & (ch.SSHPRNAMTTYPE == "SH")]
            parts.append(ch)
    info = pd.concat(parts)
    info["CUSIP"] = info.CUSIP.str.upper()
    info["SH"] = pd.to_numeric(info.SSHPRNAMT, errors="coerce").fillna(0)
    info["VAL"] = pd.to_numeric(info.VALUE, errors="coerce").fillna(0)
    pos = info.groupby(["ACCESSION_NUMBER", "CUSIP"])[["SH", "VAL"]].sum()
    sub = sub[sub.SUBMISSIONTYPE == "13F-HR"].merge(cov, on="ACCESSION_NUMBER", how="left")
    by_acc = defaultdict(dict)
    for (acc, cu), r in pos.iterrows():
        by_acc[acc][cu] = [r.SH, r.VAL]
    reps = []
    for r in sub.itertuples():
        reps.append({"cik": int(r.CIK), "name": r.FILINGMANAGER_NAME or "", "acc": r.ACCESSION_NUMBER,
                     "period": sec_date(r.PERIODOFREPORT), "filed": sec_date(r.FILING_DATE),
                     "pos": by_acc.get(r.ACCESSION_NUMBER, {})})
    print(f"  13F set {url.split('/')[-1]}: {len(reps)} reports, {len(info)} rows in our names", flush=True)
    return reps


TAG = lambda t: rf"<(?:\w+:)?{t}>\s*(.*?)\s*</(?:\w+:)?{t}>"


def load_13f_filing(row, cusips):
    """One 13F-HR full submission -> report dict (only our CUSIPs kept). Cached."""
    form, name, cik, filed, path = row
    acc = path.split("/")[-1].replace(".txt", "")
    p = EC / "13f_new" / f"{acc}.json"
    d = json.loads(p.read_text()) if p.exists() else None
    if not d or d.get("ver") != 2:  # ver 2 keeps the reported value too
        txt = get(f"{SEC}/Archives/{path}")
        per = re.search(TAG("periodOfReport"), txt, re.S)
        pos = defaultdict(lambda: [0.0, 0.0])
        for blk in re.findall(r"<(?:\w+:)?infoTable>(.*?)</(?:\w+:)?infoTable>", txt, re.S):
            cu = re.search(TAG("cusip"), blk, re.S)
            if not cu or cu.group(1).upper() not in cusips:
                continue
            if re.search(TAG("putCall"), blk, re.S):
                continue
            ty = re.search(TAG("sshPrnamtType"), blk, re.S)
            if ty and ty.group(1).upper() != "SH":
                continue
            sh = re.search(TAG("sshPrnamt"), blk, re.S)
            va = re.search(TAG("value"), blk, re.S)
            x = pos[cu.group(1).upper()]
            x[0] += num(sh.group(1)) if sh else 0
            x[1] += num(va.group(1)) if va else 0
        d = {"ver": 2, "cik": int(cik), "name": name, "acc": acc, "filed": filed,
             "period": per.group(1) if per else None, "pos": pos}
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(d))
    d["period"] = sec_date(d["period"]) if d["period"] else None
    d["filed"] = sec_date(d["filed"])
    return d


def plausible(sh, val, price):
    """False when a 13F row's value / shares cannot be the stock's price.

    Catches filers that put a wrong number in the shares column (one big
    pension fund reported 6.4B AAPL shares worth $22M). Value is in dollars;
    a filer that still reports in $1000s passes through the x1000 test.
    """
    if sh <= 0:
        return False
    if not price or price != price or price <= 0 or val <= 0:  # no price (NaN) -> keep
        return True
    imp = val / sh
    return any(price / 8 <= x <= price * 8 for x in (imp, imp * 1000))


def institutions(cusips, cprice):
    """{cusip: {"rows": [...holders...], "shares": total, "filers": n, "dropped": n}}."""
    links = [u for u in dataset_links("form-13f-data-sets", r"_form13f\.zip")
             if re.search(r"-\d{2}[a-z]{3}\d{4}_", u)]  # older sets use another name
    end = lambda u: datetime.strptime(re.search(r"-(\d{2}[a-z]{3}\d{4})_", u).group(1), "%d%b%Y")
    links = sorted(links, key=end)[-2:]
    reps = []
    for u in links:
        reps += load_13f_bulk(u, cusips)
    after = end(links[-1])
    rows = [r for y, q in quarters_since(after) for r in form_idx(y, q)
            if r[0] == "13F-HR" and r[3] > after.strftime("%Y-%m-%d")]
    print(f"  13F-HR filed after {after:%Y-%m-%d}: {len(rows)} filings", flush=True)
    done = [0]

    def one(r):
        try:
            return load_13f_filing(r, cusips)
        except Exception as e:
            print(f"  ! 13F {r[4]}: {e}", flush=True)
            return None
        finally:
            done[0] += 1
            if done[0] % 500 == 0:
                print(f"    13F filings {done[0]}/{len(rows)}", flush=True)
    with ThreadPoolExecutor(WORKERS) as ex:
        reps += [r for r in ex.map(one, rows) if r and r["period"]]

    # newest report per filer; the newest report with an older period gives the change
    by = defaultdict(list)
    for r in reps:
        by[r["cik"]].append(r)
    newest = max(r["period"] for r in reps)
    out = defaultdict(lambda: {"rows": [], "shares": 0.0, "filers": 0, "dropped": 0})
    for cik, rs in by.items():
        rs.sort(key=lambda r: (r["period"], r["filed"]))
        cur = rs[-1]
        if (newest - cur["period"]).days > STALE_DAYS:
            continue
        prev = next((r for r in reversed(rs) if r["period"] < cur["period"]), None)
        for cu, (sh, val) in cur["pos"].items():
            o = out[cu]
            if not plausible(sh, val, cprice.get(cu, 0)):
                o["dropped"] += sh > 0
                continue
            o["shares"] += sh
            o["filers"] += 1
            pv = prev["pos"].get(cu) if prev else None
            if prev is None or (pv and not plausible(pv[0], pv[1], cprice.get(cu, 0))):
                chg = None
            else:
                chg = sh - (pv[0] if pv else 0.0)
            o["rows"].append({"name": norm_holder(cur["name"]), "raw": pretty(cur["name"]),
                              "cik": cik, "date": cur["period"].strftime("%m/%d/%Y"),
                              "shares": sh, "chg": chg})
    for o in out.values():
        o["rows"].sort(key=lambda r: -r["shares"])
    return out


# ---------------------------------------------------------------- Form 4
def rel_text(director, officer, ten, other, title):
    if officer and title:
        return title.strip()
    if officer:
        return "Officer"
    if director:
        return "Director"
    if ten:
        return "10% Owner"
    return "Other" if other else ""


def load_f4_bulk(url, ciks, since):
    """One insider data set -> list of filings in the common shape."""
    p = cached("f345/" + url.split("/")[-1], SEC + url)
    with zipfile.ZipFile(p) as z:
        rd = lambda f, cols=None: pd.read_csv(z.open(f), sep="\t", dtype=str, usecols=cols,
                                              keep_default_na=False)
        sub = rd("SUBMISSION.tsv", ["ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK"])
        sub = sub[(sub.DOCUMENT_TYPE == "4") & sub.ISSUERCIK.astype(int).isin(ciks)]
        accs = set(sub.ACCESSION_NUMBER)
        own = rd("REPORTINGOWNER.tsv", ["ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME",
                                        "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"])
        own = own[own.ACCESSION_NUMBER.isin(accs)]
        co = own.groupby("ACCESSION_NUMBER").RPTOWNERCIK.apply(lambda x: sorted({int(c or 0) for c in x}))
        own = own.drop_duplicates("ACCESSION_NUMBER")
        tr = rd("NONDERIV_TRANS.tsv")
        tr = tr[tr.ACCESSION_NUMBER.isin(accs)]
        tr = tr.assign(k=pd.to_numeric(tr.NONDERIV_TRANS_SK)).sort_values("k")
        ho = rd("NONDERIV_HOLDING.tsv")
        ho = ho[ho.ACCESSION_NUMBER.isin(accs)]
        ho = ho.assign(k=pd.to_numeric(ho.NONDERIV_HOLDING_SK)).sort_values("k")
    owners = {r.ACCESSION_NUMBER: r for r in own.itertuples()}
    trs, hos = defaultdict(list), defaultdict(list)
    for r in tr.itertuples():
        trs[r.ACCESSION_NUMBER].append(r)
    for r in ho.itertuples():
        hos[r.ACCESSION_NUMBER].append(r)
    out = []
    for s in sub.itertuples():
        filed = sec_date(s.FILING_DATE)
        o = owners.get(s.ACCESSION_NUMBER)
        if not o or not filed or filed < since:
            continue
        rl = o.RPTOWNER_RELATIONSHIP or ""
        out.append({
            "acc": s.ACCESSION_NUMBER, "issuer": int(s.ISSUERCIK), "filed": filed.strftime("%Y-%m-%d"),
            "owner": pretty(o.RPTOWNERNAME), "ocik": int(o.RPTOWNERCIK or 0),
            "co": co.get(s.ACCESSION_NUMBER, []),
            "rel": rel_text("Director" in rl, "Officer" in rl, "TenPercent" in rl, "Other" in rl,
                            o.RPTOWNER_TITLE),
            "rows": [{"title": r.SECURITY_TITLE, "date": (sec_date(r.TRANS_DATE) or filed).strftime("%Y-%m-%d"),
                      "code": r.TRANS_CODE, "shares": num(r.TRANS_SHARES), "price": num(r.TRANS_PRICEPERSHARE),
                      "post": num(r.SHRS_OWND_FOLWNG_TRANS), "di": r.DIRECT_INDIRECT_OWNERSHIP,
                      "nature": r.NATURE_OF_OWNERSHIP} for r in trs[s.ACCESSION_NUMBER]]
                    + [{"title": r.SECURITY_TITLE, "date": None, "code": None, "shares": 0, "price": 0,
                        "post": num(r.SHRS_OWND_FOLWNG_TRANS), "di": r.DIRECT_INDIRECT_OWNERSHIP,
                        "nature": r.NATURE_OF_OWNERSHIP} for r in hos[s.ACCESSION_NUMBER]],
        })
    print(f"  Form 4 set {url.split('/')[-1]}: {len(out)} filings for our issuers", flush=True)
    return out


def xv(el, path):
    """Text of <path>/value or <path> under el."""
    if el is None:
        return ""
    n = el.find(path + "/value")
    if n is None:
        n = el.find(path)
    return (n.text or "").strip() if n is not None and n.text else ""


def load_f4_filing(row):
    """One Form 4 full submission -> filing dict. Cached."""
    form, name, cik, filed, path = row
    acc = path.split("/")[-1].replace(".txt", "")
    p = EC / "f4_new" / f"{acc}.json"
    if p.exists():
        d = json.loads(p.read_text())
        if d.get("ver") == 2:  # ver 2 keeps all co-filers
            return d
    txt = get(f"{SEC}/Archives/{path}")
    m = re.search(r"<ownershipDocument>.*?</ownershipDocument>", txt, re.S)
    if not m:
        return None
    doc = ET.fromstring(m.group(0))
    ro = doc.find("reportingOwner")
    rr = ro.find("reportingOwnerRelationship") if ro is not None else None
    yes = lambda t: xv(rr, t).lower() in ("1", "true")
    rows = []
    for tag, is_tr in (("nonDerivativeTable/nonDerivativeTransaction", True),
                       ("nonDerivativeTable/nonDerivativeHolding", False)):
        for e in doc.findall(tag):
            rows.append({
                "title": xv(e, "securityTitle"),
                "date": xv(e, "transactionDate")[:10] or None if is_tr else None,
                "code": xv(e, "transactionCoding/transactionCode") if is_tr else None,
                "shares": num(xv(e, "transactionAmounts/transactionShares")),
                "price": num(xv(e, "transactionAmounts/transactionPricePerShare")),
                "post": num(xv(e, "postTransactionAmounts/sharesOwnedFollowingTransaction")),
                "di": xv(e, "ownershipNature/directOrIndirectOwnership"),
                "nature": xv(e, "ownershipNature/natureOfOwnership"),
            })
    co = sorted({int(xv(e, "reportingOwnerId/rptOwnerCik") or 0) for e in doc.findall("reportingOwner")})
    d = {"ver": 2, "co": co, "acc": acc, "issuer": int(xv(doc, "issuer/issuerCik") or cik), "filed": filed,
         "owner": pretty(xv(ro, "reportingOwnerId/rptOwnerName")),
         "ocik": int(xv(ro, "reportingOwnerId/rptOwnerCik") or 0),
         "rel": rel_text(yes("isDirector"), yes("isOfficer"), yes("isTenPercentOwner"), yes("isOther"),
                         xv(rr, "officerTitle")),
         "rows": rows}
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(d))
    return d


def insiders(ciks):
    """{issuer cik: [filing, ...]} for the INSIDER_DAYS window."""
    since = datetime.now() - timedelta(days=INSIDER_DAYS)
    links = dataset_links("insider-transactions-data-sets", r"\d{4}q\d_form345\.zip")
    yq = lambda u: tuple(map(int, re.search(r"(\d{4})q(\d)_", u).groups()))
    links = sorted([u for u in links if yq(u) >= (since.year, (since.month - 1) // 3 + 1)], key=yq)
    fil = []
    for u in links:
        fil += load_f4_bulk(u, ciks, since)
    y, q = yq(links[-1])
    after = datetime(y + (q == 4), 1 if q == 4 else 3 * q + 1, 1)  # first day after that quarter
    rows = [r for yy, qq in quarters_since(after) for r in form_idx(yy, qq)
            if r[0] == "4" and int(r[2]) in ciks]
    print(f"  Form 4 filed since {after:%Y-%m-%d} for our issuers: {len(rows)}", flush=True)
    done = [0]

    def one(r):
        try:
            return load_f4_filing(r)
        except Exception as e:
            print(f"  ! Form 4 {r[4]}: {e}", flush=True)
            return None
        finally:
            done[0] += 1
            if done[0] % 500 == 0:
                print(f"    Form 4 filings {done[0]}/{len(rows)}", flush=True)
    with ThreadPoolExecutor(WORKERS) as ex:
        fil += [f for f in ex.map(one, rows) if f]
    by = defaultdict(list)
    seen = set()
    for f in fil:
        if f["acc"] not in seen:
            seen.add(f["acc"])
            by[f["issuer"]].append(f)
    return by


def class_tag(title):
    m = re.search(r"class\s+([a-z])\b", (title or "").lower())
    return m.group(1) if m else ""


ACCT_DAYS = 120   # an account not reported this close to the owner's newest filing is stale
TWIN_TOL = 0.005  # joint filers: owners whose totals agree this closely are one holder


def insider_view(filings, price):
    """Filings for one issuer -> (trades, ins) in the page's shape.

    Holdings: for each owner, the last 'shares owned following' per
    (share class, direct / each named indirect account), in filing order.
    Accounts last reported more than ACCT_DAYS before the owner's newest
    filing are dropped as stale. Joint filers (same holding reported by
    several entities) are merged into the one with the most filings.
    Trades: open-market buys (P) and sells (S), one row per owner, day
    and side. A price more than 5x away from today's price is a filing
    error: such a trade is valued at today's price and marked "est".
    """
    filings = sorted(filings, key=lambda f: (f["filed"], f["acc"]))
    # joint filers: owners that appear together on one filing are one holder
    parent = {}
    def root(x):
        while parent.get(x, x) != x:
            x = parent[x]
        return x
    nf = defaultdict(int)
    for f in filings:
        nf[f["ocik"] or f["owner"]] += 1
    for f in filings:
        co = [c for c in f.get("co", []) if c] or [f["ocik"] or f["owner"]]
        a = root(co[0])
        for c in co[1:]:
            b = root(c)
            if a != b:
                parent[b] = a
    groups = defaultdict(list)
    for k in nf:
        groups[root(k)].append(k)
    head = {}
    for r, ks in groups.items():
        h = max(ks, key=lambda k: nf[k])
        for k in ks:
            head[k] = h

    pos, raw = {}, []
    cutoff = (datetime.now() - timedelta(days=INSIDER_DAYS)).strftime("%Y-%m-%d")
    for f in filings:
        key = head.get(f["ocik"] or f["owner"], f["ocik"] or f["owner"])
        mine = key == (f["ocik"] or f["owner"])
        p = pos.setdefault(key, {"n": f["owner"], "rel": f["rel"], "ocik": f["ocik"],
                                 "last": f["filed"], "acct": {}, "nf": 0, "names": set()})
        p["names"].add(f["owner"])
        if mine:
            p.update(n=f["owner"] or p["n"], rel=f["rel"] or p["rel"], ocik=f["ocik"])
        p["last"] = f["filed"]
        p["nf"] += 1
        for r in f["rows"]:
            ind = (r["di"] or "D").upper().startswith("I")
            nat = re.sub(r"[^a-z0-9 ]", "", (r["nature"] or "").lower()).strip() if ind else ""
            p["acct"][(class_tag(r["title"]), "I" if ind else "D", nat)] = (r["post"], f["filed"])
            if r["code"] in ("P", "S") and r["date"] and r["date"] >= cutoff and r["shares"] > 0:
                raw.append((key, r["date"], r["code"], r["shares"], r["price"], f["acc"]))

    # holdings = the newest filing's accounts, plus recent older accounts that do not
    # repeat one of them (filers sometimes drop the account name, so one trust shows twice)
    for p in pos.values():
        snap = max((d for v, d in p["acct"].values()), default=p["last"])
        old = (datetime.strptime(snap, "%Y-%m-%d") - timedelta(days=ACCT_DAYS)).strftime("%Y-%m-%d")
        live = {k: v for k, (v, d) in p["acct"].items() if d == snap}
        for k, (v, d) in p["acct"].items():
            if k in live or d < old or v <= 0:
                continue
            if k[1] == "D" and any(kk[1] == "D" and kk[0] == k[0] for kk in live):
                continue
            if any(abs(v - lv) <= 0.02 * max(v, lv) for lv in live.values()):
                continue
            live[k] = v
        p["d"] = sum(v for k, v in live.items() if k[1] == "D")
        p["i"] = sum(v for k, v in live.items() if k[1] == "I")
        p["accts"] = sum(1 for k in live if k[1] == "I")

    # joint filers that never co-filed but report the same total: keep the busiest
    alias = {}
    big = sorted((p for p in pos.values() if p["d"] + p["i"] > 1e6), key=lambda p: -p["nf"])
    for i, a in enumerate(big):
        if id(a) in alias:
            continue
        ta = a["d"] + a["i"]
        for b in big[i + 1:]:
            if id(b) not in alias and abs(b["d"] + b["i"] - ta) <= TWIN_TOL * ta:
                alias[id(b)] = a
                a["names"] |= b["names"]
    keep = {k: p for k, p in pos.items() if id(p) not in alias}
    twin_of = {k: alias[id(p)] for k, p in pos.items() if id(p) in alias}

    agg, seen = {}, {}
    for key, date, code, sh, px, acc in raw:
        if key in twin_of:  # the same trade filed again by a joint filer
            continue
        first = seen.setdefault((key, date, code, sh, px), acc)
        if first != acc:  # same trade, filed again by a joint filer
            continue
        est = not price or not px or not (price / 5 <= px <= price * 5)
        if px and est and price:
            px = price
        k = (key, date, code)
        a = agg.setdefault(k, {"insider": keep[key]["n"], "date": date,
                               "type": "Buy" if code == "P" else "Sell",
                               "kind": "buy" if code == "P" else "sell",
                               "shares": 0.0, "value": 0.0, "est": False})
        a["shares"] += sh
        a["value"] += sh * (px or 0)
        a["est"] = a["est"] or est
    trades = []
    for a in agg.values():
        a["price"] = round(a["value"] / a["shares"], 2) if a["shares"] else 0
        a["shares"], a["value"] = round(a["shares"]), round(a["value"])
        if not a["est"]:
            del a["est"]
        trades.append(a)
    trades.sort(key=lambda t: t["date"], reverse=True)

    ins = []
    for k, p in keep.items():
        if p["d"] + p["i"] <= 0 and not any(t["insider"] == p["n"] for t in trades):
            continue
        url = (f"{SEC}/cgi-bin/browse-edgar?action=getcompany&CIK={p['ocik']:010d}&type=4"
               if p["ocik"] else "")
        tw = sorted(p["names"] - {p["n"]})
        ins.append({"n": p["n"], "rel": p["rel"], "url": url, "last": p["last"],
                    "d": p["d"], "i": p["i"], "accts": p["accts"], **({"also": tw} if tw else {})})
    return trades, ins


# ---------------------------------------------------------------- shares, prices
def shares_out(cik):
    """Latest undimensioned share count from XBRL (dei, then us-gaap)."""
    for concept in ("dei/EntityCommonStockSharesOutstanding", "us-gaap/CommonStockSharesOutstanding",
                    "us-gaap/WeightedAverageNumberOfSharesOutstandingBasic"):
        try:
            d = json.loads(get(f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/{concept}.json"))
        except Exception:
            continue
        facts = [f for u in d["units"].values() for f in u if f.get("val")]
        if not facts:
            continue
        end = max(f["end"] for f in facts)
        if end < (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d"):
            continue
        latest = [f for f in facts if f["end"] == end]
        newest = max(latest, key=lambda f: f.get("filed", ""))
        return float(newest["val"]), end, concept.split("/")[1]
    return 0.0, None, None


def class_shares(cik):
    """{ticker: shares} per share class, from the newest 10-Q / 10-K cover page.

    The cover tags each class's trading symbol and shares outstanding with
    the same class member (inline XBRL), so the two can be joined.
    """
    sub = json.loads(get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json"))["filings"]["recent"]
    i = next(i for i, f in enumerate(sub["form"]) if f in ("10-Q", "10-K"))
    acc = sub["accessionNumber"][i].replace("-", "")
    html = get(f"{SEC}/Archives/edgar/data/{cik}/{acc}/{sub['primaryDocument'][i]}")
    ctx = {}
    for cid, body in re.findall(r'<xbrli:context id="([^"]+)">(.*?)</xbrli:context>', html, re.S):
        ctx[cid] = tuple(sorted(re.findall(r"<xbrldi:explicitMember[^>]*>([^<]+)<", body)))
    attr = lambda tag, a: (re.search(rf'{a}="([^"]*)"', tag) or [None, None])[1]
    sym, shr = {}, defaultdict(float)
    for tag, val in re.findall(r'(<ix:nonNumeric[^>]*name="dei:TradingSymbol"[^>]*>)(.*?)</ix:nonNumeric>', html, re.S):
        sym[ctx.get(attr(tag, "contextRef"), ())] = re.sub(r"<[^>]+>|\W", "", val).upper()
    for tag, val in re.findall(r'(<ix:nonFraction[^>]*name="dei:EntityCommonStockSharesOutstanding"[^>]*>)(.*?)</ix:nonFraction>', html, re.S):
        v = num(re.sub(r"<[^>]+>", "", val)) * 10 ** int(attr(tag, "scale") or 0)
        shr[ctx.get(attr(tag, "contextRef"), ())] += v
    out = {sym[m]: v for m, v in shr.items() if m in sym and v > 0}
    # one listed class, symbol tagged without a class (DELL, META, NKE): the
    # "Security12bTitle" text names the class ("Class C Common Stock")
    if not out and len(sym) == 1 and () in sym and len(shr) > 1:
        title = re.search(r'<ix:nonNumeric[^>]*name="dei:Security12bTitle"[^>]*>(.*?)</ix:nonNumeric>', html, re.S)
        letter = re.search(r"class\s+([a-z])\b", re.sub(r"<[^>]+>", " ", title.group(1)).lower()) if title else None
        for m, v in shr.items():
            ml = re.search(r"Class([A-Z])Member", " ".join(m))
            if (letter and ml and ml.group(1).lower() == letter.group(1)) or (not letter and m and not ml):
                out[sym[()]] = v
    return out


def prices(tickers):
    import yfinance as yf
    ys = [t.replace(".", "-") for t in tickers]
    df = yf.download(ys, period="7d", auto_adjust=False, progress=False, threads=True)["Close"]
    last = df.ffill().iloc[-1]
    return {t: float(last.get(y, float("nan"))) for t, y in zip(tickers, ys)}


# ---------------------------------------------------------------- main
def main():
    args = [a.upper() for a in sys.argv[1:] if not a.startswith("--")]
    EC.mkdir(exist_ok=True)
    t0 = time.time()
    members = {t: [] for t in args} if args else index_members()
    cikmap, order = company_tickers()
    tickers = [t for t in sorted(members) if t in cikmap]
    missing = sorted(set(members) - set(tickers))
    if missing:
        print(f"  ! no SEC CIK for: {' '.join(missing)}")
    # several tickers can share one CIK (GOOG/GOOGL): insiders go to the first one SEC lists
    rank = {t: i for i, t in enumerate(order)}
    ins_owner = {}
    for t in sorted(tickers, key=lambda t: rank.get(t, 1e9)):
        ins_owner.setdefault(cikmap[t][0], t)
    ciks = {cikmap[t][0] for t in tickers}
    print(f"{len(tickers)} tickers, {len(ciks)} issuers", flush=True)

    cmap = cusip_map(tickers)
    by_t = defaultdict(set)
    for cu, t in cmap.items():
        by_t[t].add(cu)
    nocusip = [t for t in tickers if t not in by_t]
    print(f"  CUSIPs for {len(by_t)} tickers; none for: {' '.join(nocusip) or '-'}", flush=True)

    px = prices(tickers)
    inst = institutions(set(cmap), {cu: px.get(t) or 0 for cu, t in cmap.items()})
    print(f"  13F done {time.time()-t0:.0f}s", flush=True)
    f4 = insiders(ciks)
    print(f"  Form 4 done {time.time()-t0:.0f}s", flush=True)
    with ThreadPoolExecutor(WORKERS) as ex:
        so = dict(zip(ciks, ex.map(shares_out, ciks)))
    # SEC XBRL has no per-class count (BRK.B) and only a company total where two
    # tickers share one CIK (GOOG/GOOGL). For those, take the class count from Yahoo.
    shared = defaultdict(list)
    for t in tickers:
        shared[cikmap[t][0]].append(t)
    so_t = {t: so[cikmap[t][0]] for t in tickers}
    # a class ticker (BF.B) or a fallback concept (no plain dei count) also means classes
    fix = [t for t in tickers if not so_t[t][0] or len(shared[cikmap[t][0]]) > 1 or "." in t
           or so_t[t][2] != "EntityCommonStockSharesOutstanding"]
    if fix:
        import yfinance as yf
        cls = {}
        for c in {cikmap[t][0] for t in fix}:
            try:
                cls.update(class_shares(c))
            except Exception as e:
                print(f"  ! class shares CIK {c}: {e}")
        for t in list(fix):
            n = cls.get(re.sub(r"\W", "", t))
            if n and n >= 0.05 * (so_t[t][0] or 0):  # a tiny class (STZ "Class 1") is the wrong match
                so_t[t] = (n, datetime.now().strftime("%Y-%m-%d"), "10-Q cover")
                fix.remove(t)
        print(f"  class share counts from the 10-Q/10-K cover: {len(cls)} classes", flush=True)
        for t in [t for t in fix if not so_t[t][0] or len(shared[cikmap[t][0]]) > 1 or "." in t]:
            try:
                n = float(yf.Ticker(t.replace(".", "-")).fast_info["shares"] or 0)
                if n:
                    so_t[t] = (n, datetime.now().strftime("%Y-%m-%d"), "yahoo")
            except Exception as e:
                print(f"  ! shares {t}: {e}")
        print(f"  share counts from Yahoo: {' '.join(t for t in tickers if so_t[t][2] == 'yahoo') or '-'}", flush=True)
    print(f"  prices + shares done {time.time()-t0:.0f}s", flush=True)

    out = {"asOf": datetime.now().strftime("%Y-%m-%d %H:%M"), "source": "SEC EDGAR", "companies": []}
    for t in tickers:
        cik, title = cikmap[t]
        price = px.get(t) or 0.0
        price = 0.0 if price != price else price  # NaN
        sh_out, so_date, so_src = so_t[t]
        rows, tot, nf = [], 0.0, 0
        for cu in by_t.get(t, ()):
            o = inst.get(cu)
            if o:
                rows += o["rows"]
                tot += o["shares"]
                nf += o["filers"]
        if tot > 1.5 * sh_out:  # funds cannot hold more than exist: the count is wrong
            try:
                import yfinance as yf
                n = float(yf.Ticker(t.replace(".", "-")).fast_info["shares"] or 0)
                if n > sh_out:
                    print(f"  ! {t}: 13F holds {tot:,.0f} > count {sh_out:,.0f} ({so_src}); Yahoo {n:,.0f}")
                    sh_out, so_date, so_src = n, datetime.now().strftime("%Y-%m-%d"), "yahoo"
            except Exception as e:
                print(f"  ! shares {t}: {e}")
        merged = defaultdict(lambda: None)
        for r in rows:  # one row per filer even if it holds 2 CUSIPs of the ticker
            m = merged[r["cik"]]
            if m is None:
                merged[r["cik"]] = dict(r)
            else:
                m["shares"] += r["shares"]
                m["chg"] = None if m["chg"] is None or r["chg"] is None else m["chg"] + r["chg"]
        rows = sorted(merged.values(), key=lambda r: -r["shares"])[:TOP_HOLDERS]
        holders = [{"name": r["name"], "raw": r["raw"], "date": r["date"], "shares": r["shares"],
                    "chg": r["chg"] if r["chg"] is not None else 0,
                    "chgPct": (100 * r["chg"] / (r["shares"] - r["chg"])
                               if r["chg"] not in (None, 0) and r["shares"] - r["chg"] > 0 else 0),
                    "chgKnown": r["chg"] is not None,
                    "value": r["shares"] * price,
                    "pct": 100 * r["shares"] / sh_out if sh_out else 0} for r in rows]
        trades, ins = insider_view(f4.get(cik, []), price) if ins_owner[cik] == t else ([], [])
        out["companies"].append({
            "ticker": t, "name": title, "cik": cik, "price": price, "sharesOut": sh_out,
            "sharesAsOf": so_date, "sharesSrc": so_src,
            "instPct": 100 * tot / sh_out if sh_out else 0, "instValue": tot * price,
            "mktCap": price * sh_out, "holderCount": nf, "holders": holders,
            "trades": trades, "ins": ins, "insOn": ins_owner[cik], "idx": members[t],
        })
    out["companies"].sort(key=lambda c: -c["mktCap"])
    js = "window.MM_DATA = " + json.dumps(out, separators=(",", ":"), default=lambda o: o.item()) + ";\n"
    (HERE / "data.js").write_text(js, encoding="utf-8")
    bad = [c["ticker"] for c in out["companies"] if not c["price"] or not c["sharesOut"] or not c["holders"]]
    print(f"wrote data.js ({len(js)/1024:.0f} KB), {len(out['companies'])} companies, "
          f"{time.time()-t0:.0f}s. Missing price/shares/holders: {' '.join(bad) or '-'}")


if __name__ == "__main__":
    main()
