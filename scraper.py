#!/usr/bin/env python3
"""
Retail price scraper.

Runs Apify actors listed in config.json, saves every result row to a database,
and reports price changes vs. the previous run.

Database: uses Postgres when DATABASE_URL is set (production / Replit),
otherwise a local SQLite file (laptop testing).

Alerts: if NTFY_TOPIC is set, price changes and job failures are pushed via ntfy.

Usage:
    python scraper.py stores "Odessa, TX"      # find Dollar General store numbers
    python scraper.py run                      # run every job in config.json
    python scraper.py run --job dg_beer        # run one job
    python scraper.py changes                  # re-print changes from the latest run
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from apify_client import ApifyClient
from dotenv import load_dotenv

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.json"

# The actors return different field names, so we try several for each value.
# The raw JSON is always saved too, so nothing is lost if a name is missed.
ID_KEYS = ["upc", "UPC", "usItemId", "productId", "sku", "id", "itemId"]
TITLE_KEYS = ["name", "title", "productName", "description"]
PRICE_KEYS = ["price", "currentPrice", "salePrice", "sale_price", "regularPrice"]
WAS_KEYS = ["wasPrice", "originalPrice", "was_price", "regular_price", "listPrice"]
STORE_KEYS = ["storeId", "store_id", "storeNumber", "store_number"]


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def pick(item, keys):
    for k in keys:
        if k in item and item[k] not in (None, ""):
            return item[k]
    return None


def parse_price(value):
    """Handle 3.99, '3.99', '$3.99', or {'price': 3.99}."""
    if value is None:
        return None
    if isinstance(value, dict):
        for k in ("price", "amount", "value", "current"):
            if k in value:
                return parse_price(value[k])
        return None
    if isinstance(value, (int, float)):
        return float(value)
    m = re.search(r"\d+(?:,\d{3})*(?:\.\d+)?", str(value))
    return float(m.group(0).replace(",", "")) if m else None


class DB:
    """Thin wrapper so the same SQL runs on Postgres and SQLite."""

    def __init__(self, sqlite_path):
        url = os.environ.get("DATABASE_URL")
        if url:
            import psycopg2

            self.kind = "postgres"
            self.conn = psycopg2.connect(url)
            self.ph = "%s"
            pk = "BIGSERIAL PRIMARY KEY"
        else:
            p = ROOT / sqlite_path
            p.parent.mkdir(parents=True, exist_ok=True)
            self.kind = "sqlite"
            self.conn = sqlite3.connect(p)
            self.ph = "?"
            pk = "INTEGER PRIMARY KEY AUTOINCREMENT"
        self.execute(
            f"""CREATE TABLE IF NOT EXISTS prices (
                id {pk},
                run_ts TEXT NOT NULL,
                job TEXT NOT NULL,
                retailer TEXT NOT NULL,
                store_id TEXT,
                item_key TEXT,
                title TEXT,
                price DOUBLE PRECISION,
                was_price DOUBLE PRECISION,
                raw TEXT
            )"""
        )
        self.execute(
            "CREATE INDEX IF NOT EXISTS idx_item ON prices (retailer, store_id, item_key, run_ts)"
        )
        self.commit()

    def execute(self, sql, params=()):
        cur = self.conn.cursor()
        cur.execute(sql.replace("?", self.ph), params)
        return cur

    def commit(self):
        self.conn.commit()


def notify(title, lines):
    """Push a notification via ntfy if NTFY_TOPIC is set. Never raises."""
    topic = os.environ.get("NTFY_TOPIC")
    if not topic or not lines:
        return
    body = "\n".join(lines[:20])
    if len(lines) > 20:
        body += f"\n...and {len(lines) - 20} more"
    try:
        req = urllib.request.Request(
            f"https://ntfy.sh/{topic}",
            data=body.encode("utf-8"),
            headers={"Title": title},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:  # alerts must never break the run
        print(f"(ntfy alert failed: {e})")


def check_placeholders(job):
    for k, v in job["input"].items():
        if v == "REPLACE_ME":
            raise ValueError(
                f"Job '{job['name']}' still has REPLACE_ME for '{k}' in config.json. "
                "Fill it in first (use the 'stores' command to find store numbers)."
            )


def run_actor(client, actor, run_input):
    run = client.actor(actor).call(run_input=run_input)
    return list(client.dataset(run["defaultDatasetId"]).iterate_items())


def run_job(client, db, job, run_ts):
    check_placeholders(job)
    pages = job.get("pages", 1)
    rows = []
    for page in range(1, pages + 1):
        run_input = dict(job["input"])
        if pages > 1:
            run_input["page"] = page
        print(f"[{job['name']}] page {page}/{pages} ...")
        batch = run_actor(client, job["actor"], run_input)
        rows.extend(batch)
        if not batch:
            break

    if os.environ.get("SAVE_RAW", "1") != "0":
        raw_dir = ROOT / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        safe_ts = run_ts.replace(":", "-")
        (raw_dir / f"{safe_ts}_{job['name']}.json").write_text(json.dumps(rows, indent=2))

    saved = 0
    for item in rows:
        if item.get("resultType") == "error":
            print(f"[{job['name']}] actor error row: {item}")
            continue
        db.execute(
            """INSERT INTO prices
               (run_ts, job, retailer, store_id, item_key, title, price, was_price, raw)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                run_ts,
                job["name"],
                job["retailer"],
                str(pick(item, STORE_KEYS) or job["input"].get("store_id") or ""),
                str(pick(item, ID_KEYS) or ""),
                pick(item, TITLE_KEYS),
                parse_price(pick(item, PRICE_KEYS)),
                parse_price(pick(item, WAS_KEYS)),
                json.dumps(item),
            ),
        )
        saved += 1
    db.commit()
    print(f"[{job['name']}] {len(rows)} rows returned, {saved} saved")
    return rows


def find_changes(db, run_ts, threshold_pct):
    """Return (prev_run_ts, list of change lines, count of new items)."""
    prev = db.execute(
        "SELECT MAX(run_ts) FROM prices WHERE run_ts < ?", (run_ts,)
    ).fetchone()[0]
    if not prev:
        return None, [], 0
    cur = db.execute(
        """SELECT DISTINCT c.retailer, c.store_id, c.title, p.price, c.price
           FROM prices c
           JOIN prices p ON p.retailer=c.retailer AND p.store_id=c.store_id
                        AND p.item_key=c.item_key AND p.run_ts=?
           WHERE c.run_ts=? AND c.item_key != ''
             AND c.price IS NOT NULL AND p.price IS NOT NULL AND c.price != p.price""",
        (prev, run_ts),
    ).fetchall()
    lines = []
    for retailer, store, title, old, new in cur:
        pct = (new - old) / old * 100 if old else 0
        if abs(pct) >= threshold_pct:
            lines.append(
                f"{retailer} #{store}: {title} ${old:.2f} -> ${new:.2f} ({pct:+.1f}%)"
            )
    new_items = db.execute(
        """SELECT COUNT(DISTINCT c.retailer || '|' || c.store_id || '|' || c.item_key)
           FROM prices c WHERE c.run_ts=? AND c.item_key != '' AND NOT EXISTS (
             SELECT 1 FROM prices p WHERE p.run_ts=? AND p.retailer=c.retailer
               AND p.store_id=c.store_id AND p.item_key=c.item_key)""",
        (run_ts, prev),
    ).fetchone()[0]
    return prev, lines, new_items


def report_changes(db, run_ts, threshold_pct, alert=False):
    prev, lines, new_items = find_changes(db, run_ts, threshold_pct)
    if not prev:
        print("\nNo earlier run to compare against yet.")
        return
    print(f"\nPrice changes vs {prev}:")
    for line in lines or ["none above threshold"]:
        print(f"  {line}")
    print(f"  new items since last run: {new_items}")
    if alert and lines:
        notify(f"{len(lines)} price change(s)", lines)


def cmd_stores(client, query):
    cfg = load_config()
    actor = cfg["store_finder"]["actor"]
    rows = run_actor(client, actor, {"operation": "stores", "query": query, "limit": 20})
    print(json.dumps(rows, indent=2))


def cmd_run(client, only_job):
    cfg = load_config()
    db = DB(cfg["database"])
    print(f"Database: {db.kind}")
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    jobs = [j for j in cfg["jobs"] if not only_job or j["name"] == only_job]
    if not jobs:
        sys.exit(f"No job named '{only_job}' in config.json")
    failures = []
    for job in jobs:
        try:
            run_job(client, db, job, run_ts)
        except Exception as e:
            db.conn.rollback()
            msg = f"{job['name']}: {e}"
            print(f"[FAILED] {msg}")
            failures.append(msg)
    report_changes(db, run_ts, cfg.get("alert_threshold_pct", 1.0), alert=True)
    if failures:
        notify("Price scraper: job failures", failures)
        sys.exit(1)


def cmd_changes():
    cfg = load_config()
    db = DB(cfg["database"])
    latest = db.execute("SELECT MAX(run_ts) FROM prices").fetchone()[0]
    if not latest:
        sys.exit("Database is empty. Run the scraper first.")
    report_changes(db, latest, cfg.get("alert_threshold_pct", 1.0))


def main():
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_stores = sub.add_parser("stores")
    p_stores.add_argument("query", help='e.g. "Odessa, TX"')
    p_run = sub.add_parser("run")
    p_run.add_argument("--job")
    sub.add_parser("changes")
    args = parser.parse_args()

    if args.cmd == "changes":
        return cmd_changes()

    token = os.environ.get("APIFY_TOKEN")
    if not token:
        sys.exit("APIFY_TOKEN is missing. Set it in .env (local) or Secrets (Replit).")
    client = ApifyClient(token)

    if args.cmd == "stores":
        cmd_stores(client, args.query)
    elif args.cmd == "run":
        cmd_run(client, args.job)


if __name__ == "__main__":
    main()
