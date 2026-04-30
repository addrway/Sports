#!/usr/bin/env python3
"""
BetsAPI Table Tennis Scraper

Default scrape:
- Date: 2026-03-28
- Pages: 1 through 11
- Output CSV columns:
  League/Source, Date/Time, Match, Result/Score, Page URL

Run headed/live browser mode:
python betsapi_table_tennis_scraper.py --date-from 2026-03-28 --date-to 2026-03-28 --start-page 1 --end-page 11 --headed --output betsapi_table_tennis_2026-03-28_p1-p11.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

BASE_URL = "https://hu.betsapi.com/cf/table-tennis"
BLOCK_MARKERS = (
    "captcha",
    "cloudflare",
    "access denied",
    "forbidden",
    "verify you are human",
    "checking your browser",
    "just a moment",
)


@dataclass
class MatchRow:
    league_source: str
    date_time: str
    match: str
    result_score: str
    page_url: str


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build_page_url(day: date, page_num: int) -> str:
    date_part = day.strftime("%Y-%m-%d")
    if page_num == 1:
        return f"{BASE_URL}/{date_part}/"
    return f"{BASE_URL}/{date_part}/p.{page_num}"


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def looks_blocked(text: str) -> bool:
    lower = (text or "").lower()
    return any(marker in lower for marker in BLOCK_MARKERS)


async def wait_for_manual_unblock(page: Page, url: str, seconds: int) -> bool:
    """Give the user time to solve CAPTCHA/login in headed browser mode."""
    if seconds <= 0:
        return False

    print(f"⚠️ Possible access block found on: {url}")
    print(f"   You have {seconds} seconds to solve CAPTCHA/login in the opened browser.")
    try:
        await page.wait_for_timeout(seconds * 1000)
        body_text = await page.locator("body").inner_text(timeout=5000)
        return not looks_blocked(body_text)
    except Exception:
        return False


async def extract_rows_from_page(page: Page, url: str) -> List[MatchRow]:
    """Extract visible match rows from the currently loaded BetsAPI page."""
    rows: List[MatchRow] = []

    # BetsAPI table rows generally render as table tbody tr rows.
    # We keep this broad on purpose because the site markup can shift.
    table_rows = page.locator("table tbody tr")

    try:
        await table_rows.first.wait_for(state="visible", timeout=15000)
    except PlaywrightTimeoutError:
        print(f"   No visible table rows found: {url}")
        return rows

    count = await table_rows.count()

    for i in range(count):
        row = table_rows.nth(i)
        try:
            if not await row.is_visible():
                continue

            cells = row.locator("td")
            cell_count = await cells.count()
            if cell_count < 4:
                continue

            values = [clean_text(await cells.nth(c).inner_text()) for c in range(cell_count)]
            values = [v for v in values if v]

            # Skip navigation, ads, heart icons, and empty/non-match rows.
            joined = " ".join(values).lower()
            if not joined or "advert" in joined or "pagination" in joined:
                continue

            # Expected visible table shape:
            # League | Time | Match | Result | optional icons/details
            league = values[0] if len(values) > 0 else ""
            dt = values[1] if len(values) > 1 else ""
            match = values[2] if len(values) > 2 else ""
            result = ""

            # Score is usually the last short value like 3-1, 0-3, 2-3.
            score_candidates = [v for v in values if re.fullmatch(r"\d+\s*-\s*\d+", v)]
            if score_candidates:
                result = score_candidates[-1].replace(" ", "")
            elif len(values) > 3:
                result = values[3]

            if not league or not dt or not match or not result:
                continue

            # Avoid rows that are not match rows.
            if " v " not in f" {match.lower()} " and " vs " not in f" {match.lower()} ":
                continue

            rows.append(
                MatchRow(
                    league_source=league,
                    date_time=dt,
                    match=match,
                    result_score=result,
                    page_url=url,
                )
            )
        except Exception as exc:
            print(f"   Skipped one row due to parse issue: {exc}")
            continue

    return rows


async def scrape(args: argparse.Namespace) -> List[MatchRow]:
    all_rows: List[MatchRow] = []
    start_day = parse_date(args.date_from)
    end_day = parse_date(args.date_to)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.headed, slow_mo=args.slow_mo)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        for day in date_range(start_day, end_day):
            for page_num in range(args.start_page, args.end_page + 1):
                url = build_page_url(day, page_num)
                print(f"Opening {url}")

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
                    await page.wait_for_timeout(args.page_delay_ms)
                except PlaywrightTimeoutError:
                    print(f"   Timeout loading page, skipping: {url}")
                    continue

                body_text = ""
                try:
                    body_text = await page.locator("body").inner_text(timeout=5000)
                except Exception:
                    pass

                if looks_blocked(body_text):
                    if args.headed and args.manual_unblock_seconds > 0:
                        unblocked = await wait_for_manual_unblock(page, url, args.manual_unblock_seconds)
                        if not unblocked:
                            print("❌ Access still appears blocked. Stopping scraper safely.")
                            break
                    else:
                        print("❌ Access appears blocked. Re-run with --headed to use a live browser.")
                        break

                rows = await extract_rows_from_page(page, url)
                print(f"   Extracted {len(rows)} rows")
                all_rows.extend(rows)
            else:
                continue
            break

        await context.close()
        await browser.close()

    return all_rows


def write_csv(rows: List[MatchRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["League/Source", "Date/Time", "Match", "Result/Score", "Page URL"])
        for row in rows:
            writer.writerow([
                row.league_source,
                row.date_time,
                row.match,
                row.result_score,
                row.page_url,
            ])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape BetsAPI table tennis match rows to CSV.")
    parser.add_argument("--date-from", default="2026-03-28", help="Start date, YYYY-MM-DD")
    parser.add_argument("--date-to", default="2026-03-28", help="End date, YYYY-MM-DD")
    parser.add_argument("--start-page", type=int, default=1, help="First page number to scrape")
    parser.add_argument("--end-page", type=int, default=11, help="Last page number to scrape")
    parser.add_argument("--output", default="betsapi_table_tennis_2026-03-28_p1-p11.csv", help="CSV output path")
    parser.add_argument("--headed", action="store_true", help="Open a live visible browser window")
    parser.add_argument("--slow-mo", type=int, default=100, help="Playwright slow motion delay in ms")
    parser.add_argument("--page-delay-ms", type=int, default=1500, help="Wait after each page load in ms")
    parser.add_argument("--timeout-ms", type=int, default=45000, help="Page load timeout in ms")
    parser.add_argument(
        "--manual-unblock-seconds",
        type=int,
        default=90,
        help="In headed mode, pause this many seconds if CAPTCHA/access block appears",
    )
    return parser


async def main_async() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.start_page < 1:
        raise SystemExit("--start-page must be 1 or greater")
    if args.end_page < args.start_page:
        raise SystemExit("--end-page must be greater than or equal to --start-page")

    rows = await scrape(args)
    write_csv(rows, Path(args.output))
    print(f"✅ Done. Wrote {len(rows)} rows to {args.output}")
    print("Import into Google Sheets DATA_CHANGE tab starting at J1 or J2.")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
