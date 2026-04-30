BetsAPI Table Tennis Scraper

Setup:
pip install -r requirements.txt
python -m playwright install chromium

Run (LIVE browser):
python betsapi_table_tennis_scraper.py --date-from 2026-03-28 --date-to 2026-03-28 --start-page 1 --end-page 11 --headed --output betsapi_table_tennis_2026-03-28_p1-p11.csv

Google Sheets:
Go to DATA_CHANGE tab
Click J1 or J2
File Import Upload CSV
Replace at selected cell

Data fills columns J to N

To expand dates later:
--date-from 2026-03-28 --date-to 2026-04-29
