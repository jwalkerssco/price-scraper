# price-scraper

Daily retailer price tracking: runs Apify actors for Dollar General and Walmart,
stores results in a database, and pushes an alert when prices change.

- Local testing: leave DATABASE_URL blank and it uses a SQLite file in data/.
- Production (Replit): set DATABASE_URL to a hosted Postgres and it uses that.

## Local setup
    python -m venv .venv
    # Windows: .venv\Scripts\activate    Mac/Linux: source .venv/bin/activate
    pip install -r requirements.txt
    cp .env.example .env      # paste your Apify token into .env

## Commands
    python scraper.py stores "Odessa, TX"   # find Dollar General store numbers
    # put a store number into config.json (replace REPLACE_ME)
    python scraper.py run                   # run all jobs
    python scraper.py run --job wmt_beer    # run one job
    python scraper.py changes               # show changes from latest run

## Deploy to Replit (scheduled, cloud only)
1. Create a free hosted Postgres (Neon is simplest; Supabase also works, but use
   its pooler connection string). Copy the connection string.
2. In Replit, import this GitHub repo as a new Replit App.
3. Add Secrets: APIFY_TOKEN, DATABASE_URL, NTFY_TOPIC (optional), SAVE_RAW=0.
4. In the shell, run `pip install -r requirements.txt` and then
   `python scraper.py run --job wmt_beer` to confirm it connects and saves rows.
5. Publish -> Scheduled deployment:
   - Build command: pip install -r requirements.txt
   - Run command:   python scraper.py run
   - Schedule: e.g. "every day at 6am Central"
   - Add the same Secrets to the deployment (deployment secrets are separate).
6. Subscribe to your NTFY_TOPIC in the ntfy phone app to receive alerts.

Nothing in data/ or .env is committed to git.
