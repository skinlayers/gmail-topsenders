# gmail-topsenders

Scans your entire Gmail inbox and reports which email addresses have sent you the most messages. Useful for identifying newsletters, notification senders, or contacts to unsubscribe from — and for spotting bulk senders whose emails you can select and delete in one go to reclaim inbox space.

```
--- TOP 50 SENDERS (84312 messages) ---
1. noreply@github.com: 4821 emails (5.7%)
2. notifications@slack.com: 3204 emails (3.8%)
3. no-reply@accounts.google.com: 1847 emails (2.2%)
...
Completed in 4m32s
```

## How it works

The program fetches all message IDs from the Gmail API, then uses a concurrent worker pool to pull the `From` header from each message. It respects Gmail's quota limits (200 QPS by default, configurable via `-qps`) with exponential backoff on errors, then prints the top senders sorted by count.

On first run it prints an authorization URL, waits for you to paste back the code from your browser, and caches the resulting token in `token.json`. Subsequent runs reuse the cached token.

## Prerequisites

- [Go](https://go.dev/dl/) 1.22 or later (go.mod requires 1.26.3)
- A Google account and a Google Cloud project (free — no credit card or billing account required)

## GCP OAuth 2.0 Setup

You need a `credentials.json` file from a Google Cloud project. If you don't have a Google Cloud account, you can create one for free at [console.cloud.google.com](https://console.cloud.google.com) — no credit card is required to create a project or use the Gmail API. Choose one of the two methods below.

---

### Option A — Google Cloud Console (browser)

1. Go to [console.cloud.google.com](https://console.cloud.google.com) and create a new project (or select an existing one).

2. **Enable the Gmail API**
   Navigate to **APIs & Services → Library**, search for **Gmail API**, and click **Enable**.

3. **Configure the OAuth consent screen**
   Go to **APIs & Services → OAuth consent screen**.
   - Choose **External** user type and click **Create**. (Personal Google accounts only support External; Internal is available exclusively to Google Workspace organizations.)
   - Fill in the required fields (App name, User support email, Developer contact email). For App name, `gmail-topsenders` works well. The values only matter for the consent screen you'll see during login.
   - Click **Save and Continue** through the Scopes and Optional Info steps — no additional scopes need to be added here.
   - On the **Test users** step, click **Add Users**, enter your Gmail address, and click **Save**. This is required while the app is in *Testing* status; only listed addresses can authorize it.
   - Click **Back to Dashboard**. There is no need to publish the app — keeping it in *Testing* is intentional. Publishing requires a Google verification process that is only relevant for apps distributed to other users; for personal use, *Testing* works indefinitely.

4. **Create an OAuth 2.0 client**
   Go to **APIs & Services → Credentials** and click **Create Credentials → OAuth client ID**.
   - Application type: **Desktop app**
   - Give it a name (e.g. `gmail-topsenders`) and click **Create**.

5. **Download the credentials**
   Click the download icon next to the client you just created. Save the file as `credentials.json` in the same directory as `main.go`.

---

### Option B — gcloud CLI

```bash
# Authenticate with your Google account
gcloud auth login

# Create a new project (skip if you already have one)
gcloud projects create my-gmail-topsenders --name="Gmail Top Senders"
gcloud config set project my-gmail-topsenders

# Enable the Gmail API (free, no billing account needed)
gcloud services enable gmail.googleapis.com

# Configure the OAuth consent screen (External type, Testing status)
gcloud alpha iap oauth-brands create \
  --application_title="gmail-topsenders" \
  --support_email=$(gcloud config get-value account)
```

> **Note:** The gcloud CLI has limited support for managing OAuth clients and test users directly. For these two steps it is easiest to use the Cloud Console:
>
> - **Create the OAuth client:** **APIs & Services → Credentials → Create Credentials → OAuth client ID → Desktop app**, then download `credentials.json`.
> - **Add your test user:** **APIs & Services → OAuth consent screen → Test users → Add Users**, enter your Gmail address, and save.

---

## Build

Clone the repo and build with Go:

```bash
git clone https://github.com/skinlayers/gmail-topsenders
cd gmail-topsenders
go build -o gmail-topsenders .
```

### Precompiled releases

Download the latest binary for your platform from the [Releases](https://github.com/skinlayers/gmail-topsenders/releases) page. No Go installation required.

On macOS you may need to allow the binary in **System Settings → Privacy & Security** the first time you run it, since it is not notarized.

## Usage

Place `credentials.json` in the same directory as the binary, then run:

```bash
./gmail-topsenders
```

On first run the program prints an authorization URL. Open it in a browser, sign in with the Gmail account you added as a test user, grant access, and paste the authorization code back into the terminal. The token is saved to `token.json` and reused on subsequent runs.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-workers` | `4` | Number of concurrent workers fetching message headers |
| `-top` | `50` | Number of top senders to display (ignored when `-min` is set) |
| `-min` | _(none)_ | Show all senders with at least this many emails (overrides `-top`) |
| `-output` | _(none)_ | Path to write results as a JSON file (e.g. `results.json`) |
| `-cache` | `false` | Cache the raw sender counts locally and reuse them on subsequent runs if fresh |
| `-cache-ttl` | `1h` | How long a cache file is considered fresh (e.g. `30m`, `2h`) |
| `-cache-file` | `counts-cache.json` | Path to the cache file |
| `-qps` | `200` | Maximum Gmail API requests per second (hard quota is 250 QPS) |

```bash
# Show the top 100 senders
./gmail-topsenders -top 100

# Show every sender with 10 or more emails
./gmail-topsenders -min 10

# Save results to a file
./gmail-topsenders -min 10 -output results.json

# Cache results, then re-run instantly with a different threshold
./gmail-topsenders -cache -top 100
./gmail-topsenders -cache -min 5
```

The cache stores the full sender counts (not just the filtered view), so you can freely change `-top` or `-min` between cached runs. It is written only on clean completion — an interrupted run does not overwrite a valid cache. The default cache file (`counts-cache.json`) is excluded from version control by `.gitignore`; if you use a custom path via `-cache-file`, make sure it is also excluded.

The JSON file contains an array of `{"sender": "...", "count": N}` objects sorted by count descending, mirroring what is printed to stdout.

You can interrupt the scan at any time with `Ctrl+C`; partial results are printed based on messages processed so far.

## Files

| File | Description |
|------|-------------|
| `credentials.json` | OAuth 2.0 client credentials — download from GCP Console. |
| `token.json` | Cached OAuth token — created automatically on first run. |
| `counts-cache.json` | Sender counts cache — created when `-cache` is used. Path overridable via `-cache-file`. |

## Rate limits

Gmail's API quota is 250 QPS per user. The program defaults to 200 QPS (override with `-qps`). Rate-limit (429) and other transient errors are retried up to 20 times with exponential backoff capped at ~32s per attempt — enough to weather any realistic API hiccup without skipping messages. Only permanent errors (e.g. a malformed message) are logged and skipped. Increasing `-workers` beyond ~10 is unlikely to speed things up since the bottleneck is the per-user quota, not local concurrency.
