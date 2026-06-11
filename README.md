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

The program connects to Gmail via Google's official API, fetches every message in your inbox, and counts how many times each sender appears. Results are sorted and printed to your terminal. It uses a pool of concurrent workers and stays within Gmail's API rate limits automatically.

On first run it walks you through a one-time login flow to grant the program read-only access to your Gmail. The resulting token is saved locally and reused on subsequent runs — you won't be asked to log in again.

## Prerequisites

- [Go](https://go.dev/dl/) 1.26.3 or later — only needed if building from source; precompiled binaries are available (see [Installation](#installation))
- A Google account — the same one whose Gmail you want to scan

## One-time Setup: Connecting to Gmail

To read your Gmail, the program needs permission from Google. This is handled through **OAuth 2.0** — a standard login flow where you grant the app access via your browser, just like "Sign in with Google" on other websites. The program only requests **read-only** access; it cannot send, delete, or modify anything. **Your emails never leave your computer** — the program reads them locally and only stores a count per sender address.

The setup takes about 5 minutes and only needs to be done once. You'll need to create a free Google Cloud project to generate a `credentials.json` file that the program uses to identify itself to Google.

> **No credit card required.** Creating a Google Cloud project and using the Gmail API is completely free. You will not be asked for payment information.

---

### Option A — Google Cloud Console *(recommended)*

1. Go to [console.cloud.google.com](https://console.cloud.google.com) and sign in with your Google account. Create a new project when prompted (or select an existing one). You can name it anything — `gmail-topsenders` works well. ([Docs: creating a project](https://cloud.google.com/resource-manager/docs/creating-managing-projects))

2. **Enable the Gmail API**
   Navigate to **APIs & Services → Library**, search for **Gmail API**, and click **Enable**. ([Docs: enabling APIs](https://cloud.google.com/apis/docs/getting-started#enabling_an_api))

3. **Configure the OAuth consent screen**

   This is the screen you'll see when you log in the first time. Go to **APIs & Services → OAuth consent screen**. ([Docs: configuring the consent screen](https://developers.google.com/workspace/guides/configure-oauth-consent))
   - Choose **External** user type and click **Create**. (Personal Google accounts only support External — this is normal.)
   - Fill in the required fields: **App name** (`gmail-topsenders`), **User support email**, and **Developer contact email**. These values only appear on the consent screen you'll see during login.
   - Click **Save and Continue** through the Scopes and Optional Info steps — no changes needed on those pages.
   - On the **Test users** step, click **Add Users**, enter your Gmail address, and click **Save**. This allows your account to authorise the app while it remains in *Testing* status.
   - Click **Back to Dashboard**. There is no need to publish the app — keeping it in *Testing* is intentional and works indefinitely for personal use. Publishing would trigger a Google review process only relevant for apps distributed to others.

4. **Create an OAuth 2.0 client**

   Go to **APIs & Services → Credentials** and click **Create Credentials → OAuth client ID**. ([Docs: creating credentials](https://developers.google.com/workspace/guides/create-credentials))
   - Application type: **Desktop app**
   - Give it a name (e.g. `gmail-topsenders`) and click **Create**.

5. **Download the credentials file**

   Click the download icon next to the client you just created. Save the file as `credentials.json` in the same directory as the `gmail-topsenders` binary.

---

### Option B — gcloud CLI *(advanced)*

If you have the [gcloud CLI](https://cloud.google.com/sdk/docs/install) installed, you can complete most of the setup from the terminal. The OAuth client creation and test user steps still require the Cloud Console.

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

Then finish up in the Cloud Console:
- **Create the OAuth client:** **APIs & Services → Credentials → Create Credentials → OAuth client ID → Desktop app**, then download `credentials.json`.
- **Add your test user:** **APIs & Services → OAuth consent screen → Test users → Add Users**, enter your Gmail address, and save.

---

## Installation

### Precompiled releases *(easiest)*

Download the latest binary for your platform from the [Releases](https://github.com/skinlayers/gmail-topsenders/releases) page. No Go installation required.

```bash
# macOS / Linux — make the binary executable after downloading
chmod +x gmail-topsenders
```

On macOS you may need to allow the binary in **System Settings → Privacy & Security** the first time you run it, since it is not notarized.

### Build from source

```bash
git clone https://github.com/skinlayers/gmail-topsenders
cd gmail-topsenders
go build -o gmail-topsenders .
```

## Usage

Place `credentials.json` in the same directory as the binary, then run:

```bash
./gmail-topsenders
```

On first run the program will open your browser automatically and prompt you to sign in with the Gmail account you added as a test user. After granting access, the program receives the authorization code in the background and continues — no manual steps needed.

If the browser cannot be opened (e.g. on a remote server), the program falls back to printing an authorization URL. Open it manually, grant access, and paste the full redirect URL from your browser's address bar back into the terminal — the authorization code is extracted automatically.

The token is saved to `token.json` and reused on subsequent runs — you won't be asked again.

> **Note:** A full inbox scan can take a significant amount of time — anywhere from a few minutes to over an hour depending on the size of your mailbox and Gmail's API rate limits. Use `-query` to narrow the scan or `-cache` to avoid repeating it.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-workers` | `4` | Number of concurrent workers fetching message headers |
| `-top` | `50` | Number of top senders to display (ignored when `-min` is set) |
| `-min` | _(none)_ | Show all senders with at least this many emails (overrides `-top`) |
| `-output` | _(none)_ | Path to write results as a JSON file (e.g. `results.json`) |
| `-query` | _(none)_ | Gmail search query to filter messages (e.g. `in:inbox`, `after:2024/01/01`) |
| `-cache` | `false` | Cache the raw sender counts locally and reuse them on subsequent runs if fresh |
| `-cache-ttl` | `1h` | How long a cache file is considered fresh (e.g. `30m`, `2h`); implies `-cache` |
| `-cache-file` | `counts-cache.json` | Path to the cache file; implies `-cache` |
| `-qps` | `200` | Maximum Gmail API requests per second (hard quota is 250 QPS) |
| `-sort-by` | `count` | Sort results by `count` (number of emails) or `size` (total space used) |

```bash
# Show the top 100 senders
./gmail-topsenders -top 100

# Show every sender with 10 or more emails
./gmail-topsenders -min 10

# Only scan inbox messages from 2024 onwards
./gmail-topsenders -query "in:inbox after:2024/01/01"

# Save results to a file
./gmail-topsenders -min 10 -output results.json

# Sort by total space used instead of email count
./gmail-topsenders -sort-by size

# Cache results, then re-run instantly with a different threshold
./gmail-topsenders -cache -top 100
./gmail-topsenders -cache -min 5
```

The cache stores the full sender counts (not just the filtered view), so you can freely change `-top` or `-min` between cached runs. It is written only on clean completion — an interrupted run does not overwrite a valid cache. The default cache file (`counts-cache.json`) is excluded from version control by `.gitignore`.

The JSON file contains an array of `{"sender": "...", "count": N}` objects sorted by count descending, mirroring what is printed to stdout.

You can interrupt the scan at any time with `Ctrl+C`; partial results are printed based on messages processed so far.

## Files

| File | Description |
|------|-------------|
| `credentials.json` | OAuth 2.0 client credentials — download from GCP Console. |
| `token.json` | Cached OAuth token — created automatically on first run. |
| `counts-cache.json` | Sender counts cache — created when `-cache` is used. Path overridable via `-cache-file`. |

## Development

```bash
# Run all tests
go test -race -count=1 -shuffle=on ./...

# Run with verbose output
go test -race -count=1 -shuffle=on -v ./...

# Run linter (requires golangci-lint: https://golangci-lint.run/welcome/install/)
golangci-lint run ./...
```

## Rate limits

Gmail's API quota is 250 QPS per user. The program defaults to 200 QPS (override with `-qps`). Rate-limit (429) and other transient errors are retried up to 20 times with exponential backoff capped at ~32s per attempt — enough to weather any realistic API hiccup without skipping messages. Only permanent errors (e.g. a malformed message) are logged and skipped. Increasing `-workers` beyond ~10 is unlikely to speed things up since the bottleneck is the per-user quota, not local concurrency.

At 200 QPS, processing 50,000 messages takes roughly 4–5 minutes; 200,000 messages can take 15–20 minutes. Use `-query` to narrow the scan (e.g. `after:2024/01/01`) or `-cache` to avoid repeating it.
