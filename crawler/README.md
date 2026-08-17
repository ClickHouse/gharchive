# Crawler

GitHub activities are archived by periodically polling the [Events API](https://developer.github.com/v3/activity/events/) and archiving the raw responses into hourly archives - i.e. no additional post processing is done.

For details on how to fetch the archived data, see www.gharchive.org.

## Install

You may need to install openssl to install eventmachine correctly.

```sh
sudo apt-get install openssl-dev
OR
sudo dnf install openssl-devel
```

Then you can install the ruby gems like so:

```sh
gem install bundler:1.16.1
bundle install
```

## Run Crawler

```sh
GITHUB_TOKEN=<token>[,<token>...] bundle exec ruby crawler.rb
```

### Why more than one token

The Events API serves at most 100 events per request and refuses page 4
(`pagination is limited for this resource`), so one poll can observe 300 events.
Event types are not spread evenly over those three pages: **page 1 carries the
push / create / delete stream, and everything else — pull requests, issues,
comments, stars, reviews, releases, forks — is only reachable on pages 2 and 3.**
Polling page 1 alone captures pushes and silently drops nearly everything else.

A single response is an unordered sample of the recent window rather than the
strict newest 100, so two adjacent responses can barely overlap while their union
still covers everything. What matters is the aggregate. Measured on page 1 in
2026-08 over 20 second windows:

| interval | requests/s | events/s captured | overlap | largest timestamp gap |
| ---: | ---: | ---: | ---: | ---: |
| 0.5 s | 1.80 | **100.0** | 46% | 1.0 s |
| 2.0 s | 0.50 | 50.0 | **0%** | 2.0 s |
| 4.0 s | 0.25 | 25.0 | **0%** | 4.0 s |

The stream runs at about 100 events/second, so polling twice a second captures
all of it with margin, while polling every two seconds returns a full page of 100
unrelated events every time and silently drops the rest.

**Polling harder than that buys nothing.** Four concurrent workers at 7.7
requests/second found exactly one more event than a single worker at 1.9
requests/second over the same window, and the already-seen fraction rose from 44%
to 87% - the extra requests only re-fetched what was already captured. More
clients cannot raise the ceiling, because at this rate the feed is already
complete.

The defaults (`PAGE1_INTERVAL=0.5`, `PAGE23_INTERVAL=1.0`) cost 4 requests/second
- 14400/hour - and each response spends one unit of the primary rate limit. That
is more than the 5000/hour of a single user token, so either give the crawler a
GitHub App installation token (15000/hour) or pass several user tokens separated
by commas; they are used round-robin. Extra tokens buy the quota to sustain the
poll rate, not extra coverage.

The per-minute summary reports the overlap between polls for each page. Healthy is
comfortably above zero. If a page shows 0% overlap across a whole minute then
consecutive polls never met, the stream is outrunning the interval, and events are
being lost - the crawler warns and names the interval to lower.

### Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `GITHUB_TOKEN` | required | one token, or several separated by commas |
| `PAGE1_INTERVAL` | `0.5` | seconds between polls of page 1 |
| `PAGE23_INTERVAL` | `1.0` | seconds between polls of pages 2 and 3 |
| `SEEN_LIMIT` | `200000` | event ids remembered for de-duplication |
| `STATHATKEY` | unset | report event counts to StatHat if set |