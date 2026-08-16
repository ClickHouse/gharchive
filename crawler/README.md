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

Measured in 2026-08, page 1 replaces all 100 of its entries in well under a
second and pages 2 and 3 in one to two seconds, so all three are polled on
independent schedules. The defaults (`PAGE1_INTERVAL=0.5`, `PAGE23_INTERVAL=1.0`)
cost 4 requests/second — 14400/hour — and each response spends one unit of the
primary rate limit. That is more than the 5000/hour of a single user token, so
either give the crawler a GitHub App installation token (15000/hour) or pass
several user tokens separated by commas; they are used round-robin.

Page 1 alone runs at over 190 events/second, which is faster than the API will
serve it to one client, so pushes cannot be captured completely at any affordable
polling rate. Pages 2 and 3 can. When a page returns 100 entries that are all
new, the window turned over completely between two polls and events in between
were lost; the crawler warns about it (at most once a minute per page) and counts
it in the per-minute summary.

### Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `GITHUB_TOKEN` | required | one token, or several separated by commas |
| `PAGE1_INTERVAL` | `0.5` | seconds between polls of page 1 |
| `PAGE23_INTERVAL` | `1.0` | seconds between polls of pages 2 and 3 |
| `SEEN_LIMIT` | `200000` | event ids remembered for de-duplication |
| `STATHATKEY` | unset | report event counts to StatHat if set |