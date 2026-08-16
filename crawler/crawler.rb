require 'log4r'
require 'yajl'
require 'digest'
require 'em-http'
require 'em-stathat'

require_relative 'obfuscate.rb'

include EM

##
## Setup
##

# GitHub caps per_page at 100 for /events (a larger value is silently clamped),
# and refuses page 4 with "pagination is limited for this resource", so a single
# poll can observe at most 3 * 100 events.
PAGE_LIMIT = 100
PAGES = (1..3).to_a

# Event types are not spread evenly over the pages: page 1 carries the push /
# create / delete stream, and everything else - pull requests, issues, comments,
# stars, reviews, releases, forks - is only reachable on pages 2 and 3. A crawler
# that reads page 1 alone therefore captures pushes and silently drops almost
# everything else.
#
# Measured turnover (2026-08): page 1 replaces all 100 entries in well under a
# second, pages 2 and 3 in one to two seconds. Every page is polled on its own
# schedule, so a busy page 1 cannot delay the others.
#
# The defaults cost 4 requests/second - 14400/hour, which one GitHub App
# installation (15000/hour) or three user tokens can serve. Page 1 alone runs at
# over 190 events/second, so pushes cannot be captured completely through this
# endpoint at any affordable polling rate; pages 2 and 3 can.
PAGE_INTERVALS = {
  1 => (ENV['PAGE1_INTERVAL'] || 0.5).to_f,
  2 => (ENV['PAGE23_INTERVAL'] || 1.0).to_f,
  3 => (ENV['PAGE23_INTERVAL'] || 1.0).to_f
}

# Pass several tokens separated by commas (or a GitHub App installation token,
# 15000/hour) and they are used round-robin.
TOKENS = (ENV['GITHUB_TOKEN'] || '').split(',').map(&:strip).reject(&:empty?)

# Remember which event ids have already been written. The previous version of
# this crawler only compared against the immediately preceding response, so any
# event that briefly dropped out of the window was archived twice.
SEEN_LIMIT = (ENV['SEEN_LIMIT'] || 200_000).to_i

StatHat.config do |c|
  c.ukey  = ENV['STATHATKEY']
  c.email = 'ilya@igvita.com'
end

@log = Log4r::Logger.new('github')
@log.add(Log4r::StdoutOutputter.new('console', {
  :formatter => Log4r::PatternFormatter.new(:pattern => "[#{Process.pid}:%l] %d :: %m")
}))

if TOKENS.empty?
  @log.error "No GITHUB_TOKEN environment variable defined."
  raise "No GITHUB_TOKEN environment variable defined."
end

##
## Crawler
##

EM.run do
  stop = Proc.new do
    puts "Terminating crawler"
    EM.stop
  end

  Signal.trap("INT",  &stop)
  Signal.trap("TERM", &stop)

  @seen = {}
  @etags = {}
  @inflight = {}
  @due = {}
  @token = 0
  @stats = Hash.new(0)
  @warned = {}
  @paused_until = nil

  PAGES.each { |page| @due[page] = Time.now }

  @event_key = lambda { |e| "#{e['id']}" }

  next_token = lambda do
    token = TOKENS[@token % TOKENS.size]
    @token += 1
    token
  end

  # Insertion-ordered hash, so the oldest ids are the first to be evicted. Evict
  # in batches, otherwise every poll past the limit walks the whole key set.
  remember = lambda do |ids|
    ids.each { |id| @seen[id] = true }
    if @seen.size > SEEN_LIMIT
      @seen.keys.first(SEEN_LIMIT / 10).each { |id| @seen.delete(id) }
    end
  end

  archive_file = lambda do
    # Name the archive after the wall clock, not after the event timestamps:
    # an event may arrive after the file matching its own hour was compressed.
    archive = "data/#{Time.now.strftime('%Y-%m-%d-%-k')}.json"
    if @file.nil? || (archive != @file.to_path)
      if !@file.nil?
        @log.info "Rotating archive. Current: #{@file.to_path}, New: #{archive}"
        @file.close
      end
      @file = File.new(archive, "a+")
    end
    @file
  end

  # Slow every page down when the remaining quota runs behind the time left in
  # the window, so that a burst cannot exhaust the hour and blind the crawler.
  check_budget = lambda do |header|
    remaining = header.raw['X-RateLimit-Remaining'].to_i
    reset = header.raw['X-RateLimit-Reset'].to_i
    return if reset.zero?
    seconds_left = reset - Time.now.to_i
    return if seconds_left <= 0
    # The header describes the token that served this request, so the threshold
    # is per token and does not depend on how many are in the pool.
    if remaining < 50
      delay = seconds_left.to_f / [remaining, 1].max
      @paused_until = Time.now + [delay, 60].min
      @log.warn "Rate limit nearly exhausted (#{remaining} left, resets in #{seconds_left}s), " \
                "backing off for #{'%.1f' % [@paused_until - Time.now]}s"
    end
  end

  handle = lambda do |page, req|
    status = req.response_header.status

    if status == 304
      @stats[:not_modified] += 1
      return
    end

    if status == 403 || status == 429
      retry_after = req.response_header.raw['Retry-After'].to_i
      retry_after = 60 if retry_after.zero?
      @paused_until = Time.now + retry_after
      @log.warn "Throttled by GitHub on page #{page} (HTTP #{status}), pausing for #{retry_after}s"
      return
    end

    if status != 200
      @log.error "Unexpected HTTP #{status} on page #{page}: #{req.response[0, 500]}"
      return
    end

    events = Yajl::Parser.parse(req.response)
    unless events.is_a?(Array)
      @log.error "Page #{page} did not return a list: #{req.response[0, 500]}"
      return
    end

    ids = events.collect(&@event_key)
    fresh = events.reject { |e| @seen.key?(@event_key.call(e)) }
    remember.call(ids)

    file = archive_file.call
    fresh.each { |event| file.puts(Yajl::Encoder.encode(Obfuscate.email(event))) }
    file.flush

    @stats[:events] += fresh.size
    @stats[:polls] += 1

    # Every entry of the page being new means the window turned over completely
    # between two polls, so events in between were never seen. This is the check
    # that PAGE_LIMIT = 500 disabled: it compared against a limit the API can
    # never return, so it could not fire.
    if !events.empty? && fresh.size == events.size && @seen.size > events.size
      @stats[:"saturated_page#{page}"] += 1
      # Page 1 is expected to saturate - it moves faster than the API can serve
      # to one client - so keep this to one line a minute per page.
      if @warned[page].nil? || Time.now - @warned[page] > 60
        @warned[page] = Time.now
        @log.warn "Page #{page} turned over completely between polls " \
                  "(#{fresh.size}/#{events.size} new) - events in between were missed"
      end
    end

    check_budget.call(req.response_header)
  end

  poll = lambda do |page|
    @inflight[page] = true
    url = "https://api.github.com/events?per_page=#{PAGE_LIMIT}&page=#{page}"
    req = HttpRequest.new(url, {
      :inactivity_timeout => 5,
      :connect_timeout => 5
    }).get({
      :head => {
        'user-agent' => 'gharchive.org',
        'accept' => 'application/vnd.github+json',
        'Authorization' => 'token ' + next_token.call,
        'If-None-Match' => @etags[page]
      }.compact
    })

    req.callback do
      begin
        @etags[page] = req.response_header.etag if req.response_header.status == 200
        handle.call(page, req)
      rescue Exception => e
        @log.error "Failed to process page #{page}: #{e}, #{e.backtrace.first(5)}"
      ensure
        @inflight[page] = false
        @due[page] = Time.now + PAGE_INTERVALS[page]
      end
    end

    req.errback do
      @log.error "Error fetching page #{page}: #{req.response_header.status}, #{req.error}"
      @inflight[page] = false
      @due[page] = Time.now + PAGE_INTERVALS[page]
    end
  end

  EM.add_periodic_timer(0.1) do
    now = Time.now
    if @paused_until && now < @paused_until
      next
    end
    PAGES.each do |page|
      poll.call(page) if !@inflight[page] && now >= @due[page]
    end
  end

  EM.add_periodic_timer(60) do
    saturated = PAGES.map { |p| "p#{p}:#{@stats[:"saturated_page#{p}"]}" }.join(' ')
    @log.info "Last minute: #{@stats[:events]} events archived over #{@stats[:polls]} polls " \
              "(#{@stats[:not_modified]} not modified), #{@seen.size} ids remembered, " \
              "polls that saturated: #{saturated}"
    StatHat.new.ez_count('Github Events', @stats[:events]) if ENV['STATHATKEY']
    @stats.clear
  end
end
