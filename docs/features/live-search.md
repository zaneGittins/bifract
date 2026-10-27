# Live Search

Live is a time range in the search page's time picker. Pick it and the search keeps running: new events that match the query appear at the top of the results as they arrive, and the timeline rolls over the last 15 minutes.

Use it while onboarding a source ("are these logs arriving and normalizing?") or during an incident ("show me this host as it happens").

## Using it

- Open the time picker and choose **Live**. The clock in the picker becomes a pulsing dot and **Run** becomes **Stop**.
- New rows land at the top with a brief green edge. The header shows how many events matched in the last 15 minutes and the current rate.
- **Reading pauses it.** Scroll down, go past page 1, open a log, hold the mouse button on a row or select text, and new rows wait instead of pushing the table around. The header says how many are waiting. Stop reading (back at the top, log closed, selection cleared) and they appear.
- **Stop** ends it. Choosing any other time range, or dragging a range on the timeline, also ends it.

The table keeps the newest 1,000 events. The timeline and the event count cover every match in the window, however many there are. When more events match per poll than the table can take, the header says "newest rows only".

## What it shows

Live follows arrival time: an event appears when Bifract receives it, labelled with its own event timestamp. A source that delivers yesterday's events today shows up live, with yesterday's timestamps.

Events usually appear within a few seconds of arriving. A quiet source can take up to the ingest flush interval (30 seconds by default) to become searchable, so its events show up in small batches.

## Aggregations and charts

A query that aggregates, sorts or charts (`groupBy()`, `sort()`, `piechart()`, ...) cannot grow row by row. Under Live it re-runs over the last 15 minutes every 10 seconds instead, so the chart or table stays current. It waits while you are reading the result, so a re-run never resets the page you are on. Queries using `comment()`, `pgr()` or other source commands run the same way.

## Load

Each open Live search runs one small query every 2 seconds, reading only the last few minutes of arrivals, and nothing stays open between polls. Polling stops while the browser tab is hidden or you are on another page.
