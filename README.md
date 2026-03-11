<div align="center">
  <img src="web/static/logo.png" alt="Bifract" width="80">
  <h1 style="margin: 0;"><span style="color: #9c6ade;">Bifract</span></h1>
  <p> Open source log management, detection, and collaboration 
</div>

---

## Introduction

Bifract is an open source log management, detection, and collaboration platform designed for security teams. Built on ClickHouse for high-volume log storage and search, it supports collaborative investigation with per-log commenting and uses a pipe-based query language.

## Features

- **Pipe-based query language:** Search, filter, and aggregate logs with an intuitive and expressive language
- **Collaborative investigation:** Comment on log entries and build reusable investigation notebooks
- **Alerting:** Cursor-based evaluation so no logs are missed across restarts
- **Alert Feeds:** Sync Sigma detections via Git and automatically convert and normalize rules
- **Dashboards:** Visualize log data with customizable dashboard panels
- **AI-assisted analysis:** LLM-powered field aware chat for log investigation

## Quick Start

The easiest way to install Bifract is with the Linux setup wizard. It handles SSL, secure passwords, Docker Compose, and database initialization. This downloads the `bifract` binary and runs the interactive setup:
```bash
curl -sfL https://raw.githubusercontent.com/zaneGittins/bifract/main/scripts/install.sh | sh
```

To upgrade an existing installation (if `bifract` is already installed):
```bash
sudo bifract --upgrade
```

This automatically checks for a newer version of `bifract` itself, downloads it if available, then runs the upgrade.

## Documentation

Full documentation is available at **[docs.bifracthq.io](https://docs.bifracthq.io)**, including installation guides, query language reference, and administration.

## Contributing

Bifract is currently maintained by a single developer. Contributions, bug reports, and feature requests are welcome via [GitHub Issues](https://github.com/zaneGittins/bifract/issues). Please be patient with response times.