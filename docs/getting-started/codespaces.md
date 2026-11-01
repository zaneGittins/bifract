# Try Bifract in Codespaces

A GitHub Codespace runs Bifract in a cloud machine you reach from your browser, with [Lab 001](https://labs.bifract.io/001/), a recorded intrusion, already loaded. Nothing is installed on your computer.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/zaneGittins/bifract?quickstart=1)

## Start

1. Click **Open in GitHub Codespaces** above and sign in to GitHub.
2. Click **Create new codespace**. It uses a 4-core machine with 16 GB of memory, the smallest that runs Bifract with the lab.
3. Wait for the editor to open. `WELCOME.md` opens with Bifract's address, username and password, and shows the lab's progress if it is still loading.
4. Click **Open Bifract** in `WELCOME.md` and sign in as `admin` with the password shown there.

Reopening the link later takes you back to the same codespace, so nothing is installed twice.

## What is loaded

| | |
|---|---|
| Lab 001: Meridian workstation intrusion | Three days of logs from a small Windows domain with an intrusion inside it: 2,083,151 logs from Sysmon, Windows event logs, Zeek and Velociraptor Autoruns, across a domain controller, three workstations and a network sensor |
| Fractal | `bifract-labs-001`. The time picker opens on the lab's three days |
| [Endpoint analytics](../features/provenance-graph.md) | On, so `pgr()` scores every process in the lab |

The domains and attacker infrastructure in the lab are fictional. Do not contact any indicator in it.

## Things to try

`WELCOME.md` links each of these straight to the search. Process launches per host:

```bql
bifract_category=process_creation | groupBy(hostname) | sort(_count, order=desc)
```

What kinds of events it holds:

```bql
* | groupBy(bifract_category) | sort(_count, order=desc)
```

The rarest programs and DNS lookups, where unusual activity tends to hide:

```bql
bifract_category=process_creation | groupBy(image) | sort(_count, order=asc) | head(10)
```

```bql
event_id=22 | groupBy(query) | sort(_count, order=asc) | head(10)
```

To see everything a process did, copy its `process_guid` from a process launch and run:

```bql
pgr(start="<guid>") | pgraph()
```

See [the provenance graph](../features/provenance-graph.md) for how to read it, and [BQL basics](../bql/basics.md) for the query language.

## Cost and cleanup

Codespaces bills the GitHub account that creates it. A free personal account includes 120 core hours and 15 GB-months of storage a month, about 30 hours on this machine. See [GitHub's billing](https://docs.github.com/en/billing/concepts/product-billing/github-codespaces).

- A codespace stops when idle and keeps its disk, so the lab is still there when you return.
- Delete it from [github.com/codespaces](https://github.com/codespaces) when you are done to free the storage.

## Good to know

- The address is private to you: GitHub asks you to sign in before it reaches Bifract. Keep the port private.
- The codespace runs a standard [install](installation.md), so everything in these docs applies. To send your own logs, create an [ingest token](../administration/ingest-tokens.md) and run `bifract --ingest <file> --token <token> --url https://localhost:8443 --insecure` in the codespace's terminal.
- It is for trying Bifract, not for keeping logs: install it on your own host for that.
