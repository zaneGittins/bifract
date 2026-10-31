# Using the Exchange

The Exchange is a shared catalog of content for Bifract. Open **Exchange** in the sidebar (under **Data**) to browse it and add an item to the current fractal or prism in one click. The [catalog](index.md) lists the same items here.

## What each kind adds

| Kind | Adds | Who can add it |
|---|---|---|
| **Dataset** | A new fractal named after the dataset, filled with its logs in the background. See [Datasets](#datasets) | Tenant admins |
| **Bundle** | Each of its items not already present, requirements first | Whoever can add each item |
| **Lookup** | A [lookup filled from a URL](../features/dictionaries.md#fill-a-lookup-from-a-url), named after the item. Rows stay upstream and refresh on the item's schedule | Tenant admins |
| **Model** | An [analytics model](../features/models.md), named after the item. It builds from new logs; run a backfill for stored ones. Fractals only | Analysts |
| **Detection** | An alert, or a draft in your name when the scope reviews alert changes. Sigma rules map fields through the normalizer the item requires | Analysts |
| **Saved search** | A saved query shared with everyone in the scope | Viewers |
| **Dashboard** | A [dashboard](../features/dashboards.md) | Analysts |
| **Normalizer** | An instance-wide normalizer | Tenant admins |
| **Policy** | Alert policy rules, appended to the scope's rule set. Rules already there are skipped, so adding it again changes nothing | Fractal admins |

Click an item's name to see exactly what it adds before you add it. Added items are ordinary lookups, models, alerts, searches, dashboards, normalizers and policies: edit or delete them as usual. Items already present under the item's name show **Installed**; policies have no name, so they never do.

## Requirements and bundles

An item can require others, such as a detection that reads a model built from a lookup. **Add** adds whatever it requires first, skipping what is already present, and says what it will add in the button's tooltip and the item's preview.

A bundle groups the items of one use case. **Add all** adds every member that is missing, in order, and stops at the first failure so nothing is added without what it needs. Each member can also be added on its own.

## Datasets

A dataset is a recorded set of logs, such as a simulated intrusion, for training, CTFs and trying Bifract. **Add** creates a fractal named after the dataset's id and imports the dataset into it:

1. The server downloads the file and checks its size and SHA-256, then that it holds the number of logs the Exchange lists. A file that fails any check imports nothing.
2. Rows go through the normal ingest path, so models and alerts in the fractal see them. The import waits while ingestion is busy, so live logs come first. The fractal keeps the rows until you delete it (no retention).
3. The import succeeds once every log is stored. The item shows progress while it runs, and **Open** switches to the fractal, which can be searched as it fills.

The fractal opens on the dataset's time range, and the time picker offers it as **Dataset range** while you are in it. **Retry** runs a failed import again, and an import cut off by a server restart starts over on its own a few minutes after the server is back. Either way the fractal is emptied first, so it never holds part of a dataset twice. With the Exchange turned off, no imports run.

To load a dataset into an existing fractal instead, use the [CLI](../ingestion/cli-tool.md#load-an-exchange-dataset).

## Where it comes from

By default the Exchange reads `exchange/index.yaml` from the Bifract repository on GitHub, refreshed every 10 minutes. Set `BIFRACT_EXCHANGE_URL` to read an index from somewhere else, such as a mirror for an offline install, or to `off` to remove it. Item files are read relative to the index URL. Only an index URL you set can reach addresses inside your network.

Items this version of Bifract does not understand, such as a kind added later, are skipped, so a newer index still lists everything an older install can add.

## Contribute an item

Add the file under `exchange/` and list it in `exchange/index.yaml`, then open a pull request. See `exchange/README.md` in the repository. CI checks that:

- every file is listed, every listed file exists, and each loads as its import would
- every lookup and model a file uses is one it requires
- each detection passes the same checks the server runs on import
- files are plain YAML or Markdown with lowercase names and printable ASCII text only
