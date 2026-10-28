# Exchange

The Exchange is a shared catalog of lookups, detections, normalizers and alert policies. Open **Exchange** in the sidebar (under **Data**) to browse it and add an item to the current fractal or prism in one click.

## What each kind adds

| Kind | Adds | Who can add it |
|---|---|---|
| **Lookup** | A [lookup filled from a URL](dictionaries.md#fill-a-lookup-from-a-url), named after the item. Rows stay upstream and refresh on the item's schedule | Tenant admins |
| **Detection** | An alert, or a draft in your name when the scope reviews alert changes. Sigma rules map fields through the item's normalizer, which must be added first | Analysts |
| **Normalizer** | An instance-wide normalizer | Tenant admins |
| **Policy** | Alert policy rules, appended to the scope's rule set. Rules already there are skipped, so adding it again changes nothing | Fractal admins |

Click an item's name to see exactly what it adds before you add it. Added items are ordinary lookups, alerts, normalizers and policies: edit or delete them as usual. Lookups, normalizers and detections already present under the item's name show **Installed**.

Example: hunt for variants of public attack tools with the **CelesTLSH attack tool hashes** lookup and a TLSH index model on your `tlsh` field (see [`tlsh()`](../bql/enrichment.md#tlsh)):

```
tlsh=*
| tlsh(field=tlsh, dict="celes_tlsh", threshold=50)
| match(dict="celes_tlsh", field=tlsh_match, column=TLSH_Hash, include=[Repo_Name,File_Name])
```

`tlsh()` compares against at most 50,000 digests in a lookup.

## Catalog

Everything in the Exchange today. Expand an item to see exactly what it adds.

<!-- exchange:catalog -->

## Where it comes from

By default the Exchange reads `exchange/index.yaml` from the Bifract repository on GitHub, refreshed every 10 minutes. Set `BIFRACT_EXCHANGE_URL` to read an index from somewhere else, such as a mirror for an offline install, or to `off` to remove it. Item files are read relative to the index URL. Only an index URL you set can reach addresses inside your network.

Items this version of Bifract does not understand, such as a kind added later, are skipped, so a newer index still lists everything an older install can add.

## Contribute an item

Add the file under `exchange/` and list it in `exchange/index.yaml`, then open a pull request. CI checks that every file is listed and every listed file exists, and lints each detection as the server would import it. See `exchange/README.md` in the repository, which also covers running the detection tests.
