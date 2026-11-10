# Navigating Bifract

Every page is in the left sidebar. The selector at its top shows the fractal or prism you are working in, and the pages below it are scoped to that selection.

![Sidebar with the scope selector and page groups](../images/getting-started/sidebar.png)

## Sidebar

| Section | Pages |
|---------|-------|
| Top | **Fractals**: every fractal and prism you can open |
| Investigate | **Query**, **Recall** (experimental, only when the archive is enabled), **Notebooks**, **Dashboards** |
| Detect | **Alerts** |
| AI | **Chat**, **Instructions** |
| Data | **Lookups**, **Analytics**, **Ingest** |
| Bottom | **Fractal settings** (fractal admins; **Prism settings** in a prism), then **Admin**: **Normalizers**, **Schema**, **API**, **System**, **Settings** (tenant admins) |

- **Switch scope**: use the selector at the top of the sidebar, or the chip at the left of the Query and Recall bars. A prism hides the pages that only exist for a fractal (Recall, Analytics, Ingest).
- **Collapse**: **Collapse** at the bottom folds the sidebar to icons; hover an icon for its name. Query and Recall start collapsed and other pages expanded, and each remembers your choice. Windows narrower than 900px show icons only; on a phone, open the sidebar from the menu button.
- **Open in a new tab**: every page and sub-page has its own URL. Ctrl-click, Cmd-click or middle-click a sidebar item.

## Command palette

![Command palette on Query](../images/getting-started/goto-palette.png)

Press Cmd+K (macOS) or Ctrl+K on any page, or click **Go to...** at the top of the sidebar. Type part of a name, then press Enter or click a result. The right side previews the highlighted result and its actions.

It finds:

- any page or sub-page, such as **Alerts > Coverage** or **Settings > Users**
- another fractal or prism
- a dashboard, notebook, alert rule, lookup or analytics model in the current scope
- your recent queries in this fractal, and its saved queries
- BQL functions and operators, with their syntax, parameters and examples

The page you are on leads. On Query, your recent and saved queries come first, then actions such as **Save current query**. On Recall, **New search**, **Restore this window** and your recent recalls come first. Typing on Query also lists matching functions.

### Tabs

Tabs under the search box show one kind of result at a time: **All**, **Saved**, **Recent**, **Functions**, **Go to**, **Actions** and **Ask AI**, depending on the page. Click a tab, or press Tab to step through them. Saved, Recent and Functions show how many results they hold for what you typed, and each section in **All** has a **See all** link to its tab.

Buttons open the palette on the tab you want:

| Button | Opens |
|--------|-------|
| **Go to...** in the sidebar, or Cmd/Ctrl+K | All |
| **Queries** on Query | Saved |
| **Functions** on Query | Functions |
| **Browse all** in the editor's function hint | Functions, on that function |
| **Recent** on Recall | Recall |

Saved and Recent show each query in full, with its name, description, tags and actions beside it (Load, Run, Edit, Favorite, Delete; Save as and Remove on recent ones).

On Functions, Enter inserts the function into the query at the cursor, starting a new `|` stage when it follows other text, and Cmd/Ctrl+Enter copies its first example.

| Key | Does |
|-----|------|
| Enter | Open the result. On a query, load it into the editor. On a function, insert it |
| Cmd/Ctrl+Enter | Run a query, or copy a function's example |
| Tab, Shift+Tab | Next or previous tab |
| `#`, `/`, `>`, `?` typed first | Open Saved, Functions, Actions, or Ask AI |
| Backspace in an empty box | Back to All |
| Cmd/Ctrl+S | On Query, save the query in the editor |
| Esc | Close a form, then the palette |

Opening Query or Recall from the palette puts the cursor in its query bar.

## Ask AI

When AI is configured, type `?` in the palette, or pick **Ask AI**, and describe what you want to find, for example `failed logins by source IP`. The AI reads the fractal's fields, writes a BQL query, and the server checks it before you see it.

The query is shown as a diff against the one in your editor, so you can ask for a change ("only the last hour", "add a count by host") and see exactly what moved. Enter loads it into the editor and Cmd/Ctrl+Enter runs it. Type a new request and press Enter to refine the answer. Nothing runs until you choose to.
