Third-party browser libraries, vendored so the UI works offline and loads a known
version. Files are copied unmodified from the npm tarball (`npm pack <pkg>@<ver>`).

| Directory | Package | Files |
|---|---|---|
| chartjs | chart.js@4.6.1 | dist/chart.umd.min.js |
| vis-network | vis-network@9.1.13 | standalone/umd/vis-network.min.js |
| leaflet | leaflet@1.9.4 | dist/leaflet.js, dist/leaflet.css, dist/images |
| leaflet-markercluster | leaflet.markercluster@1.5.3 | dist/leaflet.markercluster.js, dist/MarkerCluster*.css |
| marked | marked@18.1.2 | lib/marked.umd.js |
| dompurify | dompurify@3.4.17 | dist/purify.min.js |
| codemirror6.bundle.js | see header | esbuild bundle |

To upgrade, replace the files from the new tarball and update this table.
