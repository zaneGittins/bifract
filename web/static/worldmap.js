// Shared world map renderer used by search, notebooks, and dashboards.
// Requires Leaflet.js and Leaflet.markercluster to be loaded.
const BifractWorldMap = {
    _lastMap: null,

    // render creates a Leaflet map inside `container` with clustered markers.
    // options: { latField, lonField, labelField }
    render(container, data, options) {
        if (typeof L === 'undefined') return null;

        const latField = options.latField || 'latitude';
        const lonField = options.lonField || 'longitude';
        const labelField = options.labelField || null;

        const cv = window.ThemeManager ? ThemeManager.getCSSVar : () => '';
        const markerColor = cv('--worldmap-marker') || '#9c6ade';
        const glowColor = cv('--worldmap-marker-glow') || 'rgba(156,106,222,0.4)';
        const isDark = document.documentElement.getAttribute('data-theme') !== 'light';

        const map = L.map(container, {
            center: [20, 0],
            zoom: 2,
            minZoom: 2,
            maxZoom: 18,
            zoomControl: true,
            attributionControl: true
        });

        // Use dark tiles for dark theme, standard for light
        const tileUrl = isDark
            ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
            : 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png';
        L.tileLayer(tileUrl, {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
            subdomains: 'abcd',
            maxZoom: 19
        }).addTo(map);

        // Cluster group with custom icons
        const clusterGroup = L.markerClusterGroup({
            maxClusterRadius: 50,
            spiderfyOnMaxZoom: true,
            showCoverageOnHover: false,
            zoomToBoundsOnClick: true,
            iconCreateFunction: function(cluster) {
                const count = cluster.getChildCount();
                let size, fontSize;
                if (count < 10) {
                    size = 30;
                    fontSize = 12;
                } else if (count < 100) {
                    size = 36;
                    fontSize = 13;
                } else if (count < 1000) {
                    size = 44;
                    fontSize = 14;
                } else {
                    size = 52;
                    fontSize = 15;
                }

                const displayCount = count >= 10000
                    ? (count / 1000).toFixed(0) + 'k'
                    : count >= 1000
                        ? (count / 1000).toFixed(1) + 'k'
                        : count.toString();

                return L.divIcon({
                    html: '<div class="worldmap-cluster-icon" style="' +
                        'width:' + size + 'px;height:' + size + 'px;' +
                        'background:' + markerColor + ';' +
                        'box-shadow:0 0 ' + (size / 2) + 'px ' + glowColor + ';' +
                        'font-size:' + fontSize + 'px;">' +
                        displayCount + '</div>',
                    className: '',
                    iconSize: L.point(size, size)
                });
            }
        });

        // Individual marker icon
        const markerIcon = L.divIcon({
            html: '<div style="' +
                'width:10px;height:10px;border-radius:50%;' +
                'background:' + markerColor + ';' +
                'box-shadow:0 0 6px ' + glowColor + ';' +
                'border:2px solid rgba(255,255,255,0.5);' +
                '"></div>',
            className: '',
            iconSize: L.point(14, 14),
            iconAnchor: L.point(7, 7)
        });

        const bounds = [];

        data.forEach(row => {
            const lat = parseFloat(row[latField]);
            const lon = parseFloat(row[lonField]);
            if (isNaN(lat) || isNaN(lon) || (lat === 0 && lon === 0)) return;

            const marker = L.marker([lat, lon], { icon: markerIcon });
            let popupContent = '';
            if (labelField && row[labelField]) {
                popupContent += '<div class="worldmap-popup-label">' +
                    BifractWorldMap._escape(String(row[labelField])) + '</div>';
            }
            popupContent += '<div class="worldmap-popup-coords">' +
                lat.toFixed(4) + ', ' + lon.toFixed(4) + '</div>';

            // Include other fields from the row
            const skipFields = new Set([latField, lonField, labelField, 'timestamp', 'log_line']);
            const extraFields = Object.keys(row).filter(k => !skipFields.has(k) && row[k] !== '' && row[k] != null);
            if (extraFields.length > 0) {
                popupContent += '<div style="margin-top:4px;border-top:1px solid var(--border-color);padding-top:4px;">';
                extraFields.slice(0, 6).forEach(k => {
                    popupContent += '<div style="font-size:11px;"><span style="color:var(--text-muted);">' +
                        BifractWorldMap._escape(k) + ':</span> ' +
                        BifractWorldMap._escape(String(row[k])) + '</div>';
                });
                popupContent += '</div>';
            }

            marker.bindPopup(popupContent, { maxWidth: 280 });
            clusterGroup.addLayer(marker);
            bounds.push([lat, lon]);
        });

        map.addLayer(clusterGroup);

        if (bounds.length > 0) {
            map.fitBounds(bounds, { padding: [30, 30], maxZoom: 10 });
        }

        // Ensure map renders correctly after DOM insertion
        setTimeout(() => map.invalidateSize(), 100);

        BifractWorldMap._lastMap = map;
        return map;
    },

    _escape(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }
};
