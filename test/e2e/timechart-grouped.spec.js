// A grouped timechart arrives ordered by count, not time, and a series can be
// missing buckets. The x-axis must still read in time order with every series
// aligned to it.
const { test, expect } = require('@playwright/test');
const path = require('path');

const CHARTS_JS = path.join(__dirname, '..', '..', 'web', 'static', 'charts.js');

test.use({ storageState: { cookies: [], origins: [] } });

test('grouped timechart sorts buckets and aligns each series by time', async ({ page }) => {
  await page.setContent('<canvas id="c"></canvas>');
  await page.evaluate(() => {
    window.Chart = function (_canvas, config) { window.__chartConfig = config; this.destroy = () => {}; };
  });
  await page.addScriptTag({ path: CHARTS_JS });

  const result = await page.evaluate(() => {
    const clicks = [];
    const data = [
      { time_bucket: '2026-10-26 14:00:00', computer_name: 'a', _count: '5' },
      { time_bucket: '2026-10-27 08:00:00', computer_name: 'a', _count: '7' },
      { time_bucket: '2026-10-26 13:00:00', computer_name: 'b', _count: '2' },
      { time_bucket: '2026-10-27 08:00:00', computer_name: 'b', _count: '3' },
      { time_bucket: '2026-10-26 15:00:00', computer_name: 'b', _count: '4' },
    ];
    BifractCharts.renderTimeChart(document.getElementById('c'), {
      data,
      fields: ['time_bucket', 'computer_name', '_count'],
      onDataClick: (ctx) => clicks.push(ctx),
    });
    const cfg = window.__chartConfig;
    cfg.options.onClick({}, [{ index: 0, datasetIndex: 0 }]);
    cfg.options.onClick({}, [{ index: 1, datasetIndex: 0 }]);
    return {
      labels: cfg.data.labels,
      series: cfg.data.datasets.map(d => ({ label: d.label, data: d.data })),
      clicks: clicks.map(c => ({ series: c.series, row: c.row })),
    };
  });

  expect(result.labels).toEqual([
    '2026-10-26 13:00:00', '2026-10-26 14:00:00', '2026-10-26 15:00:00', '2026-10-27 08:00:00',
  ]);
  expect(result.series).toEqual([
    { label: 'a', data: [0, 5, 0, 7] },
    { label: 'b', data: [2, 0, 4, 3] },
  ]);
  // Clicking a bucket the series has no row for still names the series and time.
  expect(result.clicks[0]).toEqual({
    series: 'a', row: { time_bucket: '2026-10-26 13:00:00', computer_name: 'a' },
  });
  expect(result.clicks[1].row._count).toBe('5');
});
