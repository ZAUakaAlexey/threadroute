import { expect, test, type Page } from '@playwright/test';

import { control, signInFull } from './helpers.ts';

// Stand settings and the trend strip (decision 0014) against the fake
// gateway. It starts at version 2: the first report of the simulator and
// one change of the load that forms a team, from 20 to 25.

test.beforeEach(async () => {
  await control('reset');
});

async function openSettings(page: Page) {
  await page.goto('#/supervisor');
  await expect(page.getByText('Messages per second')).toBeVisible();
  const toggle = page.getByRole('button', { name: 'Stand settings', exact: true });
  await toggle.click();
  await expect(toggle).toHaveAttribute('aria-expanded', 'true');
  return page.getByRole('region', { name: 'Stand settings' });
}

test('apply a change and see it in the panel, the list of changes and the trend strip', async ({ page }) => {
  const panel = await openSettings(page);
  const share = panel.getByRole('textbox', { name: 'Share of operators in teams' });
  await expect(share).toHaveValue('50');
  const apply = panel.getByRole('button', { name: 'Apply' });
  await expect(apply).toBeDisabled();
  await share.fill('30');
  await expect(panel.getByText('1 unsaved change')).toBeVisible();
  await apply.click();
  await expect(panel.getByRole('status')).toHaveText('Applied. The stand now runs version 3 of the settings.');
  await expect(share).toHaveValue('30');
  await expect(apply).toBeDisabled();
  // The demo level never shows who changed the settings.
  const changes = panel.getByRole('list', { name: 'Last changes of the stand settings' });
  await expect(changes.getByRole('listitem').first()).toContainText('version 3');
  await expect(changes).not.toContainText(' by ');
  const marks = page.getByRole('region', { name: 'Last hour' }).getByRole('list', { name: 'Settings changes in this hour' });
  await expect(marks.getByRole('listitem')).toHaveCount(3);
  await expect(marks.getByRole('listitem').last()).toContainText('Share of operators in teams: 50% to 30%');
});

test('at the full level the list of changes says who made each change', async ({ page }) => {
  await signInFull(page);
  const panel = await openSettings(page);
  await panel.getByRole('textbox', { name: 'Team size, max' }).fill('18');
  await panel.getByRole('button', { name: 'Apply' }).click();
  await expect(panel.getByRole('status')).toContainText('Applied');
  const changes = panel.getByRole('list', { name: 'Last changes of the stand settings' });
  // Each change is an item with a list of its fields inside.
  await expect(changes.getByRole('listitem').first()).toContainText('version 3 by synth-user-1');
  await expect(changes).toContainText('version 2 by guest');
});

test('an invalid value is named next to its field and blocks Apply', async ({ page }) => {
  const panel = await openSettings(page);
  const formLoad = panel.getByRole('textbox', { name: 'Load that forms a team' });
  await formLoad.fill('600');
  await expect(formLoad).toHaveAttribute('aria-invalid', 'true');
  await expect(panel.getByText('Use a value from 2 to 500.')).toBeVisible();
  await expect(panel.getByRole('button', { name: 'Apply' })).toBeDisabled();
  await formLoad.fill('25');
  await panel.getByRole('textbox', { name: 'Load that releases a team' }).fill('25');
  await expect(panel.getByText('Must be below the load that forms a team (25).')).toBeVisible();
  await expect(panel.getByRole('button', { name: 'Apply' })).toBeDisabled();
  await panel.getByRole('button', { name: 'Discard changes' }).click();
  await expect(panel.getByRole('textbox', { name: 'Load that releases a team' })).toHaveValue('8');
});

test('a conflict keeps the edits visible and shows the values of the other change', async ({ page }) => {
  const panel = await openSettings(page);
  await control('settings-answer', { answer: 'conflict' });
  const minSize = panel.getByRole('textbox', { name: 'Team size, min' });
  await minSize.fill('3');
  await panel.getByRole('button', { name: 'Apply' }).click();
  await expect(panel.getByRole('status')).toContainText('Someone else changed the settings first');
  await expect(panel.getByRole('alert')).toContainText('now version 3');
  await expect(minSize).toHaveValue('3');
  // The other change lowered the max size from 20 to 19.
  await expect(panel.getByRole('textbox', { name: 'Team size, max' })).toHaveValue('19');
  await expect(panel.getByRole('button', { name: 'Apply' })).toBeDisabled();
  await panel.getByRole('button', { name: 'Keep my edits' }).click();
  await control('settings-ready');
  await panel.getByRole('button', { name: 'Apply' }).click();
  await expect(panel.getByRole('status')).toHaveText('Applied. The stand now runs version 4 of the settings.');
  await expect(panel.getByRole('textbox', { name: 'Team size, max' })).toHaveValue('19');
});

test('a second change right after the first is too soon, and the console says how long to wait', async ({ page }) => {
  const panel = await openSettings(page);
  await panel.getByRole('textbox', { name: 'Team size, max' }).fill('18');
  await panel.getByRole('button', { name: 'Apply' }).click();
  await expect(panel.getByRole('status')).toContainText('Applied');
  await panel.getByRole('textbox', { name: 'Team size, max' }).fill('17');
  await panel.getByRole('button', { name: 'Apply' }).click();
  await expect(panel.getByRole('status')).toContainText(/You can apply again in \d+ seconds?\./);
  await expect(panel.getByRole('button', { name: 'Apply' })).toBeDisabled();
});

test('a change by someone else updates the panel live', async ({ page }) => {
  const panel = await openSettings(page);
  await control('settings-change', { fields: { 'sim.capacity': 6 } });
  await expect(panel.getByRole('textbox', { name: 'Capacity of an operator' })).toHaveValue('6');
  await expect(panel.getByRole('list', { name: 'Last changes of the stand settings' }).getByRole('listitem').first()).toContainText(
    'Capacity of an operator: 4 to 6',
  );
});

test('without the right the values are read only and no "by" is shown', async ({ page }) => {
  await control('settings-right', { may_change: false });
  const panel = await openSettings(page);
  await expect(panel).toContainText('Changing them needs a site account with access');
  await expect(panel.getByRole('textbox')).toHaveCount(0);
  await expect(panel.getByRole('button', { name: 'Apply' })).toHaveCount(0);
  await expect(panel.getByRole('list', { name: 'Last changes of the stand settings' })).not.toContainText(' by ');
});

test('before a simulator reported, the sim group says it is not available', async ({ page }) => {
  await control('settings-mode', { mode: 'no_sim' });
  const panel = await openSettings(page);
  await expect(panel.getByRole('group', { name: 'Simulated operators' })).toContainText('Not available');
  await expect(panel.getByRole('textbox', { name: 'Simulated operators' })).toHaveCount(0);
});

test('an older gateway: the screen works as before, with no settings and no trend strip', async ({ page }) => {
  await control('settings-mode', { mode: 'old' });
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(String(e)));
  page.on('console', (m) => {
    if (m.type() === 'error') errors.push(m.text());
  });
  await page.goto('#/supervisor');
  await expect(page.getByText('Messages per second')).toBeVisible();
  await expect(page.getByRole('table', { name: 'Conversations' }).getByRole('row').nth(3)).toBeVisible();
  await control('trend-point');
  await expect(page.getByRole('region', { name: 'Last hour' })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Stand settings', exact: true })).toHaveCount(0);
  // Still live: the connection did not break on something it did not know.
  await page.waitForTimeout(500);
  await expect(page.getByText('Live', { exact: true })).toBeVisible();
  expect(errors).toEqual([]);
});

test('the trend strip gets new points and a mark for a change, and its charts keep their size', async ({ page }) => {
  await page.setViewportSize({ width: 1400, height: 900 });
  await page.goto('#/supervisor');
  const strip = page.getByRole('region', { name: 'Last hour' });
  const charts = strip.getByRole('list', { name: 'Trends of the last hour' });
  const waiting = strip.getByRole('img', { name: /^Waiting over the last hour/ });
  await expect(waiting).toBeVisible();
  const before = await charts.boundingBox();
  const first = await waiting.getAttribute('aria-label');
  await control('settings-change', { fields: { 'sim.operators': 10 } });
  await control('trend-point');
  const marks = strip.getByRole('list', { name: 'Settings changes in this hour' });
  await expect(marks.getByRole('listitem')).toHaveCount(3);
  await expect(marks.getByRole('listitem').last()).toContainText('Simulated operators: 24 to 10');
  await expect.poll(() => waiting.getAttribute('aria-label')).not.toBe(first);
  // At most the 10 simulated operators that stay are online (some may be off shift).
  await expect(strip.getByRole('img', { name: /^Online operators over the last hour: now (\d|10),/ })).toBeVisible();
  expect(await charts.boundingBox()).toEqual(before);
});

test('on a phone the panel open does not make the page scroll sideways', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const panel = await openSettings(page);
  await panel.getByRole('textbox', { name: 'Load that forms a team' }).fill('600');
  await expect(page.getByRole('region', { name: 'Last hour' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
});
