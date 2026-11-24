import { expect, test, type Page } from '@playwright/test';

import { control, signInFull } from './helpers.ts';

test.beforeEach(async () => {
  await control('reset');
});

test('supervisor sees flow, queues, operators and filters conversations', async ({ page }) => {
  await page.goto('#/supervisor');
  await expect(page.getByText('Messages per second')).toBeVisible();
  await expect(page.getByRole('table', { name: 'Operators' }).getByRole('row').nth(5)).toBeVisible();
  const conversations = page.getByRole('table', { name: 'Conversations' });
  await expect(conversations.getByRole('row').nth(3)).toBeVisible();
  // The fake sends three rows per snapshot frame; every frame was applied.
  const shown = page.getByText(/^\d+ of \d+ shown$/);
  await expect(shown).toHaveText(/^(\d+) of \1 shown$/);
  expect(Number((await shown.textContent())?.split(' ')[0])).toBeGreaterThan(3);

  await page.getByLabel('Filter by state').selectOption('resolved');
  await expect(conversations.getByRole('row')).toHaveCount(1);
  await expect(page.getByText('No conversation matches the filters.')).toBeVisible();
  await page.getByLabel('Filter by state').selectOption('all');

  // Offline operators sort last, so scroll the virtualized table to its end.
  const meta = page.getByText(/^\d+ of \d+ online$/);
  const before = Number((await meta.textContent())?.split(' ')[0]);
  await control('operator-offline', { id: 'sim-op-0005' });
  await expect(meta).toHaveText(new RegExp(`^${before - 1} of`));
  await page.getByRole('group', { name: 'Operators, scrollable' }).evaluate((el) => el.scrollTo({ top: el.scrollHeight }));
  const row = page.getByRole('table', { name: 'Operators' }).getByRole('row').filter({ hasText: 'sim-op-0005' });
  await expect(row).toContainText('offline');
});

test('supervisor at the full level reads messages in a dialog', async ({ page }) => {
  await signInFull(page);
  await page.goto('#/supervisor');
  await page.getByRole('button', { name: /view messages/i }).first().click();
  const dialog = page.getByRole('dialog');
  await expect(dialog.getByRole('list', { name: 'Messages' })).toContainText('Sample');
  // A new snapshot in several frames ends every watch; the console watches
  // the shown conversation again and keeps the dialog open.
  const title = await dialog.getByRole('heading').textContent();
  await control('snapshot');
  await expect(dialog.getByRole('heading')).toHaveText(title ?? '');
  await expect(dialog.getByRole('list', { name: 'Messages' })).toContainText('Sample');
  await page.keyboard.press('Escape');
  await expect(dialog).toBeHidden();
});

test('the public page shows totals only, no keys and no text', async ({ page }) => {
  await page.goto('#/');
  await expect(page.getByText('Messages per second')).toBeVisible();
  await expect(page.getByRole('rowheader', { name: /english/i })).toBeVisible();
  const text = await page.locator('body').innerText();
  expect(text).not.toMatch(/ps-[a-z2-7]{12}|did:|sim-op|web-synth|Sample/);
  const snapshot = await page.request.get('v1/public/snapshot');
  expect(await snapshot.text()).not.toMatch(/ps-|did:|sim-op|web-|Sample/);
  await page.getByRole('link', { name: /try the operator role/i }).click();
  await expect(page.getByRole('heading', { name: 'Your desk' })).toBeVisible();
});

test.describe('teams panel next to the queues panel', () => {
  test.use({ viewport: { width: 1400, height: 900 } });

  async function heights(page: Page) {
    const queues = await page.getByRole('region', { name: 'Queues by language' }).boundingBox();
    const teams = await page.getByRole('region', { name: 'Dedicated teams' }).boundingBox();
    expect(queues).not.toBeNull();
    expect(teams).not.toBeNull();
    return { queues: queues?.height ?? 0, teams: teams?.height ?? 0 };
  }

  test('has the height of the queues panel with few teams and does not scroll', async ({ page }) => {
    await page.goto('#/supervisor');
    await expect(page.getByText('2 companies')).toBeVisible();
    await expect(page.getByRole('rowheader', { name: /english/i })).toBeVisible();
    const h = await heights(page);
    expect(Math.abs(h.teams - h.queues)).toBeLessThanOrEqual(1);
    const list = page.getByRole('group', { name: 'Dedicated teams, scrollable' });
    expect(await list.evaluate((el) => el.scrollHeight - el.clientHeight)).toBeLessThanOrEqual(0);
  });

  test('keeps the height of the queues panel with many teams and scrolls the list', async ({ page }) => {
    await page.goto('#/supervisor');
    await expect(page.getByText('2 companies')).toBeVisible();
    await control('teams', { count: 60 });
    await expect(page.getByText('62 companies')).toBeVisible();
    await expect(page.getByRole('rowheader', { name: /english/i })).toBeVisible();
    const h = await heights(page);
    expect(Math.abs(h.teams - h.queues)).toBeLessThanOrEqual(1);
    const list = page.getByRole('group', { name: 'Dedicated teams, scrollable' });
    expect(await list.evaluate((el) => el.scrollHeight > el.clientHeight)).toBe(true);

    // The keyboard scrolls the list and the header stays in view.
    await list.focus();
    await page.keyboard.press('End');
    await expect.poll(() => list.evaluate((el) => el.scrollTop)).toBeGreaterThan(0);
    await expect(page.getByRole('heading', { name: 'Dedicated teams' })).toBeInViewport();
    expect(Math.abs((await heights(page)).teams - h.queues)).toBeLessThanOrEqual(1);
  });

  test('with many languages, the queues table stops at its cap and scrolls under its column header', async ({ page }) => {
    await page.goto('#/supervisor');
    await expect(page.getByText('2 companies')).toBeVisible();
    const scroll = page.getByRole('group', { name: 'Queues by language, scrollable' });
    const table = page.getByRole('table', { name: 'Queues by language' });
    await expect(table.getByRole('rowheader', { name: /english/i })).toBeVisible();
    // Six languages fit: no scroll, and the panel is only as high as its rows.
    expect(await scroll.evaluate((el) => el.scrollHeight - el.clientHeight)).toBeLessThanOrEqual(0);
    expect(await scroll.evaluate((el) => el.clientHeight)).toBeLessThan(420);

    await control('languages', { count: 30 });
    await control('teams', { count: 60 });
    await expect(table.getByRole('row')).toHaveCount(37);
    await expect(page.getByText('62 companies')).toBeVisible();
    expect(await scroll.evaluate((el) => el.clientHeight)).toBeLessThanOrEqual(420);
    expect(await scroll.evaluate((el) => el.scrollHeight > el.clientHeight)).toBe(true);
    const h = await heights(page);
    expect(h.queues).toBeLessThanOrEqual(480);
    expect(Math.abs(h.teams - h.queues)).toBeLessThanOrEqual(1);

    // The keyboard scrolls the rows; the column header stays at the top of the area.
    await scroll.focus();
    await page.keyboard.press('End');
    await expect.poll(() => scroll.evaluate((el) => el.scrollTop)).toBeGreaterThan(0);
    await expect(table.getByRole('row').last()).toBeInViewport();
    const header = table.getByRole('columnheader', { name: 'Language' });
    await expect(header).toBeInViewport();
    const headerTop = (await header.boundingBox())?.y ?? -1;
    const scrollTop = (await scroll.boundingBox())?.y ?? -2;
    expect(Math.abs(headerTop - scrollTop)).toBeLessThanOrEqual(1);
    await expect(page.getByRole('heading', { name: 'Queues by language' })).toBeInViewport();
    expect(Math.abs((await heights(page)).teams - h.queues)).toBeLessThanOrEqual(1);
  });

  test('with many languages and few teams, both panels keep the same height', async ({ page }) => {
    await page.goto('#/supervisor');
    await expect(page.getByText('2 companies')).toBeVisible();
    await control('languages', { count: 30 });
    await expect(page.getByRole('table', { name: 'Queues by language' }).getByRole('row')).toHaveCount(37);
    const h = await heights(page);
    expect(h.queues).toBeLessThanOrEqual(480);
    expect(Math.abs(h.teams - h.queues)).toBeLessThanOrEqual(1);
  });

  test('a large team shows a bounded member list with a button for the rest', async ({ page }) => {
    await page.goto('#/supervisor');
    await expect(page.getByText('2 companies')).toBeVisible();
    await control('teams', { count: 1, size: 20 });
    await expect(page.getByText('3 companies')).toBeVisible();
    // At the demo level the company shows as a pseudonym, so find the row by its size.
    const row = page.getByRole('list', { name: 'Dedicated teams' }).getByRole('listitem').filter({ hasText: '20 operators, 0 active conversations' });
    await expect(row).toHaveCount(1);
    const members = row.getByRole('list', { name: /^Members of/ });
    await expect(members.getByRole('listitem')).toHaveCount(6);
    // The summary stays on one line at desktop width.
    const summary = row.getByText(/^20 operators,/);
    expect((await summary.boundingBox())?.height ?? 99).toBeLessThan(20);
    const more = members.getByRole('button', { name: /^\+15 more members of/ });
    await more.focus();
    await page.keyboard.press('Enter');
    await expect(members.getByRole('listitem')).toHaveCount(21);
    await expect(members.getByRole('button', { name: /^show fewer/ })).toHaveAttribute('aria-expanded', 'true');
  });

  test('stacked on a narrow screen, the list stops at its maximum height', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto('#/supervisor');
    await control('teams', { count: 60 });
    await expect(page.getByText('62 companies')).toBeVisible();
    const list = page.getByRole('group', { name: 'Dedicated teams, scrollable' });
    expect(await list.evaluate((el) => el.clientHeight)).toBeLessThanOrEqual(360);
    expect(await list.evaluate((el) => el.scrollHeight > el.clientHeight)).toBe(true);
  });
});
