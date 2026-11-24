import { expect, test } from '@playwright/test';

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
