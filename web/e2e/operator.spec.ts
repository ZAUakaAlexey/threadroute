import { expect, test, type Locator } from '@playwright/test';

import { control, offer, openDeskOnline, signInFull } from './helpers.ts';

test.beforeEach(async () => {
  await control('reset');
});

test('accepts an offer before the deadline and opens the conversation', async ({ page }) => {
  await openDeskOnline(page);
  const card = await offer(page);
  await expect(card.getByRole('timer', { name: /time to accept/i })).toHaveText(/^0:\d\d$/);
  await card.getByRole('button', { name: 'Accept' }).click();
  await expect(card).toBeHidden();
  await expect(page.getByRole('list', { name: 'Conversations you hold' }).getByRole('listitem')).toHaveCount(1);
  await expect(page.getByRole('list', { name: 'Messages' })).toContainText('Text hidden at the demo level');
});

test('after the accept, shows a fixed Waited and a running In work instead of Waiting', async ({ page }) => {
  await openDeskOnline(page);
  const card = await offer(page);
  const cardWaiting = seconds(await card.getByRole('timer', { name: /waiting time/i }).textContent());
  await card.getByRole('button', { name: 'Accept' }).click();
  await expect(card).toBeHidden();

  const header = page.getByRole('term').filter({ hasText: 'Waited' }).locator('xpath=..');
  const waited = header.getByRole('definition').locator('time');
  await expect(waited).toHaveText(/^\d+:\d\d$/);
  const inWork = page.getByRole('definition').getByRole('timer', { name: /time in work/i });
  await expect(inWork).toHaveText(/^\d+:\d\d$/);
  await expect(page.getByRole('term').filter({ hasText: /^Waiting/ })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'About: In work' })).toBeVisible();
  // Model time, as on the offer card: at least what the card showed before the accept.
  const fixed = await waited.textContent();
  expect(seconds(fixed)).toBeGreaterThanOrEqual(cardWaiting);
  const workBefore = await inWork.textContent();
  await page.waitForTimeout(1500);
  await expect(waited).toHaveText(fixed ?? '');
  await expect(inWork).not.toHaveText(workBefore ?? '');

  const row = page.getByRole('list', { name: 'Conversations you hold' }).getByRole('listitem').first();
  await expect(row).toContainText(new RegExp(`waited ${fixed ?? ''}, in work \\d+:\\d\\d`));
  await expect(row.getByRole('timer', { name: /waiting time/i })).toHaveCount(0);
  for (const width of [390, 1400]) {
    await page.setViewportSize({ width, height: 900 });
    await expectRowFits(row);
  }
});

test('lets an offer expire', async ({ page }) => {
  await openDeskOnline(page);
  const card = await offer(page);
  // 20 s of model time at speed 5 are 4 s on the clock.
  await expect(card).toBeHidden({ timeout: 8_000 });
  await expect(page.getByText(/No offer right now/)).toBeVisible();
  await expect(page.getByText('Accepted conversations appear here.')).toBeVisible();
});

test('marks replied and resolves', async ({ page }) => {
  await openDeskOnline(page);
  const card = await offer(page);
  await card.getByRole('button', { name: 'Accept' }).click();
  await page.getByRole('button', { name: 'Mark replied' }).click();
  // Exact: the held conversation row also says Replied once its badge shows.
  await expect(page.getByRole('button', { name: 'Replied', exact: true })).toBeDisabled();
  await page.getByRole('button', { name: 'Resolve' }).click();
  await expect(page.getByText('No conversation open.')).toBeVisible();
  await expect(page.getByText('Accepted conversations appear here.')).toBeVisible();
});

test('drops a removed text at once at the full level', async ({ page }) => {
  await signInFull(page);
  await openDeskOnline(page);
  await expect(page.getByRole('button', { name: 'Full level' })).toBeVisible();
  const card = await offer(page);
  await card.getByRole('button', { name: 'Accept' }).click();
  const messages = page.getByRole('list', { name: 'Messages' });
  await expect(messages).toContainText('Sample question');
  await control('remove-message');
  await expect(messages).toContainText('Message removed: the author deleted it.');
  await expect(messages).not.toContainText('Sample question');
  const stored = await page.evaluate(() =>
    [window.localStorage, window.sessionStorage]
      .flatMap((storage) => Object.keys(storage).map((key) => `${key}=${storage.getItem(key) ?? ''}`))
      .join('\n'),
  );
  expect(stored).not.toContain('Sample');
});

test('shows a new customer message of the open conversation', async ({ page }) => {
  await openDeskOnline(page);
  const card = await offer(page);
  await card.getByRole('button', { name: 'Accept' }).click();
  const messages = page.getByRole('list', { name: 'Messages' });
  await expect(messages.getByRole('listitem')).toHaveCount(1);
  await control('customer-message');
  await expect(messages.getByRole('listitem')).toHaveCount(2);
});

test('shows stale data, stops the timers, and recovers', async ({ page }) => {
  await openDeskOnline(page);
  const card = await offer(page);
  await card.getByRole('button', { name: 'Accept' }).click();
  await control('stale', { stale: true });
  await expect(page.getByRole('status').filter({ hasText: 'The data is stale.' })).toBeVisible();
  const inWork = page.getByRole('timer', { name: /time in work/i }).last();
  const before = await inWork.textContent();
  await page.waitForTimeout(1500);
  expect(await inWork.textContent()).toBe(before);
  await control('stale', { stale: false });
  await expect(page.getByText('The data is stale.')).toBeHidden();
  await expect(inWork).not.toHaveText(before ?? '');
});

test('reconnects after the gateway restarts and keeps the operator', async ({ page }) => {
  await openDeskOnline(page);
  const operatorId = await page.getByText(/^web-synth\d{4}$/).textContent();
  await control('close', { code: 1001 });
  await expect(page.getByRole('switch', { name: 'Online' })).toBeChecked({ timeout: 10_000 });
  await expect(page.getByText('Live', { exact: true })).toBeVisible({ timeout: 10_000 });
  await expect(page.getByText(/^web-synth\d{4}$/)).toHaveText(operatorId ?? '');
});

/** "1:05" in seconds. */
function seconds(clock: string | null): number {
  const [minutes = '0', secs = '0'] = (clock ?? '').split(':');
  return Number(minutes) * 60 + Number(secs);
}

/** The held row keeps its height, and its text stays left of the badge. */
async function expectRowFits(row: Locator): Promise<void> {
  const button = row.getByRole('button');
  const box = await button.boundingBox();
  expect(box?.height).toBe(68);
  const line = row.getByText(/^waited /);
  const badge = button.locator(':scope > :last-child');
  const lineBox = await line.boundingBox();
  const messagesBox = await row.getByText(/^\d+ messages?$/).boundingBox();
  const badgeBox = await badge.boundingBox();
  expect(lineBox?.height).toBeLessThanOrEqual(20);
  expect((lineBox?.x ?? 0) + (lineBox?.width ?? 0)).toBeLessThanOrEqual(badgeBox?.x ?? 0);
  expect((messagesBox?.x ?? 0) + (messagesBox?.width ?? 0)).toBeLessThanOrEqual(badgeBox?.x ?? 0);
}
