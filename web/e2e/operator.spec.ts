import { expect, test } from '@playwright/test';

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
  await expect(page.getByRole('button', { name: 'Replied' })).toBeDisabled();
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
  const waiting = page.getByRole('timer', { name: /waiting time/i }).last();
  const before = await waiting.textContent();
  await page.waitForTimeout(1500);
  expect(await waiting.textContent()).toBe(before);
  await control('stale', { stale: false });
  await expect(page.getByText('The data is stale.')).toBeHidden();
  await expect(waiting).not.toHaveText(before ?? '');
});

test('reconnects after the gateway restarts and keeps the operator', async ({ page }) => {
  await openDeskOnline(page);
  const operatorId = await page.getByText(/^web-synth\d{4}$/).textContent();
  await control('close', { code: 1001 });
  await expect(page.getByRole('switch', { name: 'Online' })).toBeChecked({ timeout: 10_000 });
  await expect(page.getByText('Live', { exact: true })).toBeVisible({ timeout: 10_000 });
  await expect(page.getByText(/^web-synth\d{4}$/)).toHaveText(operatorId ?? '');
});
