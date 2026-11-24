import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { expect, test, type Locator, type Page } from '@playwright/test';

import { control, offer, openDeskOnline } from './helpers.ts';

test.beforeEach(async () => {
  await control('reset');
});

/** Focuses a hint button, opens it with a key, and returns its text box. */
async function openWithKeyboard(page: Page, name: string, key = 'Enter'): Promise<Locator> {
  const button = page.getByRole('button', { name: `About: ${name}`, exact: true });
  await button.focus();
  await page.keyboard.press(key);
  const hint = page.getByRole('dialog', { name: `About: ${name}` });
  await expect(hint).toBeVisible();
  await expect(hint).toBeFocused();
  return hint;
}

/** The hint box lies fully inside the window. */
async function expectInsideWindow(page: Page, hint: Locator): Promise<void> {
  const box = await hint.boundingBox();
  const size = page.viewportSize();
  expect(box).not.toBeNull();
  expect(size).not.toBeNull();
  if (box === null || size === null) return;
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.y).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(size.width);
  expect(box.y + box.height).toBeLessThanOrEqual(size.height);
}

test('public page: a total explains itself from the keyboard', async ({ page }) => {
  await page.goto('#/');
  const hint = await openWithKeyboard(page, 'Messages per second');
  await expect(hint).toContainText('over the last 10 seconds');
  await page.keyboard.press('Escape');
  await expect(hint).toBeHidden();
  await expect(page.getByRole('button', { name: 'About: Messages per second' })).toBeFocused();
  // A column of the queues table, with Space.
  await expect(await openWithKeyboard(page, 'Oldest wait', ' ')).toContainText('waits longest');
});

test('supervisor: hints name the live settings and stay inside the window', async ({ page }) => {
  await page.setViewportSize({ width: 1400, height: 900 });
  await page.goto('#/supervisor');
  await expect(page.getByRole('rowheader', { name: /english/i })).toBeVisible();

  // The targets come from the welcome message of the gateway.
  const missed = await openWithKeyboard(page, 'Missed targets last hour');
  await expect(missed).toContainText('within 60 seconds after the conversation entered the queue');
  await expect(missed).toContainText('within 120 seconds');
  await page.keyboard.press('Escape');
  await expect(missed).toBeHidden();

  // A column header inside the scrolling queues table.
  const oldest = await openWithKeyboard(page, 'Oldest wait');
  await expect(oldest).toContainText('waits longest');
  await page.keyboard.press('Escape');
  await expect(oldest).toBeHidden();

  // The right edge of the window: the connection status in the header.
  const connection = await openWithKeyboard(page, 'Connection status');
  await expectInsideWindow(page, connection);
  await page.keyboard.press('Escape');
});

test('a hint opened right after another one closed stays open', async ({ page }) => {
  await page.goto('#/supervisor');
  await expect(page.getByRole('rowheader', { name: /english/i })).toBeVisible();
  // Radix gives the focus back to the first button a moment after Escape. It
  // must not take the focus from the second hint and close it. The timing
  // varies, so the steps run a few times.
  for (let i = 0; i < 6; i++) {
    await openWithKeyboard(page, 'Missed targets last hour');
    await page.keyboard.press('Escape');
    const hint = await openWithKeyboard(page, 'Oldest wait');
    await page.waitForTimeout(150);
    await expect(hint).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(hint).toBeHidden();
  }
});

test('supervisor: an open hint does not change the height of the queues and teams panels', async ({ page }) => {
  await page.setViewportSize({ width: 1400, height: 900 });
  await page.goto('#/supervisor');
  await expect(page.getByText('2 companies')).toBeVisible();
  await expect(page.getByRole('rowheader', { name: /english/i })).toBeVisible();
  const queues = page.getByRole('region', { name: 'Queues by language' });
  const teams = page.getByRole('region', { name: 'Dedicated teams' });
  const before = [(await queues.boundingBox())?.height, (await teams.boundingBox())?.height];
  await openWithKeyboard(page, 'Teams and the shared pool');
  await openWithKeyboard(page, 'Oldest wait');
  expect([(await queues.boundingBox())?.height, (await teams.boundingBox())?.height]).toEqual(before);
});

test.describe('on a phone', () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true });

  test('a hint opens with a tap, fits the screen, and a tap outside closes it', async ({ page }) => {
    await page.goto('#/supervisor');
    await expect(page.getByText('Messages per second')).toBeVisible();
    await page.getByRole('button', { name: 'About: Slots in use' }).tap();
    const hint = page.getByRole('dialog', { name: 'About: Slots in use' });
    await expect(hint).toContainText('A slot is room for one conversation.');
    await expectInsideWindow(page, hint);
    await page.getByRole('heading', { name: 'Floor overview' }).tap();
    await expect(hint).toBeHidden();
    await page.getByRole('button', { name: 'About: Demo level' }).tap();
    const demo = page.getByRole('dialog', { name: 'About: Demo level' });
    await expect(demo).toContainText('start with ps-');
    await expectInsideWindow(page, demo);
  });
});

test('operator: the offers hint names the accept timeout', async ({ page }) => {
  await openDeskOnline(page);
  await offer(page);
  const hint = await openWithKeyboard(page, 'Offers');
  await expect(hint).toContainText('Accept it within 20 seconds');
  await page.keyboard.press('Escape');
  await expect(hint).toBeHidden();
});

// The Content-Security-Policy of the deployed console, from the web server
// configuration. The preview server sends no such header, so it is added
// here to the page.
function deployedPolicy(): string {
  const conf = readFileSync(resolve(import.meta.dirname, '../../deploy/console.security-headers.conf'), 'utf8');
  const match = /add_header Content-Security-Policy "([^"]+)"/.exec(conf);
  if (match?.[1] === undefined) throw new Error('no Content-Security-Policy in the web server configuration');
  return match[1];
}

test('a hint works under the Content-Security-Policy of the deployed console', async ({ page }) => {
  const policy = deployedPolicy();
  await page.route(/\/(index\.html)?(\?.*)?$/, async (route) => {
    const response = await route.fetch();
    await route.fulfill({ response, headers: { ...response.headers(), 'content-security-policy': policy } });
  });
  await page.addInitScript(() => {
    const seen: string[] = [];
    (window as unknown as { cspViolations: string[] }).cspViolations = seen;
    document.addEventListener('securitypolicyviolation', (event) => seen.push(`${event.violatedDirective} ${event.blockedURI}`));
  });
  await page.goto('#/');
  await expect(page.getByText('Messages per second')).toBeVisible();
  const button = page.getByRole('button', { name: 'About: Typical wait' });
  await button.click();
  const hint = page.getByRole('dialog', { name: 'About: Typical wait' });
  await expect(hint).toBeVisible();
  // Positioned next to its button, so the positioning styles applied.
  const b = await button.boundingBox();
  const h = await hint.boundingBox();
  expect(b).not.toBeNull();
  expect(h).not.toBeNull();
  if (b !== null && h !== null) {
    expect(Math.abs(h.y - (b.y + b.height))).toBeLessThan(20);
    expect(h.x).toBeLessThan(b.x + b.width);
    expect(h.x + h.width).toBeGreaterThan(b.x);
  }
  expect(await page.evaluate(() => (window as unknown as { cspViolations: string[] }).cspViolations)).toEqual([]);
});
