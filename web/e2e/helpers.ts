import { expect, type Page } from '@playwright/test';

import { GATEWAY_URL, SITE_TOKEN_KEY } from '../playwright.config.ts';

/** Calls the control API of the fake gateway. */
export async function control(name: string, body: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
  const response = await fetch(`${GATEWAY_URL}/__control/${name}`, { method: 'POST', body: JSON.stringify(body) });
  expect(response.ok).toBe(true);
  return (await response.json()) as Record<string, unknown>;
}

/** Signs the page in at the full level (text visible), before it loads. */
export async function signInFull(page: Page): Promise<void> {
  await page.addInitScript(
    ([key, token]) => {
      window.localStorage.setItem(key, token);
    },
    [SITE_TOKEN_KEY, 'synthetic.full.token'] as const,
  );
}

/** Opens the operator desk and goes online. */
export async function openDeskOnline(page: Page): Promise<void> {
  await page.goto('#/operator');
  const toggle = page.getByRole('switch', { name: 'Online' });
  await expect(toggle).toBeEnabled();
  await toggle.click();
  await expect(toggle).toBeChecked();
}

/** Makes an offer to the online operator and returns its card. */
export async function offer(page: Page) {
  const result = await control('offer');
  expect(result.ok).toBe(true);
  const card = page.getByRole('article', { name: /new conversation/i });
  await expect(card).toBeVisible();
  return card;
}
