import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';

import { control, offer, openDeskOnline } from './helpers.ts';

test.beforeEach(async () => {
  await control('reset');
});

async function expectNoViolations(page: Page): Promise<void> {
  const results = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa', 'best-practice']).analyze();
  const summary = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`);
  expect(summary).toEqual([]);
}

for (const colorScheme of ['light', 'dark'] as const) {
  test.describe(`${colorScheme} theme`, () => {
    test.use({ colorScheme });

    test('public page', async ({ page }) => {
      await page.goto('#/');
      await expect(page.getByText('Messages per second')).toBeVisible();
      await expectNoViolations(page);
    });

    test('operator desk with an offer and a conversation', async ({ page }) => {
      await openDeskOnline(page);
      const first = await offer(page);
      await first.getByRole('button', { name: 'Accept' }).click();
      await expect(page.getByRole('list', { name: 'Messages' })).toBeVisible();
      await offer(page);
      await expectNoViolations(page);
    });

    test('supervisor view with a long, scrolling teams list', async ({ page }) => {
      await page.setViewportSize({ width: 1400, height: 900 });
      await page.goto('#/supervisor');
      await control('teams', { count: 60 });
      await expect(page.getByText('62 companies')).toBeVisible();
      await expect(page.getByRole('table', { name: 'Conversations' }).getByRole('row').nth(3)).toBeVisible();
      await expectNoViolations(page);
      await page.getByLabel('Filter by language').focus();
      await expectNoViolations(page);
    });

    test('supervisor view and its dialog', async ({ page }) => {
      await page.goto('#/supervisor');
      await expect(page.getByRole('table', { name: 'Conversations' }).getByRole('row').nth(3)).toBeVisible();
      await expectNoViolations(page);
      await page.getByRole('button', { name: /view messages/i }).first().click();
      await expect(page.getByRole('dialog')).toBeVisible();
      await expectNoViolations(page);
    });
  });
}
