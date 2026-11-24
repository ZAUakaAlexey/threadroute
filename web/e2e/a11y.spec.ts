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

    test('supervisor view with long, scrolling queues and teams lists', async ({ page }) => {
      await page.setViewportSize({ width: 1400, height: 900 });
      await page.goto('#/supervisor');
      await control('teams', { count: 1, size: 20 });
      await control('teams', { count: 60 });
      await control('languages', { count: 30 });
      await expect(page.getByText('63 companies')).toBeVisible();
      await expect(page.getByRole('table', { name: 'Queues by language' }).getByRole('row')).toHaveCount(37);
      await expect(page.getByRole('table', { name: 'Conversations' }).getByRole('row').nth(3)).toBeVisible();
      await expectNoViolations(page);
      await page.getByLabel('Filter by language').focus();
      await expectNoViolations(page);
      // The scroll areas are reachable with the keyboard and keep their names.
      const queues = page.getByRole('group', { name: 'Queues by language, scrollable' });
      await queues.focus();
      await expect(queues).toBeFocused();
      await page.keyboard.press('End');
      await expectNoViolations(page);
      const more = page.getByRole('button', { name: /^\+15 more members of/ });
      await more.focus();
      await page.keyboard.press('Enter');
      await expect(page.getByRole('button', { name: /^show fewer members of/ })).toBeFocused();
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
