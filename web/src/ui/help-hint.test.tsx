import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { HELP_TEXTS, helpText } from '../lib/help-texts.ts';
import { HelpHint } from './help-hint.tsx';
import { HelpValuesContext } from './help-values.ts';

const TEXT = HELP_TEXTS['supervisor.queues.oldest'];

function renderHint() {
  render(
    <>
      <span>Oldest wait</span>
      <HelpHint id="supervisor.queues.oldest" label="Oldest wait" />
      <button type="button">Next</button>
    </>,
  );
  return screen.getByRole('button', { name: 'About: Oldest wait' });
}

describe('HelpHint', () => {
  it('is a closed button named after its label', () => {
    const button = renderHint();
    expect(button).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('opens on click with a name and a description, and moves the focus into the text', async () => {
    const button = renderHint();
    await userEvent.click(button);
    const dialog = await screen.findByRole('dialog', { name: 'About: Oldest wait' });
    expect(dialog).toHaveAccessibleDescription(TEXT);
    expect(button).toHaveAttribute('aria-expanded', 'true');
    expect(button).toHaveAccessibleDescription(TEXT);
    expect(dialog).toHaveFocus();
  });

  it('closes on Escape and gives the focus back to the button', async () => {
    const button = renderHint();
    await userEvent.click(button);
    await screen.findByRole('dialog');
    await userEvent.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(button).toHaveFocus();
  });

  it('opens with Enter and with Space from the keyboard', async () => {
    const button = renderHint();
    await userEvent.tab();
    expect(button).toHaveFocus();
    await userEvent.keyboard('{Enter}');
    expect(await screen.findByRole('dialog', { name: 'About: Oldest wait' })).toHaveTextContent(TEXT);
    await userEvent.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await userEvent.keyboard(' ');
    expect(await screen.findByRole('dialog', { name: 'About: Oldest wait' })).toBeInTheDocument();
  });

  it('closes on Tab and puts the focus back on the button, not at the end of the page', async () => {
    const button = renderHint();
    await userEvent.click(button);
    await screen.findByRole('dialog');
    await userEvent.tab();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(button).toHaveFocus();
  });

  it('closes on a second click and on a click outside', async () => {
    const button = renderHint();
    await userEvent.click(button);
    await screen.findByRole('dialog');
    await userEvent.click(button);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await userEvent.click(button);
    await screen.findByRole('dialog');
    await userEvent.click(screen.getByText('Oldest wait'));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('shows on hover without taking the focus, and hides when the pointer leaves', async () => {
    const button = renderHint();
    await userEvent.hover(button);
    expect(await screen.findByRole('dialog')).toHaveTextContent(TEXT);
    expect(button).not.toHaveFocus();
    await userEvent.unhover(button);
    await act(() => new Promise((resolve) => setTimeout(resolve, 300)));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('stays open after a click on a hovered button, also when the pointer leaves', async () => {
    const button = renderHint();
    await userEvent.hover(button);
    await screen.findByRole('dialog');
    await userEvent.click(button);
    await userEvent.unhover(button);
    await act(() => new Promise((resolve) => setTimeout(resolve, 300)));
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it('opens on a tap', async () => {
    const button = renderHint();
    await userEvent.pointer({ keys: '[TouchA]', target: button });
    expect(await screen.findByRole('dialog', { name: 'About: Oldest wait' })).toBeInTheDocument();
  });

  it('names the live values, and plain words while they are not known', async () => {
    render(
      <HelpValuesContext value={{ assignmentTarget: '60 seconds' }}>
        <HelpHint id="supervisor.queues.missed" label="Missed" />
      </HelpValuesContext>,
    );
    await userEvent.click(screen.getByRole('button', { name: 'About: Missed' }));
    expect(await screen.findByRole('dialog')).toHaveTextContent('missed the assignment target of 60 seconds.');
    expect(helpText('supervisor.queues.missed')).toContain('missed the assignment target of the time set by the router.');
  });

  it('can show a badge with visible text that is part of its name', () => {
    render(
      <HelpHint id="live.level.demo" label="Demo level">
        Demo level
      </HelpHint>,
    );
    expect(screen.getByRole('button', { name: 'About: Demo level' })).toHaveTextContent('Demo level');
  });
});
