import { render, screen } from '@testing-library/react';

import { Meter } from './stats.tsx';

/** The widths of the bar parts, in order: solid first, then the offered part. */
function partWidths(meter: HTMLElement): string[] {
  return Array.from(meter.children).map((part) => (part as HTMLElement).style.width);
}

describe('Meter', () => {
  it('keeps the plain label and one part without the offered prop', () => {
    render(<Meter value={1} max={2} label="Your load against capacity" />);
    const meter = screen.getByRole('meter', { name: 'Your load against capacity' });
    expect(meter).toHaveAttribute('aria-valuenow', '1');
    expect(partWidths(meter)).toEqual(['50%']);
  });

  it('shows one solid part and no offered words with offered 0', () => {
    render(<Meter value={3} max={4} offered={0} label="Load of op-012" />);
    const meter = screen.getByRole('meter', { name: 'Load of op-012: 3 of 4' });
    expect(partWidths(meter)).toEqual(['75%']);
    expect(meter.firstElementChild).toHaveClass('bg-accent');
  });

  it('shows the offered part in the lighter tone next to the solid part', () => {
    render(<Meter value={4} max={4} offered={2} label="Load of op-012" />);
    const meter = screen.getByRole('meter', { name: 'Load of op-012: 4 of 4, 2 of them offered' });
    expect(meter).toHaveAttribute('aria-valuenow', '4');
    expect(partWidths(meter)).toEqual(['50%', '50%']);
    expect(meter.children[0]).toHaveClass('bg-wait');
    expect(meter.children[1]).toHaveClass('bg-wait-muted');
  });

  it('shows only the offered part when offered equals the load', () => {
    render(<Meter value={2} max={4} offered={2} label="Load of op-012" />);
    const meter = screen.getByRole('meter', { name: 'Load of op-012: 2 of 4, 2 of them offered' });
    expect(partWidths(meter)).toEqual(['0%', '50%']);
    expect(meter.children[1]).toHaveClass('bg-accent-muted');
  });

  it('treats offered above the load as the whole load', () => {
    render(<Meter value={1} max={4} offered={3} label="Load of op-012" />);
    const meter = screen.getByRole('meter', { name: 'Load of op-012: 1 of 4, 1 of them offered' });
    expect(partWidths(meter)).toEqual(['0%', '25%']);
  });

  it('never renders a negative width over capacity', () => {
    render(<Meter value={6} max={4} offered={2} label="Load of op-012" />);
    const meter = screen.getByRole('meter', { name: 'Load of op-012: 6 of 4, 2 of them offered' });
    expect(partWidths(meter)).toEqual(['100%']);
    expect(meter.firstElementChild).toHaveClass('bg-late');
  });
});
