// Small line icons. Decorative unless a label is given.

import type { ReactNode, SVGProps } from 'react';

interface IconProps extends Omit<SVGProps<SVGSVGElement>, 'children'> {
  readonly label?: string;
}

function Svg({ label, children, ...props }: IconProps & { readonly children: ReactNode }) {
  const a11y = label === undefined ? { 'aria-hidden': true } : { role: 'img', 'aria-label': label };
  return (
    <svg
      viewBox="0 0 16 16"
      width="16"
      height="16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      {...a11y}
      {...props}
    >
      {children}
    </svg>
  );
}

/** The mark: two stops joined by a bent route. */
export function RouteMark(props: IconProps) {
  return (
    <Svg {...props}>
      <circle cx="3.5" cy="3.5" r="1.75" />
      <circle cx="12.5" cy="12.5" r="1.75" />
      <path d="M3.5 5.25v3.25a2 2 0 0 0 2 2h5.25" />
    </Svg>
  );
}

export function CheckIcon(props: IconProps) {
  return (
    <Svg {...props}>
      <path d="m3.5 8.5 3 3 6-7" />
    </Svg>
  );
}

export function CloseIcon(props: IconProps) {
  return (
    <Svg {...props}>
      <path d="m4 4 8 8M12 4l-8 8" />
    </Svg>
  );
}

export function ChevronDownIcon(props: IconProps) {
  return (
    <Svg {...props}>
      <path d="m4 6 4 4 4-4" />
    </Svg>
  );
}

export function AlertIcon(props: IconProps) {
  return (
    <Svg {...props}>
      <path d="M8 2.5 14 13H2L8 2.5Z" />
      <path d="M8 6.5v3M8 11.25v.01" />
    </Svg>
  );
}

export function InfoIcon(props: IconProps) {
  return (
    <Svg {...props}>
      <circle cx="8" cy="8" r="6" />
      <path d="M8 7.5v3.5M8 5.25v.01" />
    </Svg>
  );
}

export function ArrowRightIcon(props: IconProps) {
  return (
    <Svg {...props}>
      <path d="M3 8h10M9 4l4 4-4 4" />
    </Svg>
  );
}

export function EyeIcon(props: IconProps) {
  return (
    <Svg {...props}>
      <path d="M1.5 8S4 3.5 8 3.5 14.5 8 14.5 8 12 12.5 8 12.5 1.5 8 1.5 8Z" />
      <circle cx="8" cy="8" r="2" />
    </Svg>
  );
}
