// Synthetic data of the fake gateway. Every ID is invented and says so
// ("synth"), and every text is a made-up sample sentence (decision 0004).

import { createHmac } from 'node:crypto';

import type { Rng } from './rng.ts';

export const LANGUAGES = ['en', 'de', 'es', 'ja', 'pt', 'und'] as const;
const LANGUAGE_WEIGHTS = [44, 12, 12, 14, 8, 10];

export function pickLanguage(rng: Rng): string {
  const total = LANGUAGE_WEIGHTS.reduce((a, b) => a + b, 0);
  let roll = rng.next() * total;
  for (let i = 0; i < LANGUAGES.length; i += 1) {
    roll -= LANGUAGE_WEIGHTS[i] ?? 0;
    if (roll < 0) return LANGUAGES[i] ?? 'en';
  }
  return 'en';
}

export function companyId(n: number): string {
  return `did:plc:synthcompany${n.toString().padStart(4, '0')}`;
}

export function customerId(n: number): string {
  return `did:plc:synthcustomer${n.toString().padStart(6, '0')}`;
}

export function conversationKey(company: string, customer: string): string {
  return `${company}|${customer}`;
}

const CUSTOMER_LINES = [
  'Sample question: my order has not arrived yet, can you check it?',
  'Sample question: the app logs me out after every update.',
  'Sample question: how do I change the address on my account?',
  'Sample question: I was charged twice this month.',
  'Sample question: the tracking page shows no news since Monday.',
  'Sample follow-up: any news on this?',
  'Sample follow-up: thanks, that worked.',
  'Sample question: is the store open on the holiday?',
];

const COMPANY_LINES = [
  'Sample answer: we are looking into it now.',
  'Sample answer: could you send us the order number in a direct message?',
  'Sample answer: a fix is on the way in the next release.',
];

export function sampleText(rng: Rng, direction: 'customer' | 'company'): string {
  return rng.pick(direction === 'customer' ? CUSTOMER_LINES : COMPANY_LINES);
}

const BASE32 = 'abcdefghijklmnopqrstuvwxyz234567';

/** Same form as gwapi.Pseudonym: "ps-" and 12 base32 characters of an HMAC. */
export function pseudonym(dayKey: string, id: string): string {
  const sum = createHmac('sha256', dayKey).update(id).digest();
  let bits = 0;
  let value = 0;
  let out = '';
  for (const byte of sum) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5 && out.length < 12) {
      out += BASE32.charAt((value >>> (bits - 5)) & 31);
      bits -= 5;
    }
    if (out.length >= 12) break;
  }
  return `ps-${out}`;
}

export const PSEUDONYM_RE = /^ps-[a-z2-7]{12}$/;
