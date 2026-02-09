import { sha256 } from '../lib/hash.js';

export function stableEntityId(prefix: string, notionId: string | null | undefined): string {
  if (!notionId) {
    return `${prefix}_unknown`;
  }
  const normalized = notionId.replace(/-/g, '').toLowerCase();
  return `${prefix}_${normalized}`;
}

export function timesliceIdFromPageId(pageId: string | null | undefined): string {
  return stableEntityId('timeslice', pageId);
}

export function normalizeNullableString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

export function normalizeNullableNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function sortKey(parts: Array<string | number | null | undefined>): string {
  return parts
    .map((part) => (part === null || part === undefined ? '' : String(part).trim()))
    .join('|')
    .toLowerCase();
}

export function digestAttributes(attributes: Record<string, unknown>): string {
  return sha256(JSON.stringify(attributes));
}
