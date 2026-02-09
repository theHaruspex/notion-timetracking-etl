import { describe, expect, it } from 'vitest';
import { stableEntityId, timesliceIdFromPageId } from '../canon/rules.js';

describe('canonical key rules', () => {
  it('normalizes notion IDs into stable IDs', () => {
    const source = '12345678-1234-1234-1234-1234567890ab';
    expect(stableEntityId('workflow_definition', source)).toBe(
      'workflow_definition_123456781234123412341234567890ab'
    );
  });

  it('builds timeslice IDs from notion page IDs', () => {
    const source = 'abcdefab-cdef-cdef-cdef-abcdefabcdef';
    expect(timesliceIdFromPageId(source)).toBe('timeslice_abcdefabcdefcdefcdefabcdefabcdef');
  });
});
