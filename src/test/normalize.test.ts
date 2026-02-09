import { describe, expect, it } from 'vitest';
import { normalizeDatasets } from '../normalize/normalizeDatasets.js';
import { sampleTimesliceRawRecord } from './fixtures.js';

describe('normalizeDatasets', () => {
  it('creates canonical timeslice rows from raw page records', () => {
    const rawTimeslice = sampleTimesliceRawRecord();

    const result = normalizeDatasets({
      workflowDefinitionsRaw: [],
      workflowStagesRaw: [],
      timeslicesRaw: [rawTimeslice]
    });

    expect(result.timeslices).toHaveLength(1);
    expect(result.timeslices[0]?.timeslice_id).toBe(
      'timeslice_123456781234123412341234567890ab'
    );
    expect(result.timeslices[0]?.duration_seconds).toBe(300);
    expect(result.timeslices[0]?.workflow_definition_id).toBe(
      'workflow_definition_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    );
  });
});
