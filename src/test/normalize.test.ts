import { beforeEach, describe, expect, it } from 'vitest';
import { normalizeDatasets } from '../normalize/normalizeDatasets.js';
import { sampleTimesliceRawRecord, sampleWorkflowStageRawRecord } from './fixtures.js';
import { overrideNotionPropertyIdsForTests } from '../config/env.js';

describe('normalizeDatasets', () => {
  beforeEach(() => {
    overrideNotionPropertyIdsForTests({
      timeslices: {
        workflowDefinitionRel: 'rel_workflow',
        fromStageRel: 'rel_from_step',
        toStageRel: 'rel_to_step',
        startedAtDate: 'start_date',
        endedAtDate: 'end_date'
      },
      workflowStages: {
        workflowDefinitionRel: 'wf_rel',
        stageNumber: 'stage_number',
        stageLabel: 'stage_label'
      },
      workflowDefinitions: {
        title: 'title_prop'
      }
    });
  });

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

  it('uses configured property IDs and ignores unrelated relation/date properties', () => {
    const rawTimeslice = sampleTimesliceRawRecord();

    // Point the workflowDefinitionRel to the "misleading_relation" property and verify it drives output.
    overrideNotionPropertyIdsForTests({
      timeslices: {
        workflowDefinitionRel: 'misleading_relation'
      }
    });

    const result = normalizeDatasets({
      workflowDefinitionsRaw: [],
      workflowStagesRaw: [],
      timeslicesRaw: [rawTimeslice]
    });

    expect(result.timeslices[0]?.workflow_definition_id).toBe(
      'workflow_definition_dddddddddddddddddddddddddddddddd'
    );
    // started_at should still come from configured start_date, not misleading_date.
    expect(result.timeslices[0]?.started_at).toBe('2026-02-01T12:00:00.000Z');
  });

  it('fails fast when required property IDs are unset', () => {
    overrideNotionPropertyIdsForTests({
      timeslices: {
        workflowDefinitionRel: ''
      }
    });

    expect(() =>
      normalizeDatasets({
        workflowDefinitionsRaw: [],
        workflowStagesRaw: [],
        timeslicesRaw: [sampleTimesliceRawRecord()]
      })
    ).toThrow(/Missing configured property IDs for timeslices/);
  });

  it('uses configured workflow stage property IDs and ignores distractors', () => {
    const rawStage = sampleWorkflowStageRawRecord();

    const result = normalizeDatasets({
      workflowDefinitionsRaw: [],
      workflowStagesRaw: [rawStage],
      timeslicesRaw: []
    });

    expect(result.workflowStages).toHaveLength(1);
    expect(result.workflowStages[0]?.workflow_definition_id).toBe(
      'workflow_definition_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    );
    expect(result.workflowStages[0]?.stage_number).toBe(3);
    expect(result.workflowStages[0]?.stage_label).toBe('Approved');
  });
});
