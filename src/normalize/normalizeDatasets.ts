import type { RawRecord } from '../ingress/rawRecord.js';
import { buildWorkflowDefinition, type WorkflowDefinition } from '../canon/workflowDefinition.js';
import { buildWorkflowStage, type WorkflowStage } from '../canon/workflowStage.js';
import { buildTimeslice, type Timeslice } from '../canon/timeslice.js';
import { evaluateQuality } from './quality/index.js';
import type { QualityIssue, RunQualityReport } from './quality/types.js';

export interface NormalizedOutput {
  workflowDefinitions: WorkflowDefinition[];
  workflowStages: WorkflowStage[];
  timeslices: Timeslice[];
}

export interface NormalizedWithQuality extends NormalizedOutput {
  qualityIssues: QualityIssue[];
  qualityReport: RunQualityReport;
}

function pageRecords(records: RawRecord[]): RawRecord[] {
  return records.filter((record) => record.entityType === 'page' && Boolean(record.pageId));
}

export function normalizeDatasets(input: {
  workflowDefinitionsRaw: RawRecord[];
  workflowStagesRaw: RawRecord[];
  timeslicesRaw: RawRecord[];
}): NormalizedOutput {
  const workflowDefinitions = pageRecords(input.workflowDefinitionsRaw)
    .map((record) => buildWorkflowDefinition(record))
    .filter((value): value is WorkflowDefinition => value !== null);

  const workflowStages = pageRecords(input.workflowStagesRaw)
    .map((record) => buildWorkflowStage(record))
    .filter((value): value is WorkflowStage => value !== null);

  const timeslices = pageRecords(input.timeslicesRaw)
    .map((record) => buildTimeslice(record))
    .filter((value): value is Timeslice => value !== null);

  return {
    workflowDefinitions,
    workflowStages,
    timeslices
  };
}

export function normalizeAndValidateDatasets(input: {
  workflowDefinitionsRaw: RawRecord[];
  workflowStagesRaw: RawRecord[];
  timeslicesRaw: RawRecord[];
}): NormalizedWithQuality {
  const normalized = normalizeDatasets(input);
  const quality = evaluateQuality(normalized);
  const filteredTimeslices = normalized.timeslices.filter(
    (timeslice) => !quality.excludedTimesliceIds.has(timeslice.timeslice_id)
  );

  return {
    workflowDefinitions: normalized.workflowDefinitions,
    workflowStages: normalized.workflowStages,
    timeslices: filteredTimeslices,
    qualityIssues: quality.issues,
    qualityReport: quality.report
  };
}
