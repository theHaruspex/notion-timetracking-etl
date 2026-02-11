import type { Timeslice } from '../../canon/timeslice.js';
import type { WorkflowDefinition } from '../../canon/workflowDefinition.js';
import type { WorkflowStage } from '../../canon/workflowStage.js';
import { evaluateTimeslicesQuality, getLosAngelesRunDate } from './timeslices.js';
import { evaluateWorkflowStagesQuality } from './workflowStages.js';
import type { QualityIssue, RunQualityReport } from './types.js';

export function evaluateQuality(normalized: {
  workflowDefinitions: WorkflowDefinition[];
  workflowStages: WorkflowStage[];
  timeslices: Timeslice[];
}): {
  issues: QualityIssue[];
  report: RunQualityReport;
  excludedTimesliceIds: Set<string>;
  flags: { no_to_step_in_run: boolean };
} {
  const runDate = getLosAngelesRunDate();
  const timeslicesResult = evaluateTimeslicesQuality({
    timeslices: normalized.timeslices,
    runDate
  });
  const workflowStagesResult = evaluateWorkflowStagesQuality({
    workflowStages: normalized.workflowStages,
    runDate
  });

  const issues = [...timeslicesResult.issues, ...workflowStagesResult.issues];
  const issuesByRule = issues.reduce<Record<string, number>>((acc, issue) => {
    acc[issue.rule] = (acc[issue.rule] ?? 0) + 1;
    return acc;
  }, {});

  return {
    issues,
    report: {
      run_date: runDate,
      counts: {
        timeslices_total: normalized.timeslices.length,
        timeslices_excluded_missing_workflow_definition: timeslicesResult.excludedTimesliceIds.size,
        issues_total: issues.length,
        issues_by_rule: issuesByRule
      },
      flags: timeslicesResult.flags
    },
    excludedTimesliceIds: timeslicesResult.excludedTimesliceIds,
    flags: timeslicesResult.flags
  };
}
