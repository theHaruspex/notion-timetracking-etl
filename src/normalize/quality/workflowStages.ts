import type { WorkflowStage } from '../../canon/workflowStage.js';
import { sha256 } from '../../lib/hash.js';
import type { QualityIssue } from './types.js';

export function evaluateWorkflowStagesQuality(args: {
  workflowStages: WorkflowStage[];
  runDate: string;
}): { issues: QualityIssue[] } {
  const issues: QualityIssue[] = [];

  for (const workflowStage of args.workflowStages) {
    if (workflowStage.stage_label === null || workflowStage.stage_number === null) {
      issues.push({
        issue_id: sha256(
          `${args.runDate}|workflowStages|${workflowStage.workflow_stage_id}|STAGE_MISSING_LABEL_OR_NUMBER`
        ),
        run_date: args.runDate,
        dataset: 'workflowStages',
        entity_id: workflowStage.workflow_stage_id,
        severity: 'warn',
        rule: 'STAGE_MISSING_LABEL_OR_NUMBER',
        message: 'Workflow stage is missing stage_label or stage_number.',
        sample: {
          stage_label: workflowStage.stage_label,
          stage_number: workflowStage.stage_number
        }
      });
    }
  }

  return { issues };
}
