export interface QualityIssue {
  issue_id: string;
  run_date: string;
  dataset: 'timeslices' | 'workflowStages';
  entity_id: string;
  severity: 'warn' | 'error';
  rule: string;
  message: string;
  sample?: Record<string, unknown>;
}

export interface RunQualityReport {
  run_date: string;
  counts: {
    timeslices_total: number;
    timeslices_excluded_missing_workflow_definition: number;
    issues_total: number;
    issues_by_rule: Record<string, number>;
  };
  flags: {
    no_to_step_in_run: boolean;
  };
}
