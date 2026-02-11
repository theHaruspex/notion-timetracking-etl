import type { Timeslice } from '../../canon/timeslice.js';
import { sha256 } from '../../lib/hash.js';
import type { QualityIssue } from './types.js';

const LOS_ANGELES_DATE_FORMATTER = new Intl.DateTimeFormat('en-CA', {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  timeZone: 'America/Los_Angeles'
});

export function evaluateTimeslicesQuality(args: {
  timeslices: Timeslice[];
  runDate?: string;
}): {
  issues: QualityIssue[];
  excludedTimesliceIds: Set<string>;
  flags: { no_to_step_in_run: boolean };
} {
  const runDate = args.runDate ?? getLosAngelesRunDate();
  const issues: QualityIssue[] = [];
  const excludedTimesliceIds = new Set<string>();
  let toStepCount = 0;

  for (const timeslice of args.timeslices) {
    if (timeslice.to_step_id !== null) {
      toStepCount += 1;
    }

    if (timeslice.workflow_definition_id === null) {
      issues.push(
        createIssue({
          runDate,
          dataset: 'timeslices',
          entityId: timeslice.timeslice_id,
          severity: 'error',
          rule: 'MISSING_WORKFLOW_DEFINITION',
          message: 'Timeslice is missing workflow_definition_id and will be excluded.',
          sample: {
            timeslice_id: timeslice.timeslice_id,
            from_step_id: timeslice.from_step_id,
            to_step_id: timeslice.to_step_id
          }
        })
      );
      excludedTimesliceIds.add(timeslice.timeslice_id);
    }

    if (timeslice.from_step_id !== null && timeslice.started_at === null) {
      issues.push(
        createIssue({
          runDate,
          dataset: 'timeslices',
          entityId: timeslice.timeslice_id,
          severity: 'warn',
          rule: 'FROM_STEP_WITHOUT_STARTED_AT',
          message: 'Timeslice has from_step_id but started_at is null.',
          sample: {
            from_step_id: timeslice.from_step_id
          }
        })
      );
    }

    if (timeslice.to_step_id !== null && timeslice.ended_at === null) {
      issues.push(
        createIssue({
          runDate,
          dataset: 'timeslices',
          entityId: timeslice.timeslice_id,
          severity: 'warn',
          rule: 'TO_STEP_WITHOUT_ENDED_AT',
          message: 'Timeslice has to_step_id but ended_at is null.',
          sample: {
            to_step_id: timeslice.to_step_id
          }
        })
      );
    }

    if (
      timeslice.workflow_definition_id !== null &&
      timeslice.from_step_id === null &&
      timeslice.to_step_id === null
    ) {
      issues.push(
        createIssue({
          runDate,
          dataset: 'timeslices',
          entityId: timeslice.timeslice_id,
          severity: 'warn',
          rule: 'WORKFLOW_WITH_NO_STEPS',
          message: 'Timeslice has workflow_definition_id but no from/to steps.',
          sample: {
            workflow_definition_id: timeslice.workflow_definition_id
          }
        })
      );
    }

    if (
      (timeslice.from_step_id !== null || timeslice.to_step_id !== null) &&
      timeslice.started_at === null &&
      timeslice.ended_at === null
    ) {
      issues.push(
        createIssue({
          runDate,
          dataset: 'timeslices',
          entityId: timeslice.timeslice_id,
          severity: 'warn',
          rule: 'STEPS_WITHOUT_ANY_TIMESTAMP',
          message: 'Timeslice has steps but both started_at and ended_at are null.',
          sample: {
            from_step_id: timeslice.from_step_id,
            to_step_id: timeslice.to_step_id
          }
        })
      );
    }

    if (timeslice.started_at !== null && timeslice.ended_at !== null) {
      const startMs = Date.parse(timeslice.started_at);
      const endMs = Date.parse(timeslice.ended_at);
      if (Number.isFinite(startMs) && Number.isFinite(endMs) && endMs < startMs) {
        issues.push(
          createIssue({
            runDate,
            dataset: 'timeslices',
            entityId: timeslice.timeslice_id,
            severity: 'warn',
            rule: 'NEGATIVE_DURATION',
            message: 'Timeslice ended_at is earlier than started_at.',
            sample: {
              started_at: timeslice.started_at,
              ended_at: timeslice.ended_at
            }
          })
        );
      }
    }
  }

  return {
    issues,
    excludedTimesliceIds,
    flags: {
      no_to_step_in_run: toStepCount === 0
    }
  };
}

function createIssue(input: {
  runDate: string;
  dataset: 'timeslices' | 'workflowStages';
  entityId: string;
  severity: 'warn' | 'error';
  rule: string;
  message: string;
  sample?: Record<string, unknown>;
}): QualityIssue {
  return {
    issue_id: sha256(`${input.runDate}|${input.dataset}|${input.entityId}|${input.rule}`),
    run_date: input.runDate,
    dataset: input.dataset,
    entity_id: input.entityId,
    severity: input.severity,
    rule: input.rule,
    message: input.message,
    sample: input.sample
  };
}

export function getLosAngelesRunDate(now: Date = new Date()): string {
  return LOS_ANGELES_DATE_FORMATTER.format(now);
}
