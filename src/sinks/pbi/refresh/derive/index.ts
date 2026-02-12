import type { Timeslice as CanonTimeslice } from '../../../../canon/timeslice.js';
import type { WorkflowDefinition as CanonWorkflowDefinition } from '../../../../canon/workflowDefinition.js';
import type { WorkflowStage as CanonWorkflowStage } from '../../../../canon/workflowStage.js';
import { sha256 } from '../../../../lib/hash.js';
import type {
  DimDateRow,
  DimPlaybackFrameRow,
  DimStageRow,
  DimWorkflowRow,
  FactTimesliceRow,
  StageOccupancyHourlyRow,
  StageThroughputDailyRow,
  PbiTableRowsByName
} from './types.js';

const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * HOUR_MS;
const OLE_AUTOMATION_EPOCH_MS = Date.UTC(1899, 11, 30, 0, 0, 0, 0);
const LOS_ANGELES_TIME_ZONE = 'America/Los_Angeles';

const DATE_PARTS_FORMATTER = new Intl.DateTimeFormat('en-US', {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  timeZone: LOS_ANGELES_TIME_ZONE
});
const MONTH_FORMATTER = new Intl.DateTimeFormat('en-US', {
  month: 'short',
  timeZone: LOS_ANGELES_TIME_ZONE
});
const DAY_FORMATTER = new Intl.DateTimeFormat('en-US', {
  weekday: 'short',
  timeZone: LOS_ANGELES_TIME_ZONE
});
const SNAPSHOT_LABEL_FORMATTER = new Intl.DateTimeFormat('sv-SE', {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
  timeZone: LOS_ANGELES_TIME_ZONE
});

const EXPECTED_TABLE_NAMES = [
  'FactTimeslices',
  'DimWorkflow',
  'DimStage',
  'DimDate',
  'DimPlaybackFrame',
  'StageOccupancy_Hourly',
  'StageThroughput_Daily'
] as const;

const COLOR_HEX_VALUES: string[] = [
  '#FF68A0',
  '#FF6C8B',
  '#FF7076',
  '#FF735F',
  '#FF7643',
  '#FF7800',
  '#EF8600',
  '#E19000',
  '#D59800',
  '#C89F00',
  '#BBA500',
  '#ABAC00',
  '#98B300',
  '#7BBB00',
  '#3DC500',
  '#00C55B',
  '#00C380',
  '#00C197',
  '#00BFA8',
  '#00BDB6',
  '#00BBC3',
  '#00B9CF',
  '#00B7DD',
  '#00B4EC',
  '#0EAFFF',
  '#51A9FF',
  '#6DA4FF',
  '#829EFF',
  '#9398FF',
  '#A491FF',
  '#B688FF',
  '#CA7BFF',
  '#E365FF',
  '#FF41F7',
  '#FF56D2',
  '#FF61B7'
];

export function derivePbiTableRows(input: {
  workflowDefinitions: CanonWorkflowDefinition[];
  workflowStages: CanonWorkflowStage[];
  timeslices: CanonTimeslice[];
}): PbiTableRowsByName {
  let occupancySkippedMissingOrInvalidInterval = 0;
  let entryEdgeCounted = 0;
  let entryEdgeSkippedMissingTimestamp = 0;
  let nonStage1EntryEdgeObserved = 0;

  const workflowDefinitionByCanonId = new Map(
    input.workflowDefinitions.map((workflowDefinition) => [
      workflowDefinition.workflow_definition_id,
      workflowDefinition
    ])
  );
  const workflowLabelByKey = new Map<string, string>();
  for (const workflowDefinition of input.workflowDefinitions) {
    workflowLabelByKey.set(
      workflowDefinition.source_page_id,
      workflowDefinition.page_title ?? workflowDefinition.source_page_id
    );
  }

  const stageByCanonId = new Map(input.workflowStages.map((stage) => [stage.workflow_stage_id, stage]));
  const stageByPageId = new Map(input.workflowStages.map((stage) => [stage.source_page_id, stage]));
  const stageKeyByCanonId = new Map<string, string>();
  for (const stage of input.workflowStages) {
    stageKeyByCanonId.set(stage.workflow_stage_id, stage.source_page_id);
  }

  const workflowPageIdSet = new Set(input.workflowDefinitions.map((workflowDefinition) => workflowDefinition.source_page_id));
  const workflowKeys = new Set(workflowPageIdSet);
  const stagePageIdSet = new Set(input.workflowStages.map((stage) => stage.source_page_id));
  const factRows: FactTimesliceRow[] = [];
  const stageRowsByKey = new Map<string, DimStageRow>();

  for (const item of input.timeslices) {
    const workflowDefinitionCanonicalId = item.workflow_definition_id;
    const workflowDefinition = workflowDefinitionCanonicalId
      ? workflowDefinitionByCanonId.get(workflowDefinitionCanonicalId)
      : undefined;
    const workflowDefinitionKey =
      workflowDefinition?.source_page_id ??
      toUuidMaybe(workflowDefinitionCanonicalId) ??
      'workflow_definition_unknown';
    const workflowDefinitionLabel =
      workflowDefinition?.page_title ?? workflowLabelByKey.get(workflowDefinitionKey) ?? workflowDefinitionKey;

    const fromStageKey = resolveStageKey(item.from_step_id, stageKeyByCanonId);
    const toStageKey = resolveStageKey(item.to_step_id, stageKeyByCanonId);
    const fromStageMeta = fromStageKey ? stageByPageId.get(fromStageKey) : undefined;
    const toStageMeta = toStageKey ? stageByPageId.get(toStageKey) : undefined;
    const fromStageNumber = normalizeStageNumberOrNull(fromStageMeta?.stage_number);
    const toStageNumber = normalizeStageNumberOrNull(toStageMeta?.stage_number);
    const fromStageLabel = fromStageMeta?.stage_label ?? null;
    const toStageLabel = toStageMeta?.stage_label ?? null;
    const toDateTimeRaw = item.ended_at ?? item.started_at ?? item.last_edited_time ?? item.created_time;
    const toDateTime = normalizeIsoTimestamp(toDateTimeRaw);
    const toDate = toLosAngelesDateStartIso(toDateTimeRaw);

    factRows.push({
      Name: item.page_title ?? item.timeslice_id,
      'From Event': null,
      'From Status': null,
      'From Step N': fromStageNumber,
      'From Task Name': null,
      'From Task Page ID': null,
      'From Time': toPowerBiSerial(item.started_at),
      'From Workflow Step': fromStageLabel,
      'Minutes Diff':
        typeof item.duration_seconds === 'number' ? Math.round(item.duration_seconds / 60) : null,
      'Slice Label': item.page_title ?? item.timeslice_id,
      'To Event': null,
      'To Status': null,
      'To Step N': toStageNumber,
      'To Task Name': null,
      'To Task Page ID': null,
      'To Time': toPowerBiSerial(item.ended_at),
      'To Workflow Step': toStageLabel,
      'Workflow Definition': workflowDefinitionLabel,
      'Workflow Record': item.source_page_id,
      'Workflow Type': null,
      'To DateTime': toDateTime,
      'To Date': toDate,
      from_stage_key: fromStageKey,
      to_stage_key: toStageKey
    });
  }

  for (const workflowStage of input.workflowStages) {
    const stageKey = workflowStage.source_page_id;
    const workflowDefinition = workflowStage.workflow_definition_id
      ? workflowDefinitionByCanonId.get(workflowStage.workflow_definition_id)
      : undefined;
    const workflowDefinitionKey =
      workflowDefinition?.source_page_id ??
      toUuidMaybe(workflowStage.workflow_definition_id) ??
      'workflow_definition_unknown';
    const workflowDefinitionLabel =
      workflowDefinition?.page_title ?? workflowLabelByKey.get(workflowDefinitionKey) ?? workflowDefinitionKey;

    stageRowsByKey.set(stageKey, {
      stage_key: stageKey,
      color_hex: assignStageColorHex(stageKey),
      workflow_definition_key: workflowDefinitionKey,
      workflow_definition: workflowDefinitionLabel,
      stage: workflowStage.stage_label ?? stageKey,
      stage_n: normalizeStageNumber(workflowStage.stage_number),
      'Stage Label': `${pad2(normalizeStageNumber(workflowStage.stage_number))}. ${
        workflowStage.stage_label ?? stageKey
      }`
    });
  }

  const dimWorkflowRows: DimWorkflowRow[] = Array.from(workflowKeys)
    .sort((a, b) => a.localeCompare(b))
    .map((workflow_definition_key) => ({
      workflow_definition_key,
      workflow_definition: workflowLabelByKey.get(workflow_definition_key) ?? workflow_definition_key
    }));

  const dimStageRows: DimStageRow[] = Array.from(stageRowsByKey.values()).sort((a, b) =>
    a.stage_key.localeCompare(b.stage_key)
  );

  const missingFactStageKeys = factRows
    .flatMap((row) => [row.from_stage_key, row.to_stage_key])
    .filter((key): key is string => typeof key === 'string' && key.length > 0)
    .filter((key) => !stagePageIdSet.has(key));
  if (missingFactStageKeys.length > 0) {
    throw new Error(
      `FactTimeslices references stage keys not present in workflowStages: ${Array.from(
        new Set(missingFactStageKeys)
      )
        .slice(0, 10)
        .join(', ')}`
    );
  }

  const illegalWorkflowKeys = dimWorkflowRows
    .filter((row) => !workflowPageIdSet.has(row.workflow_definition_key))
    .map((row) => row.workflow_definition_key);
  if (illegalWorkflowKeys.length > 0) {
    throw new Error(
      `DimWorkflow contains keys not present in workflowDefinitions: ${illegalWorkflowKeys.slice(0, 10).join(', ')}`
    );
  }

  const illegalStageKeys = dimStageRows
    .filter((row) => !stagePageIdSet.has(row.stage_key))
    .map((row) => row.stage_key);
  if (illegalStageKeys.length > 0) {
    throw new Error(
      `DimStage contains keys not present in workflowStages: ${illegalStageKeys.slice(0, 10).join(', ')}`
    );
  }

  const dimDateRows = deriveDimDateRows(factRows);
  const dimPlaybackFrameRows = deriveDimPlaybackFrameRows(input.timeslices);
  const stageOccupancyHourlyRows = deriveStageOccupancyHourlyRows({
    dimPlaybackFrameRows,
    dimStageRows,
    timeslices: input.timeslices,
    stageKeyByCanonId,
    onSkippedMissingOrInvalidInterval: () => {
      occupancySkippedMissingOrInvalidInterval += 1;
    }
  });
  const stageThroughputDailyRows = deriveStageThroughputDailyRows({
    dimStageRows,
    stageOccupancyHourlyRows,
    timeslices: input.timeslices,
    stageByPageId,
    stageKeyByCanonId,
    onEntryEdgeCounted: () => {
      entryEdgeCounted += 1;
    },
    onEntryEdgeSkippedMissingTimestamp: () => {
      entryEdgeSkippedMissingTimestamp += 1;
    },
    onNonStage1EntryEdgeObserved: () => {
      nonStage1EntryEdgeObserved += 1;
    }
  });

  const tableRowsByName: PbiTableRowsByName = {
    FactTimeslices: factRows,
    DimWorkflow: dimWorkflowRows,
    DimStage: dimStageRows,
    DimDate: dimDateRows,
    DimPlaybackFrame: dimPlaybackFrameRows,
    StageOccupancy_Hourly: stageOccupancyHourlyRows,
    StageThroughput_Daily: stageThroughputDailyRows
  };
  assertExpectedTableKeys(tableRowsByName);
  console.warn('[derive] Stage 3 counters', {
    occupancySkippedMissingOrInvalidInterval,
    entryEdgeCounted,
    entryEdgeSkippedMissingTimestamp,
    nonStage1EntryEdgeObserved
  });
  return tableRowsByName;
}

function resolveStageKey(
  canonicalStageId: string | null,
  stageKeyByCanonId: Map<string, string>
): string | null {
  if (!canonicalStageId) {
    return null;
  }
  return stageKeyByCanonId.get(canonicalStageId) ?? null;
}

function toPowerBiSerial(value: string | null): number | null {
  if (!value) {
    return null;
  }
  const asMs = Date.parse(value);
  if (!Number.isFinite(asMs)) {
    return null;
  }
  return (asMs - OLE_AUTOMATION_EPOCH_MS) / (24 * 60 * 60 * 1000);
}

function normalizeIsoTimestamp(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const asMs = Date.parse(value);
  if (!Number.isFinite(asMs)) {
    return null;
  }
  return new Date(asMs).toISOString();
}

function normalizeStageNumber(value: number | null | undefined): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return 1;
  }
  return Math.max(1, Math.round(value));
}

function normalizeStageNumberOrNull(value: number | null | undefined): number | null {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  return Math.round(value);
}

function toUuidMaybe(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const lower = value.toLowerCase();
  const hyphenatedMatch = /([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/.exec(lower);
  if (hyphenatedMatch) {
    return hyphenatedMatch[1];
  }

  const compactMatch = /([0-9a-f]{32})/.exec(lower);
  if (!compactMatch) {
    return null;
  }
  const compact = compactMatch[1];
  return `${compact.slice(0, 8)}-${compact.slice(8, 12)}-${compact.slice(12, 16)}-${compact.slice(
    16,
    20
  )}-${compact.slice(20)}`;
}

function toLosAngelesDateStartIso(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const asMs = Date.parse(value);
  if (!Number.isFinite(asMs)) {
    return null;
  }
  const parts = getLosAngelesDateParts(new Date(asMs));
  return `${parts.year}-${pad2(parts.month)}-${pad2(parts.day)}T00:00:00.000Z`;
}

function deriveDimDateRows(factRows: FactTimesliceRow[]): DimDateRow[] {
  const dateValues = factRows
    .map((row) => row['To Date'])
    .filter((value): value is string => typeof value === 'string' && value.length > 0)
    .sort((a, b) => a.localeCompare(b));

  if (dateValues.length === 0) {
    return [];
  }

  const minDateParts = parseDateLabel(dateValues[0]);
  const maxDateParts = parseDateLabel(dateValues[dateValues.length - 1]);
  if (!minDateParts || !maxDateParts) {
    return [];
  }

  const rows: DimDateRow[] = [];
  const minDateUtcMs = Date.UTC(minDateParts.year, minDateParts.month - 1, minDateParts.day, 0, 0, 0, 0);
  const maxDateUtcMs = Date.UTC(maxDateParts.year, maxDateParts.month - 1, maxDateParts.day, 0, 0, 0, 0);
  for (let ts = minDateUtcMs; ts <= maxDateUtcMs; ts += DAY_MS) {
    const date = new Date(ts);
    const yyyy = date.getUTCFullYear();
    const mm = date.getUTCMonth() + 1;
    const dd = date.getUTCDate();
    const labelRef = new Date(Date.UTC(yyyy, mm - 1, dd, 12, 0, 0, 0));
    rows.push({
      Date: date.toISOString(),
      date_key: yyyy * 10000 + mm * 100 + dd,
      year: yyyy,
      month_num: mm,
      month_name: MONTH_FORMATTER.format(labelRef),
      day_of_month: dd,
      day_name: DAY_FORMATTER.format(labelRef)
    });
  }
  return rows;
}

function deriveDimPlaybackFrameRows(timeslices: CanonTimeslice[]): DimPlaybackFrameRow[] {
  const timestamps: number[] = [];
  for (const item of timeslices) {
    const candidates = [item.started_at, item.ended_at, item.last_edited_time, item.created_time];
    for (const candidate of candidates) {
      const asMs = candidate ? Date.parse(candidate) : Number.NaN;
      if (Number.isFinite(asMs)) {
        timestamps.push(asMs);
      }
    }
  }

  if (timestamps.length === 0) {
    return [];
  }

  const minTs = Math.min(...timestamps);
  const maxTs = Math.max(...timestamps);
  const minHourTs = Math.floor(minTs / HOUR_MS) * HOUR_MS;
  const maxHourTs = Math.floor(maxTs / HOUR_MS) * HOUR_MS;

  const rows: DimPlaybackFrameRow[] = [];
  let frameN = 0;
  for (let ts = minHourTs; ts <= maxHourTs; ts += HOUR_MS) {
    const frameDatetime = new Date(ts);
    const frameDate = toLosAngelesDateStartIso(frameDatetime.toISOString());
    rows.push({
      frame_n: frameN,
      frame_datetime: frameDatetime.toISOString(),
      frame_date: frameDate ?? frameDatetime.toISOString()
    });
    frameN += 1;
  }
  return rows;
}

function deriveStageOccupancyHourlyRows(args: {
  dimPlaybackFrameRows: DimPlaybackFrameRow[];
  dimStageRows: DimStageRow[];
  timeslices: CanonTimeslice[];
  stageKeyByCanonId: Map<string, string>;
  onSkippedMissingOrInvalidInterval: () => void;
}): StageOccupancyHourlyRow[] {
  type StageInterval = {
    workflow_record: string;
    startMs: number;
    endMs: number;
  };
  const intervalsByStageKey = new Map<string, StageInterval[]>();
  for (const timeslice of args.timeslices) {
    const stageKey = resolveStageKey(timeslice.from_step_id, args.stageKeyByCanonId);
    if (!stageKey) {
      continue;
    }
    const startMs = parseTimestampMs(timeslice.started_at);
    const endMs = parseTimestampMs(timeslice.ended_at);
    if (startMs === null || endMs === null) {
      args.onSkippedMissingOrInvalidInterval();
      continue;
    }
    if (endMs < startMs) {
      args.onSkippedMissingOrInvalidInterval();
      continue;
    }
    const existing = intervalsByStageKey.get(stageKey);
    const interval: StageInterval = {
      workflow_record: timeslice.source_page_id,
      startMs,
      endMs
    };
    if (existing) {
      existing.push(interval);
    } else {
      intervalsByStageKey.set(stageKey, [interval]);
    }
  }

  const stageByKey = new Map(args.dimStageRows.map((stage) => [stage.stage_key, stage]));
  const rows: StageOccupancyHourlyRow[] = [];
  for (const frame of args.dimPlaybackFrameRows) {
    const frameMs = parseTimestampMs(frame.frame_datetime);
    if (frameMs === null) {
      continue;
    }
    for (const [stageKey, intervals] of intervalsByStageKey) {
      const stage = stageByKey.get(stageKey);
      if (!stage) {
        continue;
      }
      const activeWorkflowRecords = new Set<string>();
      for (const interval of intervals) {
        if (interval.startMs <= frameMs && frameMs <= interval.endMs) {
          activeWorkflowRecords.add(interval.workflow_record);
        }
      }
      const itemCount = activeWorkflowRecords.size;
      if (itemCount <= 0) {
        continue;
      }
      rows.push({
        frame_n: frame.frame_n,
        snapshot_dt: frame.frame_datetime,
        snapshot_day: frame.frame_date,
        snapshot_label: toLosAngelesSnapshotLabel(frame.frame_datetime),
        workflow_definition: stage.workflow_definition,
        stage: stage.stage,
        stage_n: stage.stage_n,
        stage_key: stage.stage_key,
        item_count: itemCount,
        'Objective Count': itemCount
      });
    }
  }
  return rows;
}

function deriveStageThroughputDailyRows(args: {
  dimStageRows: DimStageRow[];
  stageOccupancyHourlyRows: StageOccupancyHourlyRow[];
  timeslices: CanonTimeslice[];
  stageByPageId: Map<string, CanonWorkflowStage>;
  stageKeyByCanonId: Map<string, string>;
  onEntryEdgeCounted: () => void;
  onEntryEdgeSkippedMissingTimestamp: () => void;
  onNonStage1EntryEdgeObserved: () => void;
}): StageThroughputDailyRow[] {
  const stageByKey = new Map(args.dimStageRows.map((stage) => [stage.stage_key, stage]));
  const dailyCounts = new Map<string, { entry_count: number; exit_count: number }>();
  const occupancyByDayAndStage = new Map<string, { peak: number; total: number; count: number }>();

  const incrementDailyCount = (
    stageKey: string,
    bucketDay: string,
    field: 'entry_count' | 'exit_count'
  ): void => {
    const key = `${bucketDay}|${stageKey}`;
    const current = dailyCounts.get(key) ?? { entry_count: 0, exit_count: 0 };
    current[field] += 1;
    dailyCounts.set(key, current);
  };

  for (const timeslice of args.timeslices) {
    const fromStageKey = resolveStageKey(timeslice.from_step_id, args.stageKeyByCanonId);
    if (fromStageKey) {
      const startedDay = toLosAngelesDateStartIso(timeslice.started_at);
      if (startedDay) {
        incrementDailyCount(fromStageKey, startedDay, 'entry_count');
      }

      const endedDay = toLosAngelesDateStartIso(timeslice.ended_at);
      if (endedDay) {
        incrementDailyCount(fromStageKey, endedDay, 'exit_count');
      }
    }

    if (timeslice.from_step_id === null && timeslice.to_step_id !== null) {
      const toStageKey = resolveStageKey(timeslice.to_step_id, args.stageKeyByCanonId);
      const toStageMeta = toStageKey ? args.stageByPageId.get(toStageKey) : undefined;
      const toStageNumber = normalizeStageNumberOrNull(toStageMeta?.stage_number);
      if (toStageKey && toStageNumber === 1) {
        const eventTs =
          normalizeIsoTimestamp(timeslice.ended_at) ??
          normalizeIsoTimestamp(timeslice.started_at) ??
          normalizeIsoTimestamp(timeslice.last_edited_time) ??
          normalizeIsoTimestamp(timeslice.created_time);
        const eventDay = toLosAngelesDateStartIso(eventTs);
        if (eventDay) {
          incrementDailyCount(toStageKey, eventDay, 'entry_count');
          args.onEntryEdgeCounted();
        } else {
          args.onEntryEdgeSkippedMissingTimestamp();
        }
      } else if (toStageKey) {
        args.onNonStage1EntryEdgeObserved();
      }
    }
  }

  for (const occupancyRow of args.stageOccupancyHourlyRows) {
    const key = `${occupancyRow.snapshot_day}|${occupancyRow.stage_key}`;
    const current = occupancyByDayAndStage.get(key) ?? { peak: 0, total: 0, count: 0 };
    current.peak = Math.max(current.peak, occupancyRow.item_count);
    current.total += occupancyRow.item_count;
    current.count += 1;
    occupancyByDayAndStage.set(key, current);
  }

  const rowKeys = new Set<string>();
  for (const key of dailyCounts.keys()) {
    rowKeys.add(key);
  }
  for (const key of occupancyByDayAndStage.keys()) {
    rowKeys.add(key);
  }

  const rows: StageThroughputDailyRow[] = [];
  for (const key of Array.from(rowKeys).sort((a, b) => a.localeCompare(b))) {
    const [bucketDay, stageKey] = key.split('|');
    const stage = stageByKey.get(stageKey);
    if (!stage || !bucketDay) {
      continue;
    }
    const counts = dailyCounts.get(key) ?? { entry_count: 0, exit_count: 0 };
    const occupancy = occupancyByDayAndStage.get(key) ?? { peak: 0, total: 0, count: 0 };
    const bucketParts = parseDateLabel(bucketDay);
    if (!bucketParts) {
      continue;
    }
    const bucketN = bucketParts.year * 10000 + bucketParts.month * 100 + bucketParts.day;

    rows.push({
      bucket_day: bucketDay,
      bucket_n: bucketN,
      workflow_definition: stage.workflow_definition,
      stage: stage.stage,
      stage_n: stage.stage_n,
      stage_key: stage.stage_key,
      entry_count: counts.entry_count,
      exit_count: counts.exit_count,
      occupancy_peak: occupancy.peak,
      occupancy_avg: occupancy.count > 0 ? occupancy.total / occupancy.count : 0
    });
  }

  return rows.filter(
    (row) => row.entry_count > 0 || row.exit_count > 0 || row.occupancy_peak > 0 || row.occupancy_avg > 0
  );
}

function getLosAngelesDateParts(date: Date): { year: number; month: number; day: number } {
  const parts = DATE_PARTS_FORMATTER.formatToParts(date);
  const yearPart = parts.find((part) => part.type === 'year')?.value;
  const monthPart = parts.find((part) => part.type === 'month')?.value;
  const dayPart = parts.find((part) => part.type === 'day')?.value;
  const year = yearPart ? Number.parseInt(yearPart, 10) : Number.NaN;
  const month = monthPart ? Number.parseInt(monthPart, 10) : Number.NaN;
  const day = dayPart ? Number.parseInt(dayPart, 10) : Number.NaN;
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    throw new Error(`Failed to resolve Los Angeles date parts for timestamp: ${date.toISOString()}`);
  }
  return { year, month, day };
}

function toLosAngelesSnapshotLabel(value: string): string {
  const asMs = Date.parse(value);
  if (!Number.isFinite(asMs)) {
    return value;
  }
  const formatted = SNAPSHOT_LABEL_FORMATTER.format(new Date(asMs));
  return formatted.replace(' ', ' ');
}

function parseTimestampMs(value: string | null | undefined): number | null {
  if (!value) {
    return null;
  }
  const asMs = Date.parse(value);
  if (!Number.isFinite(asMs)) {
    return null;
  }
  return asMs;
}

function parseDateLabel(value: string): { year: number; month: number; day: number } | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})T00:00:00\.000Z$/.exec(value);
  if (!match) {
    return null;
  }
  return {
    year: Number.parseInt(match[1], 10),
    month: Number.parseInt(match[2], 10),
    day: Number.parseInt(match[3], 10)
  };
}

function assertExpectedTableKeys(tableRowsByName: PbiTableRowsByName): void {
  const actual = Object.keys(tableRowsByName);
  const missing = EXPECTED_TABLE_NAMES.filter((name) => !Object.prototype.hasOwnProperty.call(tableRowsByName, name));
  const extra = actual.filter((name) => !EXPECTED_TABLE_NAMES.includes(name as (typeof EXPECTED_TABLE_NAMES)[number]));
  if (missing.length === 0 && extra.length === 0) {
    return;
  }
  throw new Error(
    `derivePbiTableRows returned unexpected table keys. Missing: ${missing.join(', ') || 'none'}. Extra: ${
      extra.join(', ') || 'none'
    }.`
  );
}

function pad2(value: number): string {
  return value.toString().padStart(2, '0');
}

function assignStageColorHex(stageKey: string): string {
  const digest = sha256(stageKey);
  const numericPrefix = Number.parseInt(digest.slice(0, 8), 16);
  const zeroBased = Number.isFinite(numericPrefix) ? numericPrefix % COLOR_HEX_VALUES.length : 0;
  return COLOR_HEX_VALUES[zeroBased];
}
