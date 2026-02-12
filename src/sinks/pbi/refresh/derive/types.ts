export interface FactTimesliceRow {
  Name: string | null;
  'From Event': string | null;
  'From Status': string | null;
  'From Step N': number | null;
  'From Task Name': string | null;
  'From Task Page ID': string | null;
  'From Time': number | null;
  'From Workflow Step': string | null;
  'Minutes Diff': number | null;
  'Slice Label': string | null;
  'To Event': string | null;
  'To Status': string | null;
  'To Step N': number | null;
  'To Task Name': string | null;
  'To Task Page ID': string | null;
  'To Time': number | null;
  'To Workflow Step': string | null;
  'Workflow Definition': string;
  'Workflow Record': string | null;
  'Workflow Type': string | null;
  'To DateTime': string | null;
  'To Date': string | null;
  from_stage_key: string | null;
  to_stage_key: string | null;
}

export interface DimWorkflowRow {
  workflow_definition_key: string;
  workflow_definition: string;
}

export interface DimStageRow {
  stage_key: string;
  color_hex: string;
  workflow_definition_key: string;
  workflow_definition: string;
  stage: string;
  stage_n: number;
  'Stage Label': string;
}

export interface DimDateRow {
  Date: string;
  date_key: number;
  year: number;
  month_num: number;
  month_name: string;
  day_of_month: number;
  day_name: string;
}

export interface DimPlaybackFrameRow {
  frame_n: number;
  frame_datetime: string;
  frame_date: string;
}

export interface StageOccupancyHourlyRow {
  frame_n: number;
  snapshot_dt: string;
  snapshot_day: string;
  snapshot_label: string;
  workflow_definition: string;
  stage: string;
  stage_n: number;
  stage_key: string;
  item_count: number;
  'Objective Count': number;
}

export interface StageThroughputDailyRow {
  bucket_day: string;
  bucket_n: number;
  workflow_definition: string;
  stage: string;
  stage_n: number;
  stage_key: string;
  entry_count: number;
  exit_count: number;
  occupancy_peak: number;
  occupancy_avg: number;
}

export type PbiTableRowsByName = Record<string, object[]>;
