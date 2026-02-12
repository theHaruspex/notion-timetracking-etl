import type { PbiDatasetSpec } from './types.js';
import { validateSpec } from './validateSpec.js';

export function buildModelSpec(datasetName: string): PbiDatasetSpec {
  const relName = (fromTable: string, fromColumn: string, toTable: string, toColumn: string): string =>
    `${fromTable}.${fromColumn}__to__${toTable}.${toColumn}`;

  const spec: PbiDatasetSpec = {
    name: datasetName,
    defaultRetentionPolicy: 'None',
    tables: [
      {
        name: 'FactTimeslices',
        columns: [
          { name: 'Name', dataType: 'String' },
          { name: 'From Event', dataType: 'String' },
          { name: 'From Status', dataType: 'String' },
          { name: 'From Step N', dataType: 'Int64' },
          { name: 'From Task Name', dataType: 'String' },
          { name: 'From Task Page ID', dataType: 'String' },
          { name: 'From Time', dataType: 'Double' },
          { name: 'From Workflow Step', dataType: 'String' },
          { name: 'Minutes Diff', dataType: 'Int64' },
          { name: 'Slice Label', dataType: 'String' },
          { name: 'To Event', dataType: 'String' },
          { name: 'To Status', dataType: 'String' },
          { name: 'To Step N', dataType: 'Int64' },
          { name: 'To Task Name', dataType: 'String' },
          { name: 'To Task Page ID', dataType: 'String' },
          { name: 'To Time', dataType: 'Double' },
          { name: 'To Workflow Step', dataType: 'String' },
          { name: 'Workflow Definition', dataType: 'String' },
          { name: 'Workflow Record', dataType: 'String' },
          { name: 'Workflow Type', dataType: 'String' },
          { name: 'To DateTime', dataType: 'DateTime' },
          { name: 'To Date', dataType: 'DateTime' },
          { name: 'from_stage_key', dataType: 'String' },
          { name: 'to_stage_key', dataType: 'String' }
        ]
      }
    ],
    relationships: [
      {
        name: relName('StageOccupancy_Hourly', 'frame_n', 'DimPlaybackFrame', 'frame_n'),
        fromTable: 'StageOccupancy_Hourly',
        fromColumn: 'frame_n',
        toTable: 'DimPlaybackFrame',
        toColumn: 'frame_n',
        crossFilteringBehavior: 'OneDirection'
      },
      {
        name: relName('StageOccupancy_Hourly', 'stage_key', 'DimStage', 'stage_key'),
        fromTable: 'StageOccupancy_Hourly',
        fromColumn: 'stage_key',
        toTable: 'DimStage',
        toColumn: 'stage_key',
        crossFilteringBehavior: 'OneDirection'
      },
      {
        name: relName('FactTimeslices', 'from_stage_key', 'DimStage', 'stage_key'),
        fromTable: 'FactTimeslices',
        fromColumn: 'from_stage_key',
        toTable: 'DimStage',
        toColumn: 'stage_key',
        crossFilteringBehavior: 'OneDirection'
      },
      {
        name: relName('StageThroughput_Daily', 'stage_key', 'DimStage', 'stage_key'),
        fromTable: 'StageThroughput_Daily',
        fromColumn: 'stage_key',
        toTable: 'DimStage',
        toColumn: 'stage_key',
        crossFilteringBehavior: 'OneDirection'
      },
      {
        name: relName('StageThroughput_Daily', 'bucket_day', 'DimDate', 'Date'),
        fromTable: 'StageThroughput_Daily',
        fromColumn: 'bucket_day',
        toTable: 'DimDate',
        toColumn: 'Date',
        crossFilteringBehavior: 'OneDirection'
      },
      {
        name: relName('DimPlaybackFrame', 'frame_date', 'DimDate', 'Date'),
        fromTable: 'DimPlaybackFrame',
        fromColumn: 'frame_date',
        toTable: 'DimDate',
        toColumn: 'Date',
        crossFilteringBehavior: 'OneDirection'
      },
      {
        name: relName('FactTimeslices', 'To Date', 'DimDate', 'Date'),
        fromTable: 'FactTimeslices',
        fromColumn: 'To Date',
        toTable: 'DimDate',
        toColumn: 'Date',
        crossFilteringBehavior: 'OneDirection'
      },
      {
        name: relName('DimStage', 'workflow_definition_key', 'DimWorkflow', 'workflow_definition_key'),
        fromTable: 'DimStage',
        fromColumn: 'workflow_definition_key',
        toTable: 'DimWorkflow',
        toColumn: 'workflow_definition_key',
        crossFilteringBehavior: 'OneDirection'
      }
    ]
  };

  spec.tables.push(
    {
      name: 'DimWorkflow',
      columns: [
        { name: 'workflow_definition_key', dataType: 'String' },
        { name: 'workflow_definition', dataType: 'String' }
      ]
    },
    {
      name: 'DimStage',
      columns: [
        { name: 'stage_key', dataType: 'String' },
        { name: 'color_hex', dataType: 'String' },
        { name: 'workflow_definition_key', dataType: 'String' },
        { name: 'workflow_definition', dataType: 'String' },
        { name: 'stage', dataType: 'String' },
        { name: 'stage_n', dataType: 'Int64' },
        { name: 'Stage Label', dataType: 'String' }
      ]
    },
    {
      name: 'DimDate',
      columns: [
        { name: 'Date', dataType: 'DateTime' },
        { name: 'date_key', dataType: 'Int64' },
        { name: 'year', dataType: 'Int64' },
        { name: 'month_num', dataType: 'Int64' },
        { name: 'month_name', dataType: 'String' },
        { name: 'day_of_month', dataType: 'Int64' },
        { name: 'day_name', dataType: 'String' }
      ]
    },
    {
      name: 'DimPlaybackFrame',
      columns: [
        { name: 'frame_n', dataType: 'Int64' },
        { name: 'frame_datetime', dataType: 'DateTime' },
        { name: 'frame_date', dataType: 'DateTime' }
      ]
    },
    {
      name: 'StageOccupancy_Hourly',
      columns: [
        { name: 'frame_n', dataType: 'Int64' },
        { name: 'snapshot_dt', dataType: 'DateTime' },
        { name: 'snapshot_day', dataType: 'DateTime' },
        { name: 'snapshot_label', dataType: 'String' },
        { name: 'workflow_definition', dataType: 'String' },
        { name: 'stage', dataType: 'String' },
        { name: 'stage_n', dataType: 'Int64' },
        { name: 'stage_key', dataType: 'String' },
        { name: 'item_count', dataType: 'Int64' },
        { name: 'Objective Count', dataType: 'Int64' }
      ]
    },
    {
      name: 'StageThroughput_Daily',
      columns: [
        { name: 'bucket_day', dataType: 'DateTime' },
        { name: 'bucket_n', dataType: 'Int64' },
        { name: 'workflow_definition', dataType: 'String' },
        { name: 'stage', dataType: 'String' },
        { name: 'stage_n', dataType: 'Int64' },
        { name: 'stage_key', dataType: 'String' },
        { name: 'entry_count', dataType: 'Int64' },
        { name: 'exit_count', dataType: 'Int64' },
        { name: 'occupancy_peak', dataType: 'Int64' },
        { name: 'occupancy_avg', dataType: 'Double' }
      ]
    }
  );

  validateSpec(spec);
  return spec;
}
