export type PbiColumnType = 'Int64' | 'Double' | 'Boolean' | 'String' | 'DateTime';

export interface PbiColumnSpec {
  name: string;
  dataType: PbiColumnType;
}

export interface PbiTableSpec {
  name: string;
  columns: PbiColumnSpec[];
  description?: string;
}

export interface PbiRelationshipSpec {
  fromTable: string;
  fromColumn: string;
  toTable: string;
  toColumn: string;
  crossFilteringBehavior?: 'OneDirection' | 'BothDirections' | 'Automatic';
}

export interface PbiDatasetSpec {
  name: string;
  defaultRetentionPolicy?: 'None' | 'BasicFIFO';
  tables: PbiTableSpec[];
  relationships?: PbiRelationshipSpec[];
}
