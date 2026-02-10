import type { PbiDatasetSpec } from './types.js';

const MAX_TABLES = 75;
const MAX_COLUMNS_PER_TABLE = 75;
const MAX_RELATIONSHIPS = 75;
const MAX_NAME_LENGTH = 100;
const CONTROL_CHAR_REGEX = /[\u0000-\u001F\u007F]/;
const VALID_CROSS_FILTERING = new Set(['OneDirection', 'BothDirections', 'Automatic']);

function validateName(kind: 'table' | 'column', name: string, context?: string): string {
  if (name.length === 0) {
    throw new Error(`Invalid spec: ${kind} name cannot be empty${context ? ` (${context})` : ''}.`);
  }
  if (name !== name.trim()) {
    throw new Error(
      `Invalid spec: ${kind} name "${name}" cannot have leading/trailing whitespace${
        context ? ` (${context})` : ''
      }.`
    );
  }
  if (CONTROL_CHAR_REGEX.test(name)) {
    throw new Error(
      `Invalid spec: ${kind} name "${name}" contains control characters${
        context ? ` (${context})` : ''
      }.`
    );
  }
  if (name.length > MAX_NAME_LENGTH) {
    throw new Error(
      `Invalid spec: ${kind} name "${name}" exceeds ${MAX_NAME_LENGTH} characters${
        context ? ` (${context})` : ''
      }.`
    );
  }
  return name;
}

export function validateSpec(spec: PbiDatasetSpec): void {
  if (spec.tables.length > MAX_TABLES) {
    throw new Error(`Power BI limit exceeded: tables (${spec.tables.length}) > ${MAX_TABLES}.`);
  }

  const tableNameSet = new Set<string>();

  for (const table of spec.tables) {
    const tableName = validateName('table', table.name);
    if (tableNameSet.has(tableName.toLowerCase())) {
      throw new Error(`Invalid spec: duplicate table name "${tableName}".`);
    }
    tableNameSet.add(tableName.toLowerCase());

    if (table.columns.length > MAX_COLUMNS_PER_TABLE) {
      throw new Error(
        `Power BI limit exceeded: columns in "${tableName}" (${table.columns.length}) > ${MAX_COLUMNS_PER_TABLE}.`
      );
    }

    const columnNameSet = new Set<string>();
    for (const column of table.columns) {
      const columnName = validateName('column', column.name, `table "${tableName}"`);
      if (columnNameSet.has(columnName.toLowerCase())) {
        throw new Error(`Invalid spec: duplicate column "${columnName}" in table "${tableName}".`);
      }
      columnNameSet.add(columnName.toLowerCase());
    }
  }

  const relationshipCount = spec.relationships?.length ?? 0;
  if (relationshipCount > MAX_RELATIONSHIPS) {
    throw new Error(
      `Power BI limit exceeded: relationships (${relationshipCount}) > ${MAX_RELATIONSHIPS}.`
    );
  }

  if (!spec.relationships || spec.relationships.length === 0) {
    return;
  }

  const tableByLowerName = new Map(spec.tables.map((table) => [table.name.toLowerCase(), table]));

  for (const relationship of spec.relationships) {
    const crossFilteringBehavior = relationship.crossFilteringBehavior;
    if (
      crossFilteringBehavior !== undefined &&
      !VALID_CROSS_FILTERING.has(crossFilteringBehavior)
    ) {
      throw new Error(
        `Invalid spec: relationship ${relationship.fromTable}.${relationship.fromColumn} -> ${relationship.toTable}.${relationship.toColumn} has invalid crossFilteringBehavior "${crossFilteringBehavior}".`
      );
    }

    const fromTable = tableByLowerName.get(relationship.fromTable.toLowerCase());
    if (!fromTable) {
      throw new Error(
        `Invalid spec: relationship references missing fromTable "${relationship.fromTable}".`
      );
    }

    const toTable = tableByLowerName.get(relationship.toTable.toLowerCase());
    if (!toTable) {
      throw new Error(
        `Invalid spec: relationship references missing toTable "${relationship.toTable}".`
      );
    }

    const fromColumnExists = fromTable.columns.some(
      (column) => column.name.toLowerCase() === relationship.fromColumn.toLowerCase()
    );
    if (!fromColumnExists) {
      throw new Error(
        `Invalid spec: relationship references missing fromColumn "${relationship.fromColumn}" on table "${relationship.fromTable}".`
      );
    }

    const toColumnExists = toTable.columns.some(
      (column) => column.name.toLowerCase() === relationship.toColumn.toLowerCase()
    );
    if (!toColumnExists) {
      throw new Error(
        `Invalid spec: relationship references missing toColumn "${relationship.toColumn}" on table "${relationship.toTable}".`
      );
    }
  }
}
