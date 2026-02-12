import ExcelJS from 'exceljs';
import type { PbiTableRowsByName } from '../pbi/refresh/derive/types.js';

export interface WriteExcelInput {
  tableRowsByName: PbiTableRowsByName;
  outputPath: string;
}

export async function writeExcelFile(input: WriteExcelInput): Promise<void> {
  const workbook = new ExcelJS.Workbook();

  // Sort table names for consistent sheet order
  const tableNames = Object.keys(input.tableRowsByName).sort();

  for (const tableName of tableNames) {
    const rows = input.tableRowsByName[tableName];
    if (!Array.isArray(rows) || rows.length === 0) {
      // Still create empty sheet
      const worksheet = workbook.addWorksheet(tableName);
      worksheet.addRow([]);
      continue;
    }

    const worksheet = workbook.addWorksheet(tableName);

    // Extract column names from first row
    const firstRow = rows[0];
    if (!firstRow || typeof firstRow !== 'object') {
      continue;
    }

    const columnNames = Object.keys(firstRow as Record<string, unknown>);
    worksheet.addRow(columnNames);

    // Style header row
    const headerRow = worksheet.getRow(1);
    headerRow.font = { bold: true };
    headerRow.fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'FFE0E0E0' }
    };

    // Add data rows
    for (const row of rows) {
      if (!row || typeof row !== 'object') {
        continue;
      }
      const rowData = columnNames.map((colName) => {
        const value = (row as Record<string, unknown>)[colName];
        // Convert null/undefined to empty string for Excel
        if (value === null || value === undefined) {
          return '';
        }
        return value;
      });
      worksheet.addRow(rowData);
    }

    // Auto-fit columns
    worksheet.columns.forEach((column) => {
      if (column.header) {
        column.width = Math.max(column.width ?? 10, 15);
      }
    });
  }

  await workbook.xlsx.writeFile(input.outputPath);
}
