import type { PbiDatasetSpec } from './types.js';
import { validateSpec } from './validateSpec.js';

export function buildModelSpec(datasetName: string): PbiDatasetSpec {
  const spec: PbiDatasetSpec = {
    name: datasetName,
    defaultRetentionPolicy: 'None',
    tables: [
      {
        name: '__bootstrap',
        columns: [
          { name: 'ingested_at', dataType: 'DateTime' },
          { name: 'note', dataType: 'String' }
        ]
      }
    ],
    relationships: []
  };

  validateSpec(spec);
  return spec;
}
