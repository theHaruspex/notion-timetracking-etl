import type { RawRecord } from '../ingress/rawRecord.js';

export function sampleTimesliceRawRecord(): RawRecord {
  return {
    source: 'notion',
    entityType: 'page',
    databaseId: 'db_timeslices',
    pageId: '12345678-1234-1234-1234-1234567890ab',
    lastEditedTime: '2026-02-01T12:10:00.000Z',
    metadata: {
      created_time: '2026-02-01T12:00:00.000Z',
      url: 'https://notion.so/page'
    },
    properties: {
      title_prop: {
        propertyId: 'title_prop',
        propertyName: 'Name',
        propertyType: 'title',
        rawValue: {
          type: 'title',
          title: [{ plain_text: 'Example Slice' }]
        }
      },
      rel_workflow: {
        propertyId: 'rel_workflow',
        propertyName: 'Workflow',
        propertyType: 'relation',
        rawValue: {
          type: 'relation',
          relation: [{ id: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa' }]
        }
      },
      rel_from_step: {
        propertyId: 'rel_from_step',
        propertyName: 'From',
        propertyType: 'relation',
        rawValue: {
          type: 'relation',
          relation: [{ id: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb' }]
        }
      },
      rel_to_step: {
        propertyId: 'rel_to_step',
        propertyName: 'To',
        propertyType: 'relation',
        rawValue: {
          type: 'relation',
          relation: [{ id: 'cccccccc-cccc-cccc-cccc-cccccccccccc' }]
        }
      },
      start_date: {
        propertyId: 'start_date',
        propertyName: 'Start',
        propertyType: 'date',
        rawValue: {
          type: 'date',
          date: { start: '2026-02-01T12:00:00.000Z', end: null }
        }
      },
      end_date: {
        propertyId: 'end_date',
        propertyName: 'End',
        propertyType: 'date',
        rawValue: {
          type: 'date',
          date: { start: '2026-02-01T12:05:00.000Z', end: null }
        }
      }
    }
  };
}
