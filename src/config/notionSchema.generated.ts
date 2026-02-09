export const notionSchema = {
  "workflowDefinitions": {
    "properties": {
      "Workflow Type": {
        "id": "%3Dy%5EO",
        "type": "select"
      },
      "Container Property": {
        "id": "%40nA%5B",
        "type": "rich_text"
      },
      "Workflow Steps": {
        "id": "PygX",
        "type": "relation"
      },
      "Notes": {
        "id": "%5Cllc",
        "type": "rich_text"
      },
      "Enabled": {
        "id": "mgCW",
        "type": "checkbox"
      },
      "Name": {
        "id": "title",
        "type": "title"
      }
    }
  },
  "workflowStages": {
    "properties": {
      "Deploy Tasks": {
        "id": "%3Dboo",
        "type": "select"
      },
      "Default Owner": {
        "id": "AtG%3E",
        "type": "relation"
      },
      "Workflow": {
        "id": "SA%5Dy",
        "type": "select"
      },
      "Workflow Definition": {
        "id": "%5Bn%40l",
        "type": "relation"
      },
      "Workflow Step": {
        "id": "j%5D_%3F",
        "type": "number"
      },
      "Workflow Type": {
        "id": "m%5BR%3B",
        "type": "rollup"
      },
      "Siblings": {
        "id": "o%3B%3AF",
        "type": "relation"
      },
      "Department": {
        "id": "xz%5Bw",
        "type": "relation"
      },
      "Label Name": {
        "id": "title",
        "type": "title"
      }
    }
  },
  "timeslices": {
    "properties": {
      "To Task Page ID": {
        "id": "AWGN",
        "type": "rollup"
      },
      "To Task Name": {
        "id": "E%3FuS",
        "type": "rollup"
      },
      "From Status": {
        "id": "He%3D%60",
        "type": "rollup"
      },
      "To Step N": {
        "id": "J%5CDl",
        "type": "formula"
      },
      "To Workflow Step": {
        "id": "KgCD",
        "type": "rollup"
      },
      "From Workflow Step": {
        "id": "Pe%3A%7C",
        "type": "rollup"
      },
      "Slice Label": {
        "id": "RYYr",
        "type": "formula"
      },
      "From Step N": {
        "id": "S%3B%5CZ",
        "type": "formula"
      },
      "Workflow Record": {
        "id": "U%3CU%7B",
        "type": "relation"
      },
      "Workflow Type": {
        "id": "XoTC",
        "type": "formula"
      },
      "Minutes Diff": {
        "id": "YdOL",
        "type": "formula"
      },
      "To Time": {
        "id": "cZbu",
        "type": "rollup"
      },
      "From Task Page ID": {
        "id": "fA%5DF",
        "type": "rollup"
      },
      "Workflow Definition": {
        "id": "fR%3E%3B",
        "type": "rollup"
      },
      "From Task Name": {
        "id": "iNer",
        "type": "rollup"
      },
      "From Time": {
        "id": "w%5Czt",
        "type": "rollup"
      },
      "From Event": {
        "id": "yBeO",
        "type": "relation"
      },
      "To Status": {
        "id": "%7Ba_e",
        "type": "rollup"
      },
      "To Event": {
        "id": "%7Dg%40%5E",
        "type": "relation"
      },
      "Name": {
        "id": "title",
        "type": "title"
      }
    }
  }
} as const;
