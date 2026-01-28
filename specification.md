# Specification

## Question Generation
- When analyzing PDFs, the system generates "Anticipated Questions".
- Each question must include:
    - Question text
    - Ground Truth (Answer)
    - Reference Document (Path)
    - Page Number(s)
    - **Checklist** (Evaluation Criteria)
        - A list of key points that must be included in the answer to be considered correct.
        - Format: Bulleted list or newlines in the CSV cell.

## Output Format
- `database/subdirectory/想定質問/想定質問.csv`
- Columns: `Question`, `Ground Truth`, `Reference Document`, `Page`, `Checklist`
