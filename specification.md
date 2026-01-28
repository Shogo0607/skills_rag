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
- Output: `database/subdirectory/想定質問/想定質問.csv` (encoding: `utf-8-sig`)
- Columns: `Question`, `Ground Truth`, `Reference Document`, `Page`, `Checklist`
- **Reference Document Format**:
    - Must be the relative path to the generated markdown file for the specific page.
    - Example: `database/冷蔵庫/r_h54xg_b/操作と機能/page_008.md`

## RAG Agent
### Modes
1.  **Single Question Mode**:
    - Usage: `python rag_agent.py "Question"`
    - Outputs answer to stdout.
2.  **Batch Processing Mode**:
    - Usage: `python rag_agent.py input.csv`
    - Input CSV can have:
        - `Question` only: Generates answers.
        - `Question`, `Ground Truth`, `Reference Document`, `Page`, `Checklist`: performing RAG and evaluating accuracy.

### Evaluation Metrics
When Ground Truth is provided, the agent calculates:
1.  **Checklist Verification**:
    - **Recall**: Percentage of checklist items present in the RAG answer.
    - **Precision**: Percentage of key points in the RAG answer that match the checklist.
2.  **Reference Verification**:
    - **Recall**: Percentage of Ground Truth pages that were correctly retrieved.
    - **Precision**: Percentage of retrieved pages that match the Ground Truth pages.
    - **Recall**: Percentage of Ground Truth pages that were correctly retrieved.
    - **Precision**: Percentage of retrieved pages that match the Ground Truth pages.
    - Logic: Matches the full path of the markdown file (Reference Document now points to the specific file).
