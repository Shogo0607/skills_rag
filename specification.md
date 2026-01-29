
## Specification

### Configuration
- Environment Variables (.env):
    - `OPENAI_API_KEY`: Required.
    - `OPENAI_API_BASE`: Optional. Base URL for the LLM API (e.g. for compatible endpoints).

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
    - Evaluation columns added: `RAG Answer`, `Retrieved Files`, `Checklist Recall`, `Checklist Precision`, `Checklist TP`, `Checklist FP`, `Checklist FN`, `Ref Recall`, `Ref Precision`, `Ref Specificity`, `Ref TP`, `Ref TN`, `Ref FP`, `Ref FN`, `Evaluation Reason`
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
    - **Parallel Processing**:
        - Batch processing runs in parallel to improve speed.
        - Default max workers: 5 (adjustable in code).

### Evaluation Metrics
When Ground Truth is provided, the agent calculates the following.
The results are saved to the CSV with detailed counts (TP, TN, FP, FN).

1.  **Checklist Verification**:
    - **Recall**: Percentage of checklist items present in the RAG answer.
    - **Precision**: Percentage of key points in the RAG answer that match the checklist.
    - **Specificity**: (N/A for Checklist usually, or calculated if applicable).
    - **Counts**:
        - **TP**: Number of checklist items found in the answer.
        - **FP**: Number of keys points in answer NOT in checklist.
        - **FN**: Number of checklist items NOT found in answer.
        - **TN**: (N/A)

2.  **Reference Verification**:
    - **Recall**: Percentage of Ground Truth pages that were correctly retrieved.
    - **Precision**: Percentage of retrieved pages that match the Ground Truth pages.
    - **Specificity**: Percentage of irrelevant pages that were correctly NOT retrieved.
        - Formula: TN / (TN + FP)
    - **Counts**:
        - **TP** (True Positive): Relevant pages retrieved.
        - **TN** (True Negative): Irrelevant pages NOT retrieved.
        - **FP** (False Positive): Irrelevant pages retrieved.
        - **FN** (False Negative): Relevant pages NOT retrieved.
    - Logic: Matches the full path of the markdown file.
    - **Aliases**: Can handle multiple valid paths for the same logical content using `|` separator.
      - Example: `path/to/original.md|path/to/optimized_copy.md`
      - If ANY of these paths are retrieved, it counts as a True Positive (TP).
    
3.  **Optimized Reference Verification** (New):
    - Calculates the same metrics as Reference Verification but against the `Optimized Reference Document` column.
    - This allows comparing performance before and after folder optimization.
    
### Folder Structure Optimization
- **Trigger**: Runs automatically after batch processing if Ground Truth is provided.
- **Goal**: Improve reference retrieval by creating better semantic paths for files that were missed (Recall < 1.0).
- **Process**:
    1.  Analyzes failed queries (where reference was not found).
    2.  Fetches the **existing directory structure** of the current scope to inform the LLM.
    3.  Uses LLM to propose a new, more intuitive folder path within the **current working directory scope**.
        - **Strategy**: Prioritizes copying to an **existing** folder if it is optimal for the query. Only creates a **new** folder if existing ones are insufficient.
    4.  **Copies** the missed file to the new location.
    5.  **Updates** the input CSV: Adds/Updates `Optimized Reference Document` column containing both the original and new paths.
- **Parallelization**: The analysis of failed cases runs in parallel using `ThreadPoolExecutor`.
