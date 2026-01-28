---
name: RAG Skill
description: A skill to answer questions based on documents in the `database` directory using RAG.
---

# RAG Skill

This skill allows the agent to answer questions by retrieving information from files within the `database` directory.

## Capabilities
- List all files in the `database` directory.
- Identify relevant files based on the user's question.
- Read content from CSV and Markdown files using a helper Python script.
- specific handling for CSV (pandas) and Markdown (text).

## Instructions

When the user asks a question that requires external knowledge from the `database` folder, follow these steps:

1.  **Analyze the Request**: Identify the core question.

2.  **Explore Database Structure**:
    -   Run the following command to get the full file tree of the `database` directory:
        ```bash
        find database -maxdepth 5 -not -path '*/.*'
        ```
    -   *Note*: Ensure you are in the project root or adjust the path accordingly.

3.  **Identify Candidates**:
    -   Review the file list from Step 2.
    -   Select files that are likely to contain information relevant to the user's question.
    -   List these candidate files in your thought process.

4.  **Read File Content**:
    -   For **EACH** candidate file identified in Step 3, you **MUST** use the provided Python script to read its content.
    -   Run the following command for each file:
        ```bash
        python3 skills/rag/scripts/read_data.py <path_to_file>
        ```
    -   *Note*: The script automatically handles `.csv` (using pandas/csv) and `.md`/`.txt` (text read).

5.  **Generate Answer**:
    -   Synthesize the information read from the files.
    -   Answer the user's question based *only* on the retrieved information.
    -   If the information is insufficient, state what is missing or perform a broader search if applicable (repeat from Step 2 with different keywords if possible, though usually file names are key).

## Dependencies
-   `python3`
-   `pandas` (optional, for better CSV formatting)
-   `skills/rag/scripts/read_data.py` (must exist)
