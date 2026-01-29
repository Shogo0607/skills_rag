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

When the user asks a question that requires external knowledge from the `database` folder, or asks to perform analysis on PDF files, follow these steps.

### 1. Identify the Mode

Determine if the user wants to:
-   **Analyze PDFs**: Run the agent to scan for new PDFs.
-   **Ask a Question**: Get an answer based on existing documents.
-   **Process a Batch**: Run multiple questions from a CSV file.

### 2. Execution

Use the `skills.rag.rag_agent` module.

#### Case A: PDF Analysis / General Startup
To simply run PDF analysis (and check for unanalyzed files):
```bash
python -m skills.rag.rag_agent "help"
```
*(The agent always runs PDF analysis on startup. Using "help" or no arguments will show usage but trigger analysis first.)*

#### Case B: Single Question RAG
To answer a specific question using the database:
```bash
python -m skills.rag.rag_agent "ここに質問内容を記述"
```
-   **Context**: The agent will search `database` for relevant files, read them, and generate an answer.
-   **Output**: The answer will be printed to stdout.

#### Case C: Batch Processing
To process a list of questions from a CSV file:
```bash
python -m skills.rag.rag_agent "path/to/input.csv"
```
-   **Input CSV Format**:
    -   Header: `Question` (Required)
    -   Optional: `Ground Truth`, `Reference Document`, `Checklist` (for evaluation)
-   **Output**: A new CSV file `original_name_results.csv` will be created with answers and metrics.

### 3. Dependencies
-   `python3`
-   `openai`
-   `langchain`
-   `skills/rag/rag_agent.py` (Main entry point)

