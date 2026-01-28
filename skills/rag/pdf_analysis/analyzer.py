import os
from pathlib import Path
import csv
import json
from langchain_core.messages import SystemMessage, HumanMessage
from skills.rag.pdf_analysis.file_manager import (
    find_unanalyzed_pdfs,
    create_output_directory,
    save_markdown,
    rename_processed_pdf
)
from skills.rag.pdf_analysis.converter import convert_pdf_to_images
from skills.rag.pdf_analysis.document_processor import (
    process_pages_batch,
    categorize_pages
)

from typing import Dict, Any, List

MULTIPAGE_QUESTION_PROMPT = """
あなたは文書全体の分析を行う専門家です。
以下の「各ページの要約」を読み、複数のページ（異なるページ）の情報を組み合わせないと回答できないような
「複合的な質問」と「回答」のペアを作成してください。

- 最低3つ、最大10個作成してください。
- 各質問には、回答の根拠となる「参照ページ番号（複数）」を明記してください。
- 各質問に対して、正解とみなすための重要な要素を箇条書きにした「チェックリスト」を作成してください。
- 回答は、決して参考文献の内容に脚色をつけないでください。提供された要約の情報のみに基づいて作成してください。

出力形式 (JSON):
{
  "questions": [
    {
      "question": "質問文...",
      "answer": "回答文...",
      "reference_pages": [1, 5, 10],
      "checklist": [
        "チェック項目1",
        "チェック項目2"
      ]
    }
  ]
}
"""

def generate_multipage_questions(summaries: Dict[int, str], model: Any) -> List[Dict]:
    if not summaries:
        return []
    
    input_text = ""
    for p in sorted(summaries.keys()):
        input_text += f"Page {p}: {summaries[p]}\n"

    messages = [
        SystemMessage(content=MULTIPAGE_QUESTION_PROMPT),
        HumanMessage(content=input_text)
    ]
    
    try:
        response = model.invoke(messages)
        content = json.loads(response.content)
        return content.get("questions", [])
    except Exception as e:
        print(f"Error generating multi-page questions: {e}")
        return []

def save_questions_csv(questions: List[Dict], output_path: Path):
    if not questions:
        return
    
    fieldnames = ["Question", "Ground Truth", "Reference Document", "Page", "Checklist"]
    
    try:
        with open(output_path, "w", newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(questions)
        print(f"  Saved {len(questions)} questions to {output_path.name}")
    except Exception as e:
        print(f"  Failed to save questions CSV: {e}")


def analyze_new_pdfs(database_dir: str, models: Dict[str, Any]):
    """
    Main entry point. Finds unanalyzed PDFs in the database directory
    and processes them.
    models: dict containing 'vision', 'summary', and 'category' LangChain models.
    """
    print(f"Checking for unanalyzed PDFs in {database_dir}...")
    pdf_files = find_unanalyzed_pdfs(database_dir)
    
    if not pdf_files:
        print("No new PDFs to analyze.")
        return

    print(f"Found {len(pdf_files)} PDF(s) to analyze.")

    for pdf_path in pdf_files:
        _process_single_pdf(pdf_path, database_dir, models)

def _process_single_pdf(pdf_path: Path, database_dir: str, models: Dict[str, Any]):
    print(f"Processing {pdf_path.name}...")
    
    # 1. Convert to images
    print("  Converting to images...")
    images = convert_pdf_to_images(pdf_path)
    if not images:
        print(f"  Failed to convert {pdf_path.name} to images. Skipping.")
        return

    # 2. Process all pages in batch
    print(f"  Processing {len(images)} pages (parallel)...")
    # This will handle image->markdown and markdown->summary in batch
    page_data = process_pages_batch(
        images, 
        vision_model=models["vision"],
        summary_model=models["summary"],
        max_concurrency=100
    )
    
    summaries_for_categorization = {
        p_num: data["summary"] for p_num, data in page_data.items()
    }

    # 3. Categorize
    print("  Categorizing pages...")
    categories_result = categorize_pages(summaries_for_categorization, category_model=models["category"])
    categories = categories_result.get("categories", [])
    
    # Map page number to category folder name
    # Default to "Uncategorized" if not assigned
    page_to_category = {}
    
    # First ensure all pages in categories are mapped
    for cat in categories:
        cat_name = cat.get("name", "Uncategorized")
        # Sanitize category name for filesystem
        safe_cat_name = "".join(c for c in cat_name if c.isalnum() or c in (' ', '_', '-')).strip()
        if not safe_cat_name:
            safe_cat_name = "Category_Unnamed"
            
        for p_num in cat.get("pages", []):
            page_to_category[p_num] = safe_cat_name

    # 4. Create output structure and save files
    print("  Saving results...")
    base_output_dir = create_output_directory(pdf_path)
    
    for page_num, data in page_data.items():
        cat_name = page_to_category.get(page_num, "Uncategorized")
        
        # Create category directory inside the PDF's output directory
        category_dir = base_output_dir / cat_name
        
        # Save markdown
        md_filename = f"page_{page_num:03}.md"
        save_markdown(data["markdown"], category_dir / md_filename)

    # 4.5. Generate and Save Questions
    print("  Generating and saving questions...")
    all_questions = []

    # Collect single page questions
    page_to_md_path = {}
    
    for page_num, data in page_data.items():
        cat_name = page_to_category.get(page_num, "Uncategorized")
        
        # Construct markdown filename and path
        md_filename = f"page_{page_num:03}.md"
        # base_output_dir is where we are strictly saving the file. 
        # It is usually relative if database_dir was relative.
        saved_md_path = base_output_dir / cat_name / md_filename
        
        # Ensure path format starts with database/ or relative from project root
        # If saved_md_path is absolute, try to make it relative to CWD.
        try:
             if saved_md_path.is_absolute():
                 ref_path = str(saved_md_path.relative_to(Path.cwd()))
             else:
                 ref_path = str(saved_md_path)
        except ValueError:
             # Fallback if cannot make relative (e.g. outside CWD)
             ref_path = str(saved_md_path)

        page_to_md_path[page_num] = ref_path

        for q in data["questions"]:
            all_questions.append({
                "Question": q["question"],
                "Ground Truth": q["answer"],
                "Reference Document": ref_path,
                "Page": str(page_num),
                "Checklist": "\n".join([f"・{item}" for item in q.get("checklist", [])])
            })
            
    # Generate multi-page questions
    summaries = {p: d["summary"] for p, d in page_data.items()}
    # Reuse summary model (which is JSON bound) for multi-page questions
    multi_questions = generate_multipage_questions(summaries, models["summary"])
    
    for q in multi_questions:
        refs = []
        for p_num in q.get("reference_pages", []):
             if p_num in page_to_md_path:
                 refs.append(page_to_md_path[p_num])
        
        # Join multiple references with newline
        ref_str = "\n".join(refs)

        all_questions.append({
            "Question": q["question"],
            "Ground Truth": q["answer"],
            "Reference Document": ref_str,
            "Page": ",".join(map(str, q.get("reference_pages", []))),
            "Checklist": "\n".join([f"・{item}" for item in q.get("checklist", [])])
        })
        
    # Save to CSV
    questions_dir = base_output_dir / "想定質問"
    questions_dir.mkdir(exist_ok=True)
    save_questions_csv(all_questions, questions_dir / "想定質問.csv")

    # 5. Rename original PDF
    print("  Renaming processed PDF...")
    try:
        new_path = rename_processed_pdf(pdf_path)
        print(f"  Finished {pdf_path.name} -> {new_path.name}")
    except Exception as e:
        print(f"  Failed to rename PDF: {e}")
