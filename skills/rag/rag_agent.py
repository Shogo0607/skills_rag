import os
import sys
import openai
import json
import csv
import re
from pathlib import Path
from typing import List, Dict, Tuple, Any

from skills.rag.scripts.read_data import read_file
from skills.rag.pdf_analysis.analyzer import analyze_new_pdfs
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()

# Initialize OpenAI client (for RAG part)
client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

# Initialize LangChain models (for PDF Analysis)
# Centralized configuration for models used in PDF processing
pdf_analysis_models = {
    "vision": ChatOpenAI(model="gpt-4.1-mini", temperature=0.0),
    "summary": ChatOpenAI(model="gpt-4.1-mini", temperature=0.0),
    "category": ChatOpenAI(model="gpt-4.1-mini", temperature=0.0).bind(response_format={"type": "json_object"}),
}

def list_files(directory):
    """
    List all files in the given directory recursively, excluding hidden files.
    """
    file_list = []
    for root, dirs, files in os.walk(directory):
        # Remove hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for file in files:
            if not file.startswith('.') and not file.lower().endswith('.pdf'):
                file_list.append(os.path.join(root, file))
    return file_list

def decide_action(question, file_list, context, history_files):
    """
    Decide the next action based on the question, file list, and current context.
    Returns a dict with 'action' ("read" or "answer") and 'payload'.
    """
    prompt = f"""
    あなたは優秀なAIアシスタントです。
    ユーザーの質問に答えるために、必要な情報を収集しています。

    ユーザーの質問: "{question}"
    
    これまでに読み込んだ情報 (Context):
    {context}
    
    これまでに読んだファイル: {json.dumps(history_files, ensure_ascii=False)}
    
    利用可能なファイルリスト:
    {json.dumps(file_list, indent=2, ensure_ascii=False)}
    
    次の行動を決定してください:
    1. 情報が不十分な場合: "read" アクションを選択し、読むべきファイルのパス（複数可）を指定してください。
       (既に読んだファイルは指定しないでください)
    2. 情報が十分、またはこれ以上有効なファイルがない場合: "answer" アクションを選択し、回答を作成してください。
    
    出力フォーマット (JSON):
    {{
        "action": "read" | "answer",
        "payload": ["path/to/file1", "path/to/file2"]  (action="read"の場合)
                   OR
                   "ここに回答のテキストを記述" (action="answer"の場合)
         "thought": "なぜこの行動を選んだかの短い理由"
    }}
    JSONのみを出力してください。
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        content = response.choices[0].message.content
        return json.loads(content)
    except Exception as e:
        print(f"Error in decide_action: {e}")
        # Fallback to answer if something breaks to avoid infinite error loops
        return {"action": "answer", "payload": "エラーが発生したため、現在の情報で回答できませんでした。"}

def run_rag_session(question: str, database_dir: str, all_files: List[str] = None, verbose: bool = True) -> Dict[str, Any]:
    """
    Runs the RAG thinking loop for a single question.
    Returns a dict: {"answer": str, "history_files": List[str]}
    """
    if all_files is None:
        if verbose: print("Searching for files...")
        all_files = list_files(database_dir)
        
    context = ""
    history_files = []
    max_loops = 10
    
    if verbose: print("\n=== Start Thinking Loop ===")
    
    for i in range(max_loops):
        if verbose: print(f"\n--- Loop {i+1}/{max_loops} ---")
        
        decision = decide_action(question, all_files, context, history_files)
        action = decision.get("action")
        payload = decision.get("payload")
        thought = decision.get("thought", "No thought provided")
        
        if verbose:
            print(f"Thought: {thought}")
            print(f"Action: {action}")
        
        if action == "read":
            files_to_read = payload
            if isinstance(files_to_read, str):
                files_to_read = [files_to_read]
                
            new_info_found = False
            for file_path in files_to_read:
                if file_path in history_files:
                    if verbose: print(f"Skipping already read file: {file_path}")
                    continue
                
                if file_path not in all_files:
                    if verbose: print(f"Warning: Agent tried to read non-existent file: {file_path}")
                    continue

                if verbose: print(f"Reading {file_path}...")
                content = read_file(file_path)
                context += f"\n\n--- File: {file_path} ---\n{content}"
                history_files.append(file_path)
                new_info_found = True
            
            if not new_info_found:
                 if verbose: print("No new files were successfully read. Forcing answer generation.")
        
        elif action == "answer":
            if verbose: print("\n=== Final Answer ===\n")
            if verbose: print(payload)
            return {"answer": payload, "history_files": history_files}
            
        else:
            if verbose: print(f"Unknown action: {action}")
            break
            
    if verbose: print("\n=== Loop Limit Reached ===")
    
    # Final fallback answer
    final_decision = decide_action(question, all_files, context, history_files)
    if final_decision.get("action") == "answer":
         answer = final_decision.get("payload")
    else:
         fallback_resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": f"以下の情報に基づいて質問に答えてください。\n\nContext:\n{context}\n\nQuestion: {question}"}]
         )
         answer = fallback_resp.choices[0].message.content
         
    if verbose: print(answer)
    return {"answer": answer, "history_files": history_files}

class Evaluator:
    def __init__(self, model_name="gpt-4.1-mini"):
        self.client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        self.model_name = model_name

    def evaluate_checklist(self, checklist_text: str, answer_text: str) -> Tuple[float, float, str]:
        """
        Evaluates the answer against the checklist.
        Returns: (recall, precision, reason)
        Recall: % of checklist items found in answer.
        Precision: % of key points in answer that match checklist (approx).
        """
        if not checklist_text or not checklist_text.strip():
            return 0.0, 0.0, "No checklist provided"

        prompt = f"""
        あなたは回答の品質評価者です。
        
        以下の「チェックリスト」とシステムによる「回答」比較し、
        1. Checklist Recall (再現率): チェックリストの項目が、回答の中にどれだけ含まれているか (0.0 - 1.0)
        2. Checklist Precision (適合率): 回答に含まれる主要な主張のうち、チェックリストの項目と一致する割合 (0.0 - 1.0)
           ※ ここでは簡易的に、「回答の主要ポイント」を抽出し、そのうちチェックリストにあるものの割合とします。
        
        チェックリスト:
        {checklist_text}
        
        回答:
        {answer_text}
        
        出力フォーマット (JSON):
        {{
            "recall": 0.8,
            "precision": 0.7,
            "reason": "チェックリストの項目XとYは含まれていたが、Zは欠落していた。回答には独自の情報Aが含まれていたためPrecisionが下がった。"
        }}
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            data = json.loads(response.choices[0].message.content)
            return data.get("recall", 0.0), data.get("precision", 0.0), data.get("reason", "")
        except Exception as e:
            print(f"Error in evaluate_checklist: {e}")
            return 0.0, 0.0, f"Error: {e}"

    def evaluate_references(self, gt_ref: str, gt_page: str, retrieved_files: List[str]) -> Tuple[float, float]:
        """
        Evaluates if retrieved files match ground truth.
        GT Ref: "database/path/to/page_XXX.md" (can be multiple, newline separated)
        GT Page: "1" (legacy, used if needed, but GT Ref should be authoritative now)
        Retrieved: List of file paths read by the agent.
        
        Returns: (recall, precision)
        """
        if not gt_ref:
            return 0.0, 0.0
            
        # Parse GT references (handle multiple lines)
        gt_targets = set()
        for ref in gt_ref.split('\n'):
             ref = ref.strip()
             if ref:
                 gt_targets.add(ref)
        
        if not gt_targets:
            return 0.0, 0.0

        # Create a set of retrieved files for easier checking
        retrieved_set = set(retrieved_files)
        
        # Calculate Recall: How many GT targets were found?
        matches = 0
        for target in gt_targets:
             # Check if target is in retrieved_set
             # Allow for absolute/relative differences by checking suffix
             found = False
             for r in retrieved_set:
                 # Check if strings match or one ends with the other (to handle relative vs absolute)
                 # Target: database/.../page_008.md
                 # Retrieved: /abs/path/database/.../page_008.md OR database/.../page_008.md
                 if r.endswith(target) or target.endswith(r):
                     found = True
                     break
             if found:
                 matches += 1
                 
        recall = matches / len(gt_targets) if gt_targets else 0.0
        
        # Calculate Precision: How many retrieved files were relevant?
        # Only count files that match at least one GT target.
        relevant_retrieved = 0
        if retrieved_set:
            for r in retrieved_set:
                 is_relevant = False
                 for target in gt_targets:
                     if r.endswith(target) or target.endswith(r):
                         is_relevant = True
                         break
                 if is_relevant:
                     relevant_retrieved += 1
            
            precision = relevant_retrieved / len(retrieved_set)
        else:
            precision = 0.0
            
        return recall, precision

def process_batch_csv(csv_path: str, database_dir: str):
    print(f"Processing Batch CSV: {csv_path}")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = reader.fieldnames
        
    if not rows:
        print("CSV is empty.")
        return

    # Check mode
    has_ground_truth = "Ground Truth" in headers and "Checklist" in headers
    
    # Pre-fetch file list once
    print("Indexing files...")
    all_files = list_files(database_dir)
    
    evaluator = Evaluator()
    results = []
    
    print(f"Found {len(rows)} questions to process.")
    
    for i, row in enumerate(rows):
        question = row.get("Question")
        if not question: 
            continue
            
        print(f"\nProcessing {i+1}/{len(rows)}: {question[:50]}...")
        
        # Run RAG
        rag_result = run_rag_session(question, database_dir, all_files, verbose=True) # Set verbose=False for cleaner batch output
        answer = rag_result["answer"]
        history = rag_result["history_files"]
        
        result_row = row.copy()
        result_row["RAG Answer"] = answer
        result_row["Retrieved Files"] = "\n".join(history)
        
        if has_ground_truth:
            checklist = row.get("Checklist", "")
            gt_ref = row.get("Reference Document", "")
            gt_page = row.get("Page", "")
            
            # Evaluate Checklist
            c_recall, c_precision, c_reason = evaluator.evaluate_checklist(checklist, answer)
            result_row["Checklist Recall"] = c_recall
            result_row["Checklist Precision"] = c_precision
            result_row["Evaluation Reason"] = c_reason
            
            # Evaluate References
            r_recall, r_precision = evaluator.evaluate_references(gt_ref, gt_page, history)
            result_row["Ref Recall"] = r_recall
            result_row["Ref Precision"] = r_precision
            
            print(f"  -> C-Recall: {c_recall:.2f}, C-Precision: {c_precision:.2f}")
            print(f"  -> R-Recall: {r_recall:.2f}, R-Precision: {r_precision:.2f}")
            
        results.append(result_row)
        
    # Save output
    output_path = str(Path(csv_path).with_name(f"{Path(csv_path).stem}_results.csv"))
    
    # Determine new headers
    output_headers = list(rows[0].keys()) + ["RAG Answer", "Retrieved Files"]
    if has_ground_truth:
        output_headers += ["Checklist Recall", "Checklist Precision", "Evaluation Reason", "Ref Recall", "Ref Precision"]
        
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=output_headers)
        writer.writeheader()
        writer.writerows(results)
        
    print(f"\nBatch processing complete. Results saved to: {output_path}")

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Single Mode: python rag_agent.py \"Question\" [subdirectory]")
        print("  Batch Mode:  python rag_agent.py input.csv [subdirectory]")
        sys.exit(1)
        
    arg1 = sys.argv[1]
    
    # Determine Database Directory
    base_database_dir = "database"
    if len(sys.argv) > 2:
        # Check if arg2 is a subdirectory or part of database path
        target_subdir = sys.argv[2]
        database_dir = os.path.join(base_database_dir, target_subdir)
    else:
        database_dir = base_database_dir
        
    if not os.path.exists(database_dir):
        # Fail gracefully if directory doesn't exist? Or maybe arg2 was something else?
        # For now assume it's the database dir.
        print(f"Error: Directory '{database_dir}' not found.")
        sys.exit(1)
        
    # PDF Analysis (Always run on startup)
    try:
        analyze_new_pdfs(database_dir, pdf_analysis_models)
    except Exception as e:
        print(f"Warning: PDF analysis failed: {e}")

    # Dispatch based on extension
    if arg1.lower().endswith('.csv'):
        if not os.path.exists(arg1):
             print(f"Error: CSV file '{arg1}' not found.")
             sys.exit(1)
        process_batch_csv(arg1, database_dir)
    else:
        # Single Question Mode
        run_rag_session(arg1, database_dir)

if __name__ == "__main__":
    main()
