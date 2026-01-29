import os
import sys
import openai
import json
import csv
import re
import concurrent.futures
from pathlib import Path
from typing import List, Dict, Tuple, Any

from skills.rag.scripts.read_data import read_file
from skills.rag.pdf_analysis.analyzer import analyze_new_pdfs
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from skills.rag.optimization.folder_optimizer import FolderOptimizer

load_dotenv()

# Initialize OpenAI client (for RAG part)
client = openai.OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
    base_url=os.environ.get("OPENAI_API_BASE")
)

# Initialize LangChain models (for PDF Analysis)
# Centralized configuration for models used in PDF processing
api_base = os.environ.get("OPENAI_API_BASE")
pdf_analysis_models = {
    "vision": ChatOpenAI(model="gpt-4.1-mini", temperature=0.0, base_url=api_base),
    "summary": ChatOpenAI(model="gpt-4.1-mini", temperature=0.0, base_url=api_base),
    "category": ChatOpenAI(model="gpt-4.1-mini", temperature=0.0, base_url=api_base).bind(response_format={"type": "json_object"}),
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
        self.client = openai.OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY"),
            base_url=os.environ.get("OPENAI_API_BASE")
        )
        self.model_name = model_name

    def evaluate_checklist(self, checklist_text: str, answer_text: str) -> Tuple[float, float, str, int, int, int]:
        """
        Evaluates the answer against the checklist.
        Returns: (recall, precision, reason, tp, fp, fn)
        Recall: % of checklist items found in answer.
        Precision: % of key points in answer that match checklist (approx).
        """
        if not checklist_text or not checklist_text.strip():
            return 0.0, 0.0, "No checklist provided", 0, 0, 0

        prompt = f"""
        あなたは回答の品質評価者です。
        
        以下の「チェックリスト」とシステムによる「回答」比較し、
        1. Checklist Recall (再現率): チェックリストの項目が、回答の中にどれだけ含まれているか (0.0 - 1.0)
        2. Checklist Precision (適合率): 回答に含まれる主要な主張のうち、チェックリストの項目と一致する割合 (0.0 - 1.0)
           ※ ここでは簡易的に、「回答の主要ポイント」を抽出し、そのうちチェックリストにあるものの割合とします。
        
        また、以下のカウントも行ってください:
        - TP (True Positive): 回答に含まれていたチェックリスト項目の数
        - FP (False Positive): チェックリストに含まれていないが、回答に含まれている主要なポイントの数
        - FN (False Negative): 回答に含まれていなかったチェックリスト項目の数
        
        チェックリスト:
        {checklist_text}
        
        回答:
        {answer_text}
        
        出力フォーマット (JSON):
        {{
            "recall": 0.8,
            "precision": 0.7,
            "tp": 4,
            "fp": 1,
            "fn": 1,
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
            return (
                data.get("recall", 0.0),
                data.get("precision", 0.0),
                data.get("reason", ""),
                data.get("tp", 0),
                data.get("fp", 0),
                data.get("fn", 0)
            )
        except Exception as e:
            print(f"Error in evaluate_checklist: {e}")
            return 0.0, 0.0, f"Error: {e}", 0, 0, 0

    def evaluate_references(self, gt_ref: str, gt_page: str, retrieved_files: List[str], all_files: List[str]) -> Tuple[float, float, float, int, int, int, int]:
        """
        Evaluates if retrieved files match ground truth.
        GT Ref: "path/to/page_XXX.md" (can be multiple, newline separated, and using '|' for aliases)
        GT Page: "1" (legacy)
        Retrieved: List of file paths read by the agent.
        all_files: List of all files in the database (for TN calculation).
        
        Returns: (recall, precision, specificity, tp, tn, fp, fn)
        """
        if not gt_ref:
            return 0.0, 0.0, 0.0, 0, 0, 0, 0
            
        # Parse GT references
        # Each "target" can be a set of aliases
        gt_targets = [] # List of sets
        for line in gt_ref.split('\n'):
             line = line.strip()
             if line:
                 aliases = set(a.strip() for a in line.split('|') if a.strip())
                 if aliases:
                     gt_targets.append(aliases)
        
        retrieved_set = set(retrieved_files)
        
        # Helper to check if a retrieved file matches ANY alias in a target set
        def matches_any_alias(retrieved_path, alias_set):
            for alias in alias_set:
                if retrieved_path.endswith(alias) or alias.endswith(retrieved_path):
                    return True
            return False

        # Helper to check if a target set is satisfied by ANY retrieved file
        def is_target_satisfied(alias_set, retrieved_files_set):
            for r in retrieved_files_set:
                if matches_any_alias(r, alias_set):
                    return True
            return False

        # TP: Number of GT targets that were satisfied
        tp = 0
        for target_aliases in gt_targets:
            if is_target_satisfied(target_aliases, retrieved_set):
                tp += 1
                
        # FN: Number of GT targets NOT satisfied
        fn = len(gt_targets) - tp
        
        # FP: Retrieved files that didn't match ANY target
        # Be careful: A retrieved file might match Target A. It shouldn't be FP.
        fp = 0
        for r in retrieved_set:
            matched_something = False
            for target_aliases in gt_targets:
                if matches_any_alias(r, target_aliases):
                    matched_something = True
                    break
            if not matched_something:
                fp += 1
        
        # TN = Total Files - (TP + FP + FN) 
        # Note: This is a rough estimation for "Document Retrieval" TN.
        total_files = len(all_files) if all_files else 0
        tn = max(0, total_files - (tp + fp + fn))

        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        specificity = tn / (tn + fp) if (tn + fp) > 0 else 0.0
            
        return recall, precision, specificity, tp, tn, fp, fn

def process_single_row(row: Dict[str, Any], index: int, total: int, database_dir: str, all_files: List[str], evaluator: Evaluator, has_ground_truth: bool) -> Tuple[int, Dict[str, Any]]:
    question = row.get("Question")
    if not question:
        return index, None
        
    print(f"Processing {index+1}/{total} (Thread): {question[:30]}...")
    
    # Run RAG (verbose=False to avoid log interleaving)
    rag_result = run_rag_session(question, database_dir, all_files, verbose=False)
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
        c_recall, c_precision, c_reason, c_tp, c_fp, c_fn = evaluator.evaluate_checklist(checklist, answer)
        result_row["Checklist Recall"] = c_recall
        result_row["Checklist Precision"] = c_precision
        result_row["Checklist TP"] = c_tp
        result_row["Checklist FP"] = c_fp
        result_row["Checklist FN"] = c_fn
        result_row["Evaluation Reason"] = c_reason
        
        # Evaluate References
        r_recall, r_precision, r_specificity, r_tp, r_tn, r_fp, r_fn = evaluator.evaluate_references(gt_ref, gt_page, history, all_files)
        result_row["Ref Recall"] = r_recall
        result_row["Ref Precision"] = r_precision
        result_row["Ref Specificity"] = r_specificity
        result_row["Ref TP"] = r_tp
        result_row["Ref TN"] = r_tn
        result_row["Ref FP"] = r_fp
        result_row["Ref FN"] = r_fn
        
        # Evaluate Optimized References if present
        opt_ref = row.get("Optimized Reference Document", "")
        if opt_ref:
            or_recall, or_precision, or_specificity, or_tp, or_tn, or_fp, or_fn = evaluator.evaluate_references(opt_ref, gt_page, history, all_files)
            result_row["Opt Ref Recall"] = or_recall
            result_row["Opt Ref Precision"] = or_precision
            result_row["Opt Ref Specificity"] = or_specificity
            result_row["Opt Ref TP"] = or_tp
            result_row["Opt Ref TN"] = or_tn
            result_row["Opt Ref FP"] = or_fp
            result_row["Opt Ref FN"] = or_fn
            
            print(f"[{index+1}] Checklist(R/P): {c_recall:.2f}/{c_precision:.2f} | Ref(R/P/S): {r_recall:.2f}/{r_precision:.2f}/{r_specificity:.2f} | OptRef(R/P/S): {or_recall:.2f}/{or_precision:.2f}/{or_specificity:.2f}")
        else:
            # Simple logging of results (might overlap in output)
            print(f"[{index+1}] Checklist(R/P): {c_recall:.2f}/{c_precision:.2f} | Ref(R/P/S): {r_recall:.2f}/{r_precision:.2f}/{r_specificity:.2f}")
    
    return index, result_row

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
    results = [] # Will store (index, row)
    
    print(f"Found {len(rows)} questions to process. Starting parallel execution...")
    
    max_workers = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i, row in enumerate(rows):
            futures.append(executor.submit(process_single_row, row, i, len(rows), database_dir, all_files, evaluator, has_ground_truth))
            
        for future in concurrent.futures.as_completed(futures):
            try:
                idx, res_row = future.result()
                if res_row:
                    results.append((idx, res_row))
            except Exception as e:
                print(f"Error processing row: {e}")

    # Sort by original index to maintain order
    results.sort(key=lambda x: x[0])
    final_rows = [r[1] for r in results]

    # Save output
    output_path = str(Path(csv_path).with_name(f"{Path(csv_path).stem}_results.csv"))
    
    # Determine new headers
    output_headers = list(rows[0].keys()) + ["RAG Answer", "Retrieved Files"]
    if has_ground_truth:
        if "Optimized Reference Document" in rows[0]:
             output_headers += [
                "Opt Ref Recall", "Opt Ref Precision", "Opt Ref Specificity", "Opt Ref TP", "Opt Ref TN", "Opt Ref FP", "Opt Ref FN"
             ]
        
        output_headers += [
            "Checklist Recall", "Checklist Precision", "Checklist TP", "Checklist FP", "Checklist FN", "Evaluation Reason",
            "Ref Recall", "Ref Precision", "Ref Specificity", "Ref TP", "Ref TN", "Ref FP", "Ref FN"
        ]
        
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=output_headers)
        writer.writeheader()
        writer.writerows(final_rows)
        
    print(f"\nBatch processing complete. Results saved to: {output_path}")

    # Run Folder Optimization if Ground Truth was present
    if has_ground_truth:
        try:
            optimizer = FolderOptimizer(database_dir)
            optimizer.run(output_path, csv_path)
        except Exception as e:
            print(f"Warning: Folder optimization failed: {e}")

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
