import os
import csv
import json
import shutil
import concurrent.futures
from typing import List, Dict, Set
from collections import defaultdict
from pathlib import Path
import openai
from dotenv import load_dotenv

load_dotenv()

class FolderOptimizer:
    def __init__(self, database_dir: str):
        self.database_dir = database_dir
        self.client = openai.OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY"),
            base_url=os.environ.get("OPENAI_API_BASE")
        )
        self.model = "gpt-4.1-mini"

    def run(self, results_csv_path: str, source_csv_path: str = None):
        print("\n=== Starting Folder Structure Optimization ===")
        
        # 1. Load failures
        # failures: {missing_file_path: [related_questions]}
        failures = self._load_failures(results_csv_path)
        if not failures:
            print("No failures requiring optimization found.")
            return

        print(f"Found {len(failures)} files with retrieval failures. Analyzing...")

        # 2. Analyze and Get Suggestions (Parallel)
        suggestions = self._get_suggestions_parallel(failures)

        # 3. Apply Suggestions
        self._apply_suggestions(suggestions)
        
        # 4. Update Source CSV with Optimized References
        if source_csv_path and suggestions:
             self._update_source_csv(source_csv_path, suggestions)
             
        print("=== Folder Optimization Complete ===\n")

    def _load_failures(self, csv_path: str) -> Dict[str, List[str]]:
        """
        Reads CSV and returns a dict mapping {missing_file_path: [list_of_questions]}
        """
        failures = defaultdict(list)
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Check metrics to detect failure
                    # We focus on cases where Ref Recall is poor (missed the file)
                    # or Ref Precision is 0 (implied miss or completely wrong)
                    try:
                        recall = float(row.get("Ref Recall", 0.0))
                    except ValueError:
                        recall = 0.0
                        
                    # If recall is < 1.0, it means we missed at least one reference.
                    # This is a good candidate for optimization.
                    if recall < 1.0:
                        gt_refs = row.get("Reference Document", "").split('\n')
                        retrieved_files = row.get("Retrieved Files", "").split('\n')
                        
                        # Normalize paths for comparison (strip whitespace)
                        retrieved_set = set(r.strip() for r in retrieved_files if r.strip())
                        
                        for ref in gt_refs:
                            ref = ref.strip()
                            if not ref:
                                continue
                                
                            # Check if this ref was found. 
                            # Current evaluator logic: endswith check.
                            found = False
                            for r in retrieved_set:
                                if r.endswith(ref) or ref.endswith(r):
                                    found = True
                                    break
                            
                            if not found:
                                failures[ref].append(row.get("Question", ""))
        except Exception as e:
            print(f"Error reading results CSV: {e}")
            
        return failures

    def _get_suggestions_parallel(self, failures: Dict[str, List[str]]) -> List[Dict]:
        suggestions = []
        max_workers = 5
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(self._analyze_single_file, f, qs): f 
                for f, qs in failures.items()
            }
            
            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        suggestions.append(result)
                except Exception as e:
                    print(f"Error analyzing {file_path}: {e}")
        return suggestions

    def _get_directory_structure(self) -> str:
        """Returns a list of all subdirectories within database_dir."""
        dirs_list = []
        try:
            for root, dirs, _ in os.walk(self.database_dir):
                for d in dirs:
                    if not d.startswith('.'):
                        full_path = os.path.join(root, d)
                        # Make relative to project root for clarity
                        dirs_list.append(full_path + "/")
        except Exception as e:
            print(f"Error listing directories: {e}")
            
        # Limit to reasonable size just in case
        return "\n".join(sorted(dirs_list)[:200])

    def _analyze_single_file(self, file_path: str, questions: List[str]) -> Dict:
        # Simplify questions list if too long to save tokens
        display_questions = questions[:5]
        
        # Get existing folders
        existing_structure = self._get_directory_structure()
        
        prompt = f"""
        あなたはRAGシステムのデータベース管理者です。
        以下のドキュメントファイルは、ユーザーの質問に対して正解の情報源であるにもかかわらず、RAGエージェントによって検索されませんでした。
        これは、現在のフォルダ構成やファイルパスが、質問の意図やキーワードと意味的に乖離している、あるいは深すぎて発見しにくいことが原因であると考えられます。

        対象ファイルパス (Current Path):
        {file_path}

        検索に失敗した質問の例:
        {json.dumps(display_questions, ensure_ascii=False, indent=2)}
        
        現在の作業スコープ (Database Directory):
        {self.database_dir}
        
        現在のスコープ内の既存フォルダ構成:
        {existing_structure}

        タスク:
        これらの質問に対して、このファイルがより自然に発見されるような、新しいフォルダ構造・ファイルパスを提案してください。
        
        制約:
        1. 必ず "{self.database_dir}" (またはそのサブディレクトリ) で始まるパスにしてください。
           ユーザーはこのディレクトリスコープ内で作業しているため、この範囲外にファイルを作成してはいけません。
        2. **既存のフォルダ構成を考慮してください。**
           - まず、既存のフォルダの中に、質問の意図に合致する「最適なフォルダ」が存在しないか確認してください。あれば、そのフォルダへのコピーを提案してください。
           - 既存のフォルダでは分類が不十分、あるいは意味的に遠いと判断した場合のみ、新しいフォルダを作成してください。
           - 新しいフォルダを作成する場合は、検索性を最大限に高める名前（キーワードを含む等）にしてください。
        3. ファイル名自体 (.md) は変更せず、ディレクトリ部分を改善してください（ただし、ファイル名が内容を表していない等の場合は変更も可）。
        4. 元のファイルをこの新しい場所に「コピー」して参照性を高める運用を想定しています。
        5. 既存のパスが十分に適切である、あるいはデータベース構造として変更すべきでないと判断した場合は null を返してください。

        出力フォーマット (JSON):
        {{
            "original_path": "{file_path}",
            "suggested_path": "{self.database_dir}/new_subcategory/filename.md",
            "reason": "質問には'エラーコード'という単語が含まれているが、現在のパスには含まれていないため、'error_codes'フォルダを追加した。"
        }}
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            content = response.choices[0].message.content
            data = json.loads(content)
            
            new_path = data.get("suggested_path")
            # Validation
            if new_path and new_path != file_path and new_path.startswith(self.database_dir):
                return data
            return None
        except Exception as e:
            print(f"Error in LLM analysis: {e}")
            return None

    def _apply_suggestions(self, suggestions: List[Dict]):
        if not suggestions:
            print("No structure improvements suggested.")
            return

        print(f"Applying {len(suggestions)} folder structure improvements...")
        
        for item in suggestions:
            orig_path = item["original_path"]
            new_path = item["suggested_path"]
            reason = item.get("reason", "")
            
            # Resolve absolute paths
            # Assumes running from project root where 'database' is relative
            abs_orig = os.path.abspath(orig_path)
            abs_new = os.path.abspath(new_path)
            
            if not os.path.exists(abs_orig):
                print(f"Skipping: Original file not found at {abs_orig}")
                continue
                
            if os.path.exists(abs_new):
                # If target already exists, we skip to avoid overwriting or duplicates
                # print(f"Skipping: Target file already exists at {abs_new}")
                continue
            
            # Additional check: Don't create if it's just the same file
            if abs_orig == abs_new:
                continue

            try:
                os.makedirs(os.path.dirname(abs_new), exist_ok=True)
                shutil.copy2(abs_orig, abs_new)
                print(f"[COPIED] {orig_path} -> {new_path}")
                print(f"  Reason: {reason}")
            except Exception as e:
                print(f"Error copying {orig_path} to {new_path}: {e}")

    def _update_source_csv(self, csv_path: str, suggestions: List[Dict]):
        """
        Updates the source CSV to include the new 'Optimized Reference Document' column.
        """
        print(f"Updating source CSV: {csv_path}")
        
        # Create a mapping from original path to list of new paths (in case multiple)
        # suggestions: list of dicts {original_path, suggested_path}
        path_map = defaultdict(list)
        for s in suggestions:
            path_map[s["original_path"]].append(s["suggested_path"])

        try:
            rows = []
            fieldnames = []
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                fieldnames = reader.fieldnames
                
            if "Optimized Reference Document" not in fieldnames:
                fieldnames.append("Optimized Reference Document")
            
            updated_count = 0
            for row in rows:
                existing_refs = row.get("Reference Document", "").split('\n')
                existing_opt_refs = row.get("Optimized Reference Document", "").split('\n')
                
                # Build a set of all valid references for this row
                # Start with existing Optimzed Refs if any, else start with standard Refs
                # If Optimized Refs is empty, we assume it should at least contain the standard Refs
                if not any(existing_opt_refs):
                     current_valid_refs = set(r.strip() for r in existing_refs if r.strip())
                else:
                     current_valid_refs = set(r.strip() for r in existing_opt_refs if r.strip())
                
                # Check if any original ref has been moved
                changed = False
                for ref in existing_refs:
                    ref = ref.strip()
                    if ref in path_map:
                        for new_path in path_map[ref]:
                            if new_path not in current_valid_refs:
                                current_valid_refs.add(new_path)
                                changed = True
                
                if changed:
                    row["Optimized Reference Document"] = "\n".join(sorted(current_valid_refs))
                    
                    # Consolidate aliases into pipe-separated string
                    # We need to preserve the line-by-line structure of the original reference if possible, 
                    # OR we just list all unique groups.
                    # Given the current logic:
                    # current_valid_refs contains ALL paths (orig + new).
                    
                    # We need to re-group them.
                    # Best effort grouping:
                    # 1. Start with original refs.
                    # 2. For each original ref, find its aliases from path_map.
                    # 3. Create a group string "orig|alias1|alias2"
                    
                    grouped_refs = []
                    processed_paths = set()
                    
                    # First pass: Group by original references
                    for ref in existing_refs:
                        ref = ref.strip()
                        if not ref: continue
                        
                        group = {ref}
                        processed_paths.add(ref)
                        
                        if ref in path_map:
                            for alias in path_map[ref]:
                                group.add(alias)
                                processed_paths.add(alias)
                        
                        grouped_refs.append("|".join(sorted(list(group))))
                        
                    # Second pass: Add any paths that were in current_valid_refs but not processed 
                    # (e.g. if they were added manually or in previous runs and we lost the mapping - 
                    # though with current logic we rebuild from scratch mostly).
                    # Actually, if we run multiple times, 'existing_refs' is the fixed ground truth.
                    # 'path_map' contains ONLY current run suggestions.
                    # If we want to persist previous optimizations, we should check existing_opt_refs too.
                    # Simpler approach: Trust 'existing_refs' as the anchor.
                    # If there are paths in 'existing_opt_refs' that are NOT in existing_refs and NOT in path_map,
                    # they might be orphan optimizations. We should probably keep them? 
                    # For now, let's stick to the mapping derived from suggestions + ground truth.
                    
                    row["Optimized Reference Document"] = "\n".join(grouped_refs)
                    updated_count += 1
                elif "Optimized Reference Document" not in row or not row["Optimized Reference Document"]:
                     # Ensure the column is populated even if no change
                     row["Optimized Reference Document"] = row.get("Reference Document", "")
            
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
                
            print(f"Updated {updated_count} rows in source CSV with optimized references.")
            
        except Exception as e:
            print(f"Error updating source CSV: {e}")
