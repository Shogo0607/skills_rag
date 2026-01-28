import os
import sys
import openai
import json
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

def main():
    if len(sys.argv) < 2:
        print("Usage: python rag_agent.py \"Your question here\"")
        sys.exit(1)
        
    question = sys.argv[1]
    
    database_dir = "database"
    if not os.path.exists(database_dir):
        print(f"Error: Directory '{database_dir}' not found.")
        sys.exit(1)

    # 0. Analyze new PDFs (Keep existing functionality)
    try:
        analyze_new_pdfs(database_dir, pdf_analysis_models)
    except Exception as e:
        print(f"Warning: PDF analysis failed: {e}")
        
    # 1. List files
    print("Searching for files...")
    all_files = list_files(database_dir)
    
    context = ""
    history_files = []
    max_loops = 10
    
    print("\n=== Start Thinking Loop ===")
    
    for i in range(max_loops):
        print(f"\n--- Loop {i+1}/{max_loops} ---")
        
        decision = decide_action(question, all_files, context, history_files)
        action = decision.get("action")
        payload = decision.get("payload")
        thought = decision.get("thought", "No thought provided")
        
        print(f"Thought: {thought}")
        print(f"Action: {action}")
        
        if action == "read":
            files_to_read = payload
            if isinstance(files_to_read, str):
                files_to_read = [files_to_read]
                
            new_info_found = False
            for file_path in files_to_read:
                if file_path in history_files:
                    print(f"Skipping already read file: {file_path}")
                    continue
                
                if file_path not in all_files:
                    print(f"Warning: Agent tried to read non-existent file: {file_path}")
                    continue

                print(f"Reading {file_path}...")
                content = read_file(file_path)
                context += f"\n\n--- File: {file_path} ---\n{content}"
                history_files.append(file_path)
                new_info_found = True
            
            if not new_info_found:
                 print("No new files were successfully read. Forcing answer generation.")
                 # Loop round again, agent should likely choose answer given previous history
                 # But to prevent getting stuck if agent is stubborn, we can break or continue.
                 # Let's count on agent intelligence for now, but if it loops too much it hits max_loops.
        
        elif action == "answer":
            print("\n=== Final Answer ===\n")
            print(payload)
            sys.exit(0)
            
        else:
            print(f"Unknown action: {action}")
            break
            
    print("\n=== Loop Limit Reached ===")
    print("Loop limit reached or error occurred. Generating best effort answer...")
    
    # Final fallback answer using whatever context we have
    final_decision = decide_action(question, all_files, context, history_files)
    if final_decision.get("action") == "answer":
         print(final_decision.get("payload"))
    else:
         # Force answer generation if decide_action still wants to read
         from openai import OpenAI
         fallback_resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": f"以下の情報に基づいて質問に答えてください。\n\nContext:\n{context}\n\nQuestion: {question}"}]
         )
         print(fallback_resp.choices[0].message.content)

if __name__ == "__main__":
    main()
