import os
import io
import base64
import json
import textwrap
from typing import List, Dict, Any, TypedDict
from PIL import Image
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.runnables import RunnableLambda
from tqdm import tqdm
import concurrent.futures

load_dotenv()

# Prompts copied/adapted from pdf-func.py
PDF_EXTRACTION_SYSTEM_PROMPT = textwrap.dedent(
    """
    You are a meticulous document transcription expert.
    Transcribe every piece of textual information from provided page images,
    including headers, footers, captions, annotations inside figures, callouts,
    and all table contents. Preserve the logical structure using Markdown headings,
    bullet lists, and Markdown tables. Do not omit numeric values or labels.
    """
).strip()

PDF_EXTRACTION_PAGE_INSTRUCTIONS = textwrap.dedent(
    """
    Extract everything visible on this page image. Preserve reading order when possible.
    Render tables as Markdown tables and include unit labels. If a figure or diagram
    contains text, reproduce it verbatim and provide a short description when helpful.
    Maintain Japanese text as-is. Respond in Markdown only.
    """
).strip()

SUMMARY_SYSTEM_PROMPT = textwrap.dedent(
    """
    あなたはPDF文書の各ページを分析する専門家です。
    以下の2つのタスクを行ってください。

    1. 要約: 重要なトピック、数字、箇条書き項目などを抽出し、日本語で簡潔にまとめる（最大3つの箇条書き）。
    2. 想定質問作成: ページの内容に基づき、試験や理解度確認に使えそうな想定質問と回答のペアを作成する。
       - ページの内容量に応じて、最低5つ作成すること。
       - 質問は日本語で作成する。
       - 各質問に対して、正解とみなすための重要な要素を箇条書きにした「チェックリスト」を作成すること。
       - 回答は、決して参考文献の内容に脚色をつけないでください。要約内に記載されている情報のみに基づいて作成してください。

    出力は必ず以下のJSON形式で行ってください。
    {
      "summary": "要約テキスト...",
      "questions": [
        {
          "question": "質問文...",
          "answer": "回答文...",
          "checklist": [
            "チェック項目1",
            "チェック項目2"
          ]
        }
      ]
    }
    """
).strip()

CATEGORY_SYSTEM_PROMPT = textwrap.dedent(
    """
    あなたはPDF文書の分析と分類を担当するアナリストです。
    入力としてページ番号とその簡潔な要約が与えられます。
    それらを意味のあるカテゴリに分け、カテゴリ名と説明を日本語で作成してください。
    各ページは必ずいずれか1つのカテゴリに割り当ててください。
    出力は以下のJSON形式のみを使用してください。
    {
      "categories": [
        { "name": "カテゴリ名", "description": "カテゴリの説明", "pages": [ページ番号, ...] }
      ]
    }
    """
).strip()

class PageInput(TypedDict):
    image: str # base64 data url
    page_number: int

# Models are now injected

def _pil_image_to_data_url(image: Image.Image) -> str:
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"

def _create_vision_messages(input_data: PageInput) -> List[Any]:
    data_url = input_data["image"]
    page_number = input_data["page_number"]
    instruction = f"Page {page_number:03}: {PDF_EXTRACTION_PAGE_INSTRUCTIONS}"
    return [
        SystemMessage(content=PDF_EXTRACTION_SYSTEM_PROMPT),
        HumanMessage(
            content=[
                {"type": "text", "text": instruction},
                {"type": "image_url", "image_url": {"url": data_url, "detail": "high"}},
            ]
        ),
    ]

def _create_summary_messages(markdown: str) -> List[Any]:
    snippet = markdown.strip()
    if len(snippet) > 6000:
        snippet = snippet[:6000] + "\n...[truncated]"
    return [
        SystemMessage(content=SUMMARY_SYSTEM_PROMPT),
        HumanMessage(content=f"以下はページの内容です。\n\n```markdown\n{snippet}\n```")
    ]

# Helper to create chains with injected models
def _create_image_to_markdown_chain(vision_model: ChatOpenAI):
    return (
        RunnableLambda(_create_vision_messages) 
        | vision_model 
        | (lambda x: x.content)
    )

def _create_summarize_chain(summary_model: ChatOpenAI):
    return (
        RunnableLambda(_create_summary_messages) 
        | summary_model 
        | (lambda x: json.loads(x.content))
    )

def process_pages_batch(
    images: List[Image.Image],
    vision_model: ChatOpenAI,
    summary_model: ChatOpenAI,
    max_concurrency: int = 100
) -> Dict[int, Dict[str, str]]:
    """
    Processes a batch of images: Image -> Markdown -> Summary.
    Returns a dict: {page_num: {"markdown": str, "summary": str}}
    """
    
    # Create chains with the provided models
    image_to_markdown_chain = _create_image_to_markdown_chain(vision_model)
    summarize_chain = _create_summarize_chain(summary_model)

    # Prepare inputs for vision model
    vision_inputs = []
    for i, img in enumerate(images):
        vision_inputs.append({
            "image": _pil_image_to_data_url(img),
            "page_number": i + 1
        })

    print(f"  Starting batch processing for {len(images)} pages with max_concurrency={max_concurrency}...")
    
    print(f"  Starting batch processing for {len(images)} pages with max_concurrency={max_concurrency}...")
    
    # Use ThreadPoolExecutor for visible progress with tqdm
    markdown_results = [None] * len(images)
    summary_results = [None] * len(images)

    # 1. Image -> Markdown
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        futures = {executor.submit(image_to_markdown_chain.invoke, item): i for i, item in enumerate(vision_inputs)}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(images), desc="Converting Images to Markdown"):
            index = futures[future]
            try:
                markdown_results[index] = future.result()
            except Exception as e:
                print(f"Error processing page {index+1}: {e}")
                markdown_results[index] = ""

    # 2. Markdown -> Summary & Questions
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        futures = {executor.submit(summarize_chain.invoke, md): i for i, md in enumerate(markdown_results)}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(images), desc="Summarizing & Generating Questions"):
            index = futures[future]
            try:
                summary_results[index] = future.result()
            except Exception as e:
                print(f"Error summarizing page {index+1}: {e}")
                summary_results[index] = {}
    
    results = {}
    for i in range(len(images)):
        page_num = i + 1
        results[page_num] = {
            "markdown": markdown_results[i],
            "summary": summary_results[i].get("summary", ""),
            "questions": summary_results[i].get("questions", [])
        }
    
    return results

def categorize_pages(summaries: Dict[int, str], category_model: ChatOpenAI) -> Dict[str, Any]:
    """
    Categorizes pages based on their summaries.
    This step is naturally checking all pages at once, so no batch needed here unless multiple PDFs.
    """
    if not summaries:
        return {"categories": []}
        
    input_text = ""
    for page_num in sorted(summaries.keys()):
        input_text += f"Page {page_num}: {summaries[page_num]}\n"
        
    try:
        messages = [
            SystemMessage(content=CATEGORY_SYSTEM_PROMPT),
            HumanMessage(content=input_text)
        ]
        response = category_model.invoke(messages)
        return json.loads(response.content)
    except Exception as e:
        print(f"Error during categorization: {e}")
        return {"categories": []}

# Legacy functions removed to enforce new pattern
