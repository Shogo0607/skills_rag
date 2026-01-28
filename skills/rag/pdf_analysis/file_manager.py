import os
import shutil
from pathlib import Path
from typing import List

def find_unanalyzed_pdfs(directory: str) -> List[Path]:
    """
    Finds PDF files in the directory that have not been analyzed yet.
    A PDF is considered analyzed if its name ends with '_analyzed.pdf'.
    Returns a list of Path objects for the unanalyzed PDFs.
    """
    pdf_files = []
    base_path = Path(directory)
    
    if not base_path.exists():
        return []

    for file_path in base_path.rglob("*.pdf"):
        if file_path.name.endswith("_analyzed.pdf"):
            continue
        
        # Check if the analyzed version already exists to avoid re-processing original
        # if the renaming failed or if we have both.
        # But per requirements, we rename the original. 
        # So just checking the name is sufficient for now.
        pdf_files.append(file_path)
            
    return pdf_files

def create_output_directory(pdf_path: Path) -> Path:
    """
    Creates a directory for the PDF analysis results.
    The directory is created at the same level as the PDF, with the same name (without extension).
    Returns the Path to the created directory.
    """
    output_dir = pdf_path.parent / pdf_path.stem
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

def save_markdown(content: str, output_path: Path) -> None:
    """
    Saves the markdown content to the specified path.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

def rename_processed_pdf(pdf_path: Path) -> Path:
    """
    Renames the processed PDF file by appending '_analyzed' to the filename.
    Returns the Path to the renamed file.
    """
    new_name = f"{pdf_path.stem}_analyzed{pdf_path.suffix}"
    new_path = pdf_path.parent / new_name
    pdf_path.rename(new_path)
    return new_path
