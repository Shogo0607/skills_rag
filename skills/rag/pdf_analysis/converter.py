import pdfplumber
from PIL import Image
from typing import List
from pathlib import Path

def convert_pdf_to_images(pdf_path: Path) -> List[Image.Image]:
    """
    Converts each page of a PDF file into a PIL Image.
    Returns a list of PIL Images.
    """
    images = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Use a reasonable resolution for text extraction/OCR
                # 150-300 dpi is usually good. pdf-func.py used 150 for pptx, 
                # but for PDF conversion via pdfplumber, the default or specific resolution 
                # depends on the to_image method.
                # pdfplumber's to_image() returns a PageImage object. 
                # .original gives the PIL image at default resolution (72dpi matches PDF point size usually),
                # but we might want higher quality.
                # Let's use resolution=150 as a baseline.
                p_image = page.to_image(resolution=150)
                images.append(p_image.original)
    except Exception as e:
        print(f"Error converting PDF to images: {e}")
        # Return whatever we managed to collect or empty list
        
    return images
