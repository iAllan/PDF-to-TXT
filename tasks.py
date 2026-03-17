# tasks.py
from celery_app import celery_app
import os
import shutil
from stripped_exctractor import process_pdf, save_output_with_pages, process_pdf_page_range
from verification import clean_output_file, analyze_output

UPLOAD_DIR = "data/uploads"
OUTPUT_DIR = "data/outputs"

@celery_app.task(bind=True, max_retries=3)
def process_pdf_task(self, pdf_filename: str, page_range: str = None):
    """
    Celery task that processes a PDF and writes the output text file.
    """
    pdf_path = os.path.join(UPLOAD_DIR, pdf_filename)
    txt_filename = pdf_filename.replace(".pdf", ".txt")
    txt_path = os.path.join(OUTPUT_DIR, txt_filename)

    try:
        if page_range:
            start, end = map(int, page_range.split("-"))
            page_texts, empty_pages, total_pages = process_pdf_page_range(
                pdf_path, start_page=start, end_page=end
            )
        else:
            page_texts, empty_pages, total_pages = process_pdf(pdf_path)

        save_output_with_pages(page_texts, empty_pages, txt_path)
        clean_output_file(txt_path)
        analyze_output(txt_path, total_pages)

        return {"status": "success", "txt_file": os.path.basename(txt_path)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}