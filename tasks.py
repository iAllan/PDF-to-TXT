from celery_app import celery_app
import os
import traceback
from stripped_extractor import process_pdf, save_output_with_pages, process_pdf_page_range
from verification import clean_output_file, analyze_output

UPLOAD_DIR = "data/uploads"
OUTPUT_DIR = "data/outputs"
batch_size = 10

@celery_app.task(bind=True, max_retries=3)
def process_pdf_task(self, pdf_filename: str, page_range: str = None):
    pdf_path = os.path.join(UPLOAD_DIR, pdf_filename)
    txt_filename = pdf_filename.replace(".pdf", ".txt")
    txt_path = os.path.join(OUTPUT_DIR, txt_filename)

    try:
        if page_range:
            start, end = map(int, page_range.split("-"))
            page_texts, empty_pages, total_pages = process_pdf_page_range(
                pdf_path, start_page=start, end_page=end, batch_size=batch_size
            )
        else:
            page_texts, empty_pages, total_pages = process_pdf(pdf_path, batch_size=batch_size)

        save_output_with_pages(page_texts, empty_pages, txt_path)
        clean_output_file(txt_path)
        analyze_output(txt_path, total_pages)

        return {"status": "success", "txt_file": os.path.basename(txt_path)}

    except Exception as e:
        error_detail = traceback.format_exc()  # full traceback, never empty
        raise self.retry(
            exc=RuntimeError(error_detail),
            countdown=5
        )