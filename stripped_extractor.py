import fitz  # pymupdf
import pytesseract
from PIL import Image
from pypdf import PdfReader, PdfWriter
import io, os, logging, time
from datetime import timedelta
from typing import List, Tuple, Optional
import dotenv

dotenv.load_dotenv()
FILEPATH = os.getenv("FILEPATH") or ""

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

EMPTY_PAGE_THRESHOLD = 100
OCR_DPI = 300  # higher = more accurate but slower


OCR_DPI = 200  # was 300 — saves ~44% memory, still accurate enough

def _extract_page_text(page: fitz.Page) -> str:
    text = page.get_text("text").strip()

    if len(text) >= EMPTY_PAGE_THRESHOLD:
        return text

    logger.info(f"Page {page.number + 1} appears scanned, running OCR...")
    mat = fitz.Matrix(OCR_DPI / 72, OCR_DPI / 72)
    pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)

    try:
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        ocr_text = pytesseract.image_to_string(img, lang="eng")
        return ocr_text.strip()
    finally:
        pix = None
        img.close() if 'img' in dir() else None


def _process_pdf_pages(
    pdf_path: str,
    start_page: int,
    end_page: int,
    batch_size: int = 20,
    layout=None,              # kept for API compatibility, unused
    overall_start_time: Optional[float] = None
) -> Tuple[List[str], List[int], int]:

    if overall_start_time is None:
        overall_start_time = time.time()

    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    start_idx = max(0, start_page - 1)
    end_idx = min(total_pages - 1, end_page - 1)
    if start_idx > end_idx:
        raise ValueError(f"Invalid range: {start_page}-{end_page} (doc has {total_pages} pages)")

    actual_start = start_idx + 1
    actual_end = end_idx + 1
    total_in_range = actual_end - actual_start + 1
    logger.info(f"Processing pages {actual_start}–{actual_end} ({total_in_range} pages)")

    all_page_texts: List[str] = []
    empty_pages: List[int] = []

    batches = [
        (i, min(i + batch_size - 1, actual_end - 1))
        for i in range(actual_start - 1, actual_end, batch_size)
    ]

    for batch_num, (start_0, end_0) in enumerate(batches, 1):
        batch_start = time.time()
        pages_in_batch = end_0 - start_0 + 1
        logger.info(f"Batch {batch_num}/{len(batches)} | Pages {start_0+1}–{end_0+1}")

        # Build batch PDF in memory
        writer = PdfWriter()
        for p in range(start_0, end_0 + 1):
            writer.add_page(reader.pages[p])
        pdf_buffer = io.BytesIO()
        writer.write(pdf_buffer)
        pdf_buffer.seek(0)

        doc = fitz.open(stream=pdf_buffer.read(), filetype="pdf")
        for offset, page in enumerate(doc):
            page_num = start_0 + offset + 1  # 1-indexed
            page_text = _extract_page_text(page)
            all_page_texts.append(page_text)

            if len(page_text) < EMPTY_PAGE_THRESHOLD:
                logger.warning(f"Page {page_num} still empty after OCR ({len(page_text)} chars)")
                empty_pages.append(page_num)

        doc.close()

        elapsed = time.time() - batch_start
        pps = pages_in_batch / elapsed if elapsed > 0 else 0
        logger.info(f"Batch done in {timedelta(seconds=int(elapsed))} ({pps:.2f} pages/sec)")

        elapsed_total = time.time() - overall_start_time
        remaining = (elapsed_total / batch_num) * (len(batches) - batch_num)
        if remaining > 0:
            logger.info(f"ETA: {timedelta(seconds=int(remaining))}")

    return all_page_texts, empty_pages, total_pages


def process_pdf(pdf_path: str, batch_size: int = 20) -> Tuple[List[str], List[int], int]:
    start_time = time.time()
    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)
    page_texts, empty_pages, total_pages = _process_pdf_pages(
        pdf_path, 1, total_pages, batch_size, overall_start_time=start_time
    )
    logger.info(f"Completed in {timedelta(seconds=int(time.time() - start_time))}")
    return page_texts, empty_pages, total_pages


def process_pdf_page_range(
    pdf_path: str, start_page: int, end_page: int, batch_size: int = 20
) -> Tuple[List[str], List[int], int]:
    start_time = time.time()
    page_texts, empty_pages, total_pages = _process_pdf_pages(
        pdf_path, start_page, end_page, batch_size, overall_start_time=start_time
    )
    logger.info(f"Range done in {timedelta(seconds=int(time.time() - start_time))}")
    return page_texts, empty_pages, total_pages


def save_output_with_pages(
    page_texts: List[str], empty_pages: List[int],
    output_file: str = "extracted_text.txt", start_page_num: int = 1
):
    logger.info(f"Saving to {output_file}")
    with open(output_file, "w", encoding="utf-8") as f:
        for i, text in enumerate(page_texts, start=start_page_num):
            f.write(f"\n--- Page {i} ---\n\n{text}\n")

    if empty_pages:
        empty_file = output_file.replace(".txt", "_empty_pages.txt")
        with open(empty_file, "w") as f:
            f.write("Pages with very short text:\n")
            for p in empty_pages:
                f.write(f"{p}\n")
        logger.info(f"Empty pages saved to {empty_file}")

# -------------------------
# Run (example usage)
# -------------------------
if __name__ == "__main__":
    # Example: Process only select pages
    page_texts, empty, total_pages = process_pdf_page_range(FILEPATH, 1, 20, batch_size=10)
    # page_texts, empty = process_pdf(FILEPATH, batch_size=20)
    save_output_with_pages(page_texts, empty, "module_1_extracted_range_1-20.txt", start_page_num=1)

