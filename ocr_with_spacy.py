import spacy
from spacy_layout import spaCyLayout
from pypdf import PdfReader, PdfWriter
import io
import os
import dotenv
import logging
import time
from datetime import timedelta
from typing import List, Tuple, Optional

dotenv.load_dotenv()
FILEPATH = os.getenv("FILEPATH") or ""

# -------------------------
# Logging Setup
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

EMPTY_PAGE_THRESHOLD = 100  # characters


# -------------------------
# Load spaCy + Layout once
# -------------------------
def load_layout_model():
    logger.info("Loading spaCy layout model...")
    nlp = spacy.blank("en")
    layout = spaCyLayout(nlp)
    logger.info("Layout model ready")
    return layout


# -------------------------
# Core processing function (used by both full and range)
# -------------------------
def _process_pdf_pages(
    pdf_path: str,
    start_page: int,          # 1-indexed, inclusive
    end_page: int,            # 1-indexed, inclusive
    batch_size: int = 20,
    layout=None,              # optional pre-loaded layout
    overall_start_time: Optional[float] = None  # for ETA
) -> Tuple[List[str], List[int], int]:
    """
    Process a range of pages and return:
        - list of page texts in order (one string per page)
        - list of page numbers that appear empty (below threshold)
    """
    if layout is None:
        layout = load_layout_model()
    if overall_start_time is None:
        overall_start_time = time.time()

    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    # Clamp range to document bounds
    start_idx = max(0, start_page - 1)
    end_idx = min(total_pages - 1, end_page - 1)
    if start_idx > end_idx:
        raise ValueError(f"Invalid page range: {start_page}-{end_page} (document has {total_pages} pages)")

    actual_start = start_idx + 1
    actual_end = end_idx + 1
    if actual_start != start_page or actual_end != end_page:
        logger.warning(f"Adjusted range to pages {actual_start}-{actual_end}")

    total_pages_in_range = actual_end - actual_start + 1
    logger.info(f"Processing pages {actual_start} to {actual_end} ({total_pages_in_range} pages)")

    all_page_texts = []
    empty_pages = []

    # Create batches within the range (0-based indices)
    batches = [
        (i, min(i + batch_size - 1, actual_end - 1))
        for i in range(actual_start - 1, actual_end, batch_size)
    ]
    num_batches = len(batches)

    for batch_num, (start_0, end_0) in enumerate(batches, 1):
        batch_start_time = time.time()
        pages_in_batch = end_0 - start_0 + 1
        logger.info(f"Batch {batch_num}/{num_batches} | Pages {start_0+1}-{end_0+1}")

        # Build batch PDF in memory
        writer = PdfWriter()
        for p in range(start_0, end_0 + 1):
            writer.add_page(reader.pages[p])
        pdf_buffer = io.BytesIO()
        writer.write(pdf_buffer)
        pdf_buffer.seek(0)

        # Run layout extraction
        doc = layout(pdf_buffer.getvalue())

        if not hasattr(doc._, 'pages'):
            logger.error("doc._.pages not found – check spacy-layout installation")
            raise AttributeError("Missing pages attribute")

        # Extract each page's text
        for offset, (page_layout, spans) in enumerate(doc._.pages):
            page_num = start_0 + offset + 1   # 1-indexed
            # page_text = ''.join(span.text for span in spans).strip()
            page_text = doc._.markdown  
            all_page_texts.append(page_text)

            if len(page_text) < EMPTY_PAGE_THRESHOLD:
                logger.warning(f"Page {page_num} may be empty (length {len(page_text)} chars)")
                empty_pages.append(page_num)

        del doc

        # Performance metrics and ETA
        elapsed = time.time() - batch_start_time
        pps = pages_in_batch / elapsed if elapsed > 0 else 0
        logger.info(f"Batch finished in {timedelta(seconds=int(elapsed))} ({pps:.2f} pages/sec)")

        batches_done = batch_num
        elapsed_since_start = time.time() - overall_start_time
        avg_time_per_batch = elapsed_since_start / batches_done
        remaining_batches = num_batches - batches_done
        if remaining_batches > 0:
            eta = avg_time_per_batch * remaining_batches
            logger.info(f"Estimated remaining time: {timedelta(seconds=int(eta))}")

    return all_page_texts, empty_pages, total_pages


# -------------------------
# Public function: process entire PDF
# -------------------------
def process_pdf(pdf_path: str, batch_size: int = 20) -> Tuple[List[str], List[int], int]:
    """
    Process the whole PDF, return (page_texts, empty_pages, total_pages).
    """
    start_time = time.time()
    layout = load_layout_model()
    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    page_texts, empty_pages, total_pages = _process_pdf_pages(
        pdf_path,
        start_page=1,
        end_page=total_pages,
        batch_size=batch_size,
        layout=layout,
        overall_start_time=start_time
    )

    total_elapsed = time.time() - start_time
    logger.info(f"Completed in {timedelta(seconds=int(total_elapsed))}")
    logger.info(f"Total pages extracted: {len(page_texts)}")
    if empty_pages:
        logger.warning(f"Empty/short pages detected: {empty_pages}")
    else:
        logger.info("No empty pages detected.")
    return page_texts, empty_pages, total_pages


# -------------------------
# Public function: process a specific page range
# -------------------------
def process_pdf_page_range(
    pdf_path: str,
    start_page: int,
    end_page: int,
    batch_size: int = 20
) -> Tuple[List[str], List[int], int]:
    """
    Process a range of pages (1-indexed, inclusive).
    Returns (page_texts, empty_pages, total_pages).
    """
    start_time = time.time()
    layout = load_layout_model()

    page_texts, empty_pages, total_pages = _process_pdf_pages(
        pdf_path,
        start_page=start_page,
        end_page=end_page,
        batch_size=batch_size,
        layout=layout,
        overall_start_time=start_time
    )

    total_elapsed = time.time() - start_time
    logger.info(f"Range processing completed in {timedelta(seconds=int(total_elapsed))}")
    if empty_pages:
        logger.warning(f"Empty/short pages in range: {empty_pages}")
    else:
        logger.info("No empty pages detected in range.")
    return page_texts, empty_pages, total_pages


# -------------------------
# Save Output with Page Markers
# -------------------------
def save_output_with_pages(
    page_texts: List[str],
    empty_pages: List[int],
    output_file: str = "extracted_text.txt",
    start_page_num: int = 1
):
    """
    Save extracted page texts, marking each page.
    Also write a summary of empty pages to a separate file.
    """
    logger.info(f"Saving output to {output_file}")
    with open(output_file, "w", encoding="utf-8") as f:
        for i, text in enumerate(page_texts, start=start_page_num):
            f.write(f"\n--- Page {i} ---\n\n")
            f.write(text)
            f.write("\n")
    logger.info(f"Saved successfully ({os.path.getsize(output_file)} bytes)")

    if empty_pages:
        empty_file = output_file.replace(".txt", "_empty_pages.txt")
        with open(empty_file, "w") as f:
            f.write("Pages with very short text:\n")
            for p in empty_pages:
                f.write(f"{p}\n")
        logger.info(f"Empty pages list saved to {empty_file}")


# -------------------------
# Run (example usage)
# -------------------------
if __name__ == "__main__":
    # Example: Process only select pages
    page_texts, empty, total_pages = process_pdf_page_range(FILEPATH, 1, 20, batch_size=10)
    # page_texts, empty = process_pdf(FILEPATH, batch_size=20)
    save_output_with_pages(page_texts, empty, "module_1_extracted_range_1-20.txt", start_page_num=1)

