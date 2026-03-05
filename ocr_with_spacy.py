import spacy
from spacy_layout import spaCyLayout
import PyPDF2  # or pypdf (pip install pypdf)
import tempfile
import os
import dotenv
import logging
import time
from datetime import timedelta

dotenv.load_dotenv()
FILEPATH = os.getenv('FILEPATH')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def _process_pdf_pages_internal(pdf_reader, start_page_0idx, end_page_0idx, nlp, layout, batch_size=50, overall_start_time=None):
    """
    Internal helper to process a contiguous range of pages.
    
    Args:
        pdf_reader: PyPDF2.PdfReader object
        start_page_0idx: First page index (0-based)
        end_page_0idx: Last page index (0-based, inclusive)
        nlp: loaded spaCy model
        layout: spaCyLayout instance
        batch_size: pages per batch
        overall_start_time: time.time() when overall processing started (for ETA)
    
    Returns:
        List of extracted text per page (in order)
    """
    total_pages_in_range = end_page_0idx - start_page_0idx + 1
    num_batches = (total_pages_in_range + batch_size - 1) // batch_size
    all_text = []
    
    # If overall_start_time not provided, use current time (no meaningful ETA)
    if overall_start_time is None:
        overall_start_time = time.time()

    for batch_num, batch_start in enumerate(range(0, total_pages_in_range, batch_size), 1):
        batch_start_time = time.time()
        batch_absolute_start = start_page_0idx + batch_start
        batch_absolute_end = min(start_page_0idx + batch_start + batch_size - 1, end_page_0idx)
        pages_in_batch = batch_absolute_end - batch_absolute_start + 1

        logger.info(f"Batch {batch_num}/{num_batches}: Processing pages {batch_absolute_start+1} to {batch_absolute_end+1} ({pages_in_batch} pages)")

        # Create temporary PDF with the pages in this batch
        pdf_writer = PyPDF2.PdfWriter()
        for page_num in range(batch_absolute_start, batch_absolute_end + 1):
            pdf_writer.add_page(pdf_reader.pages[page_num])

        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            pdf_writer.write(tmp_file)
            tmp_path = tmp_file.name
        logger.info(f"Temporary file created: {tmp_path}")

        try:
            logger.info(f"Running spaCyLayout on batch {batch_num}...")
            doc = layout(tmp_path)
            logger.info(f"spaCyLayout completed for batch {batch_num}")

            # Extract text from pages in this batch
            # Note: doc._.pages should correspond to the pages in the temporary PDF in order
            page_text = doc._.markdown
            all_text.append(page_text)
            logger.debug(f"Extracted text from batch {batch_num} (length: {len(page_text)} chars)")

        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {str(e)}", exc_info=True)
            raise
        finally:
            os.unlink(tmp_path)
            logger.info(f"Temporary file deleted: {tmp_path}")

        del doc  # free memory

        batch_elapsed = time.time() - batch_start_time
        logger.info(f"Batch {batch_num} completed in {timedelta(seconds=int(batch_elapsed))}")

        # Estimate remaining time
        elapsed_since_overall = time.time() - overall_start_time
        batches_done = batch_num
        if batches_done > 0:
            avg_time_per_batch = elapsed_since_overall / batches_done
            remaining_batches = num_batches - batches_done
            eta = avg_time_per_batch * remaining_batches
            if remaining_batches > 0:
                logger.info(f"Estimated remaining time: {timedelta(seconds=int(eta))}")

    return all_text

def process_pdf_full(pdf_path, batch_size=50):
    """
    Process an entire PDF in batches.
    
    Args:
        pdf_path: Path to the PDF file
        batch_size: Number of pages per batch
    
    Returns:
        List of extracted text per page (in order)
    """
    overall_start_time = time.time()
    logger.info(f"Starting full PDF processing: {pdf_path}")
    logger.info(f"Batch size: {batch_size} pages")

    # Load spaCy model once
    logger.info("Loading spaCy model 'en_core_web_sm'...")
    nlp = spacy.load("en_core_web_sm")
    layout = spaCyLayout(nlp)
    logger.info("spaCy model loaded successfully")

    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        total_pages = len(pdf_reader.pages)
        logger.info(f"Total pages in PDF: {total_pages}")

        all_text = _process_pdf_pages_internal(
            pdf_reader=pdf_reader,
            start_page_0idx=0,
            end_page_0idx=total_pages-1,
            nlp=nlp,
            layout=layout,
            batch_size=batch_size,
            overall_start_time=overall_start_time
        )

    total_elapsed = time.time() - overall_start_time
    logger.info(f"Full PDF processing completed in {timedelta(seconds=int(total_elapsed))}")
    logger.info(f"Total characters extracted: {len(all_text)}")
    return all_text

def process_pdf_page_range(pdf_path, start_page, end_page, batch_size=50):
    """
    Process a specific range of pages from a PDF.
    
    Args:
        pdf_path: Path to the PDF file
        start_page: First page to process (1-indexed, inclusive)
        end_page: Last page to process (1-indexed, inclusive)
        batch_size: Number of pages per batch
    
    Returns:
        List of extracted text per page (in order of the range)
    """
    overall_start_time = time.time()
    logger.info(f"Starting page range processing: {pdf_path}")
    logger.info(f"Requested range: pages {start_page} to {end_page} (inclusive)")
    logger.info(f"Batch size: {batch_size} pages")

    # Validate inputs
    if start_page < 1 or end_page < 1 or start_page > end_page:
        raise ValueError("Invalid page range: start_page and end_page must be >=1 and start_page <= end_page")

    # Load spaCy model once
    logger.info("Loading spaCy model 'en_core_web_sm'...")
    nlp = spacy.load("en_core_web_sm")
    layout = spaCyLayout(nlp)
    logger.info("spaCy model loaded successfully")

    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        total_pages = len(pdf_reader.pages)
        logger.info(f"Total pages in PDF: {total_pages}")

        # Convert to 0-based indices and clamp to available pages
        start_0idx = max(0, start_page - 1)
        end_0idx = min(total_pages - 1, end_page - 1)

        if start_0idx > end_0idx:
            raise ValueError(f"Requested range {start_page}-{end_page} is outside document (only {total_pages} pages)")

        actual_start = start_0idx + 1
        actual_end = end_0idx + 1
        if actual_start != start_page or actual_end != end_page:
            logger.warning(f"Page range adjusted to {actual_start}-{actual_end} (document has {total_pages} pages)")

        all_text = _process_pdf_pages_internal(
            pdf_reader=pdf_reader,
            start_page_0idx=start_0idx,
            end_page_0idx=end_0idx,
            nlp=nlp,
            layout=layout,
            batch_size=batch_size,
            overall_start_time=overall_start_time
        )

    total_elapsed = time.time() - overall_start_time
    logger.info(f"Page range processing completed in {timedelta(seconds=int(total_elapsed))}")
    logger.info(f"Pages extracted in range: {len(all_text)}")
    return all_text

# Example usage
if __name__ == "__main__":
    pdf_path = FILEPATH

    # Option 1: Process entire document
    all_text = process_pdf_full(pdf_path, batch_size=25)
    
    # Option 2: Process only pages within a specific range (e.g., pages 50-60)
    # range_text = process_pdf_page_range(pdf_path, start_page=50, end_page=60, batch_size=20)

    # Save the extracted range
    output_path = "extracted_range.txt"
    logger.info(f"Saving extracted range to {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        for relative_idx, text in enumerate(all_text, start=1):
            f.write(f"--- Page {relative_idx} ---\n")
            f.write(text)
            f.write("\n\n")
    logger.info(f"Range saved successfully. File size: {os.path.getsize(output_path)} bytes")