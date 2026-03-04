import spacy
from spacy_layout import spaCyLayout
import PyPDF2
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

def process_pdf_in_batches(pdf_path, batch_size=50):
    """
    Process a large PDF in batches to manage memory usage.
    
    Args:
        pdf_path: Path to the PDF file
        batch_size: Number of pages to process at once (default 50)
    
    Returns:
        List of extracted text per page
    """
    start_time = time.time()
    logger.info(f"Starting PDF processing: {pdf_path}")
    logger.info(f"Batch size: {batch_size} pages")
    
    # Load spaCy model once (reuse across batches)
    logger.info("Loading spaCy model 'en_core_web_sm'...")
    nlp = spacy.load("en_core_web_sm")
    layout = spaCyLayout(nlp)
    logger.info("spaCy model loaded successfully")
    
    # Open the PDF and get total pages
    logger.info(f"Opening PDF file: {pdf_path}")
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        total_pages = len(pdf_reader.pages)
        logger.info(f"Total pages in PDF: {total_pages}")
        
        all_text = []
        
        # Process in batches
        num_batches = (total_pages + batch_size - 1) // batch_size
        logger.info(f"Processing will be done in {num_batches} batches")
        
        for batch_num, start_page in enumerate(range(0, total_pages, batch_size), 1):
            batch_start_time = time.time()
            end_page = min(start_page + batch_size, total_pages)
            pages_in_batch = end_page - start_page
            logger.info(f"Batch {batch_num}/{num_batches}: Processing pages {start_page + 1} to {end_page} ({pages_in_batch} pages)")
            
            # Create a temporary PDF with just this batch
            pdf_writer = PyPDF2.PdfWriter()
            for page_num in range(start_page, end_page):
                pdf_writer.add_page(pdf_reader.pages[page_num])
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                pdf_writer.write(tmp_file)
                tmp_path = tmp_file.name
            logger.debug(f"Temporary file created: {tmp_path}")
            
            try:
                # Process batch with spaCyLayout
                logger.info(f"Running spaCyLayout on batch {batch_num}...")
                doc = layout(tmp_path)
                logger.info(f"spaCyLayout completed for batch {batch_num}")
                
                # Extract text
                batch_text = []
                for page_idx, page_text in enumerate(doc.text, start=start_page+1):
                    batch_text.append(page_text)
                    logger.debug(f"Extracted text from page {page_idx} (length: {len(page_text)} chars)")
                
                all_text.extend(batch_text)
                logger.info(f"Batch {batch_num} text extraction complete. Total pages extracted so far: {len(all_text)}")
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {str(e)}", exc_info=True)
                raise
            finally:
                # Clean up temporary file
                os.unlink(tmp_path)
                logger.debug(f"Temporary file deleted: {tmp_path}")
            
            # Clear spaCy's memory after each batch
            del doc
            
            batch_elapsed = time.time() - batch_start_time
            logger.info(f"Batch {batch_num} completed in {timedelta(seconds=int(batch_elapsed))}")
            
            # Estimate remaining time
            batches_done = batch_num
            avg_time_per_batch = (time.time() - start_time) / batches_done
            remaining_batches = num_batches - batches_done
            eta = avg_time_per_batch * remaining_batches
            if remaining_batches > 0:
                logger.info(f"Estimated remaining time: {timedelta(seconds=int(eta))}")
    
    total_elapsed = time.time() - start_time
    logger.info(f"PDF processing completed in {timedelta(seconds=int(total_elapsed))}")
    logger.info(f"Total pages extracted: {len(all_text)}")
    
    return all_text

# Optional: generator version with logging
def process_pdf_generator(pdf_path, batch_size=50):
    """Generator version to yield pages one by one with logging."""
    logger.info(f"Starting generator processing: {pdf_path}")
    # ... similar setup but yield each page's text as it's extracted
    # ... (similar logging)

if __name__ == "__main__":
    pdf_path = FILEPATH
    extracted_text = process_pdf_in_batches(pdf_path, batch_size=50)
    
    # Save results
    output_path = "extracted_text.txt"
    logger.info(f"Saving extracted text to {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        for page_num, text in enumerate(extracted_text, 1):
            f.write(f"--- Page {page_num} ---\n")
            f.write(text)
            f.write("\n\n")
    logger.info(f"Text saved successfully. File size: {os.path.getsize(output_path)} bytes")