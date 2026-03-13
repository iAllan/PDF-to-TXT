from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from fastapi.responses import FileResponse
import os
import shutil
from stripped_exctractor import process_pdf, save_output_with_pages, process_pdf_page_range
from verification import clean_output_file, analyze_output

app = FastAPI()
UPLOAD_DIR = "data/uploads"
OUTPUT_DIR = "data/outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

@app.post("/extract")
def extract_pdf(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    # Save uploaded PDF
    if not file.filename:
        raise ValueError("Uploaded file must have a filename.")
    pdf_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(pdf_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    # Output TXT path
    txt_filename = file.filename.replace(".pdf", ".txt")
    txt_path = os.path.join(OUTPUT_DIR, txt_filename)
    # Extraction and cleaning in background
    def process_and_clean():
        page_texts, empty_pages, total_pages = process_pdf(pdf_path)
        save_output_with_pages(page_texts, empty_pages, txt_path)
        clean_output_file(txt_path)
        analyze_output(txt_path, total_pages)
    background_tasks.add_task(process_and_clean)
    return {"message": "Processing started", "txt_file": txt_filename}

@app.post("/extract_batch")
def extract_batch(background_tasks: BackgroundTasks, files: list[UploadFile] = File(...)):
    responses = []
    for file in files:
        if not file.filename:
            continue
        pdf_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(pdf_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        txt_filename = file.filename.replace(".pdf", ".txt")
        txt_path = os.path.join(OUTPUT_DIR, txt_filename)
        def process_and_clean():
            page_texts, empty_pages, total_pages = process_pdf(pdf_path)
            save_output_with_pages(page_texts, empty_pages, txt_path)
            clean_output_file(txt_path)
            analyze_output(txt_path, total_pages)
        background_tasks.add_task(process_and_clean)
        responses.append({"filename": file.filename, "txt_file": txt_filename})
    return {"message": "Batch processing started", "files": responses}

# Extract page range from filename (e.g., "document_1-3.pdf" -> pages 1 to 3)
@app.post("/extract/pages/{page_range}")
def extract_pages(background_tasks: BackgroundTasks, page_range: str, file: UploadFile = File(...)):
    if not file.filename:
        raise ValueError("Uploaded file must have a filename.")
    pdf_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(pdf_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    txt_filename = file.filename.replace(".pdf", ".txt")
    txt_path = os.path.join(OUTPUT_DIR, txt_filename)
    def process_and_clean():
        page_texts, empty_pages, total_pages = process_pdf_page_range(pdf_path, start_page=int(page_range.split("-")[0]), end_page=int(page_range.split("-")[1]))
        save_output_with_pages(page_texts, empty_pages, txt_path)
        clean_output_file(txt_path)
        analyze_output(txt_path, total_pages)
    background_tasks.add_task(process_and_clean)
    return {"message": f"Processing started for pages {page_range}", "txt_file": txt_filename}

@app.get("/list")
def list_outputs():
    txt_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".txt")]
    return {"txt_files": txt_files}

@app.get("/status/{txt_filename}")
def check_status(txt_filename: str):
    txt_path = os.path.join(OUTPUT_DIR, txt_filename)
    if os.path.exists(txt_path):
        return {"status": "completed", "txt_file": txt_filename}
    else:
        return {"status": "processing", "txt_file": txt_filename}

@app.get("/download/{txt_filename}")
def download_txt(txt_filename: str):
    txt_path = os.path.join(OUTPUT_DIR, txt_filename)
    if not os.path.exists(txt_path):
        return {"error": "File not found or still processing."}
    return FileResponse(txt_path, media_type="text/plain", filename=txt_filename)
