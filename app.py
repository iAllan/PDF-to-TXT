from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import os
import shutil
from celery.result import AsyncResult
from celery_app import celery_app
import uuid

app = FastAPI()
UPLOAD_DIR = "data/uploads"
OUTPUT_DIR = "data/outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------
# API Endpoints
# -------------------------
@app.post("/extract")
async def extract_pdf(file: UploadFile = File(...)):
    if not file.filename:
        raise ValueError("Uploaded file must have a filename.")

    # Save file locally
    pdf_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(pdf_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # Enqueue Celery task
    task = celery_app.send_task("tasks.process_pdf_task", args=[file.filename])

    return {
        "message": "Processing started",
        "task_id": task.id,
        "txt_file": file.filename.replace(".pdf", ".txt")
    }

@app.post("/extract_batch")
async def extract_batch(files: list[UploadFile] = File(...)):
    tasks = []
    for file in files:
        if not file.filename:
            continue
        pdf_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(pdf_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        task = celery_app.send_task("tasks.process_pdf_task", args=[file.filename])
        tasks.append({
            "filename": file.filename,
            "task_id": task.id,
            "txt_file": file.filename.replace(".pdf", ".txt")
        })

    return {"message": "Batch processing started", "files": tasks}

@app.post("/extract/pages/{page_range}")
async def extract_pages(page_range: str, file: UploadFile = File(...)):
    if not file.filename:
        raise ValueError("Uploaded file must have a filename.")

    pdf_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(pdf_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    task = celery_app.send_task("tasks.process_pdf_task", args=[file.filename, page_range])

    return {
        "message": f"Processing started for pages {page_range}",
        "task_id": task.id,
        "txt_file": file.filename.replace(".pdf", ".txt")
    }

@app.post("/extract/{file_name}")
async def extract_specific(file_name: str):
    pdf_path = os.path.join(UPLOAD_DIR, file_name)
    if not os.path.exists(pdf_path):
        return {"error": "File not found in upload directory."}

    task = celery_app.send_task("tasks.process_pdf_task", args=[file_name])
    return {
        "message": f"Processing started for {file_name}",
        "task_id": task.id,
        "txt_file": file_name.replace(".pdf", ".txt")
    }

# New endpoint to check task status
@app.get("/task/{task_id}")
def get_task_status(task_id: str):
    task_result = AsyncResult(task_id, app=celery_app)
    if task_result.state == "PENDING":
        response = {"state": task_result.state, "status": "Task is waiting or not started"}
    elif task_result.state == "FAILURE":
        response = {"state": task_result.state, "status": str(task_result.info)}
    else:
        response = {"state": task_result.state, "result": task_result.result}
    return response

# -------------------------
# Utility Endpoints
# -------------------------

@app.get("/")
def root():
    return {"message": "Welcome to the PDF to TXT extraction API. Use /extract to upload PDFs."}

@app.get("/list")
def list_outputs():
    txt_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".txt")]
    return {"txt_files": txt_files}

@app.get("/list_pdfs")
def list_pdfs():
    pdf_files = [f for f in os.listdir(UPLOAD_DIR) if f.endswith(".pdf")]
    return {"pdf_files": pdf_files}

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
