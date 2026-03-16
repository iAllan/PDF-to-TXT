# Use official Python image
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for opencv and others
RUN apt-get update && apt-get install -y libxcb1 libgl1 libglib2.0-0

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
