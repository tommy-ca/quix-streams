FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .
COPY static/ ./static/
COPY templates/ ./templates/

# Expose ports
EXPOSE 8888 8889

# Run the mock exchange
CMD ["python", "app.py"]