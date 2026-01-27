FROM python:3.11-slim

WORKDIR /app

# Copy requirements if you have them, otherwise install manually
# (Recommended: run 'pip freeze > requirements.txt' in your .venv first)
COPY server.py .

# Install necessary libraries
RUN pip install --no-cache-dir fastapi uvicorn requests mcp

EXPOSE 8000

CMD ["python", "server.py"]