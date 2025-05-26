# # Use the official Python image
# FROM python:3.11-slim

# # Set working directory
# WORKDIR /app

# # Copy requirements and install
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy the source code
# COPY . .

# # Command to run the app
# CMD ["python", "main.py"]



# Example Dockerfile for a Python app
FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "main.py"]