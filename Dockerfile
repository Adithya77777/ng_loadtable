# Example Dockerfile for Python
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

# Ensure this line exposes the correct port
EXPOSE 8080

# Start your app
CMD ["python", "app.py"]
