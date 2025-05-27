# FROM python:3.11

# WORKDIR /app

# COPY requirements.txt .
# RUN pip install -r requirements.txt

# COPY . .

# ENV FUNCTION_TARGET=read_streams_config
# ENV FUNCTION_SIGNATURE_TYPE=http

# RUN pip install functions-framework

# EXPOSE 8080

# CMD ["functions-framework", "--target=read_streams_config", "--port=8080"]


FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install Functions Framework
RUN pip install functions-framework

COPY . .

ENV FUNCTION_TARGET=read_streams_config
ENV PORT=8080

EXPOSE 8080

CMD ["functions-framework", "--target=read_streams_config", "--port=8080"]







# FROM python:3.10-slim

# # Install required packages
# WORKDIR /app
# COPY requirements.txt ./
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy source code
# COPY . .

# # Run your main script
# CMD ["python", "main.py"]
