# # Use the official Python image.
# FROM python:3.11

# # Set working directory
# WORKDIR /app

# # Copy requirements and install
# COPY requirements.txt .
# RUN pip install -r requirements.txt

# # Copy your code
# COPY . .

# # Set environment variables for Cloud Functions Framework
# ENV FUNCTION_TARGET=read_streams_config
# ENV FUNCTION_SIGNATURE_TYPE=http

# # Run using Functions Framework
# RUN pip install functions-framework

# # Expose port for Cloud Run
# EXPOSE 8080

# CMD ["functions-framework", "--target=read_streams_config", "--port=8080"]



FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV FUNCTION_TARGET=read_streams_config
ENV FUNCTION_SIGNATURE_TYPE=http

RUN pip install functions-framework

EXPOSE 8080

CMD ["functions-framework", "--target=read_streams_config", "--port=8080"]

