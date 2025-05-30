# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Set work directory
WORKDIR /app


COPY requirements.txt ./
COPY . ./src/



RUN pip install --no-cache-dir -r requirements.txt


# Expose port from environment variable (default 8000)
ARG PORT=8000
ENV PORT=${PORT}
EXPOSE ${PORT}