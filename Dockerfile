# Base image with Python
FROM python:3.9-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /mlops_project

# Install system dependencies
RUN apt-get update && apt-get install -y

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the entire project into the working directory
COPY . .

# Expose port (if the application requires a port to be exposed)
EXPOSE 8000

# Command to run the application
CMD ["python", "app.py"]
