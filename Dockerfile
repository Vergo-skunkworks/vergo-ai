FROM python:3.13

# Set environment variables to prevent Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED TRUE

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies
# --no-cache-dir reduces image size
# --system ensures Gunicorn finds system-installed packages if any are needed by underlying libraries
RUN pip install --no-cache-dir -r requirements.txt

# Copy your entire project structure into the container at /app
# This will include mainmain.py, Procfile, and your subfolders like 'api', 'data_analysis', etc.
COPY . .

# Cloud Run automatically sets the PORT environment variable.
# Gunicorn will bind to this port.
# Ensure your Flask app instance in "mainmain.py" is named "app"
# Or change "app:app" to "your_main_file_name_without_py:your_flask_app_variable_name"
CMD ["/bin/sh", "-c", "gunicorn --bind 0.0.0.0:$PORT --timeout 120 main:app"]