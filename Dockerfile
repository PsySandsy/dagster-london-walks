# Install Python
FROM python:3.12-slim

# Install UV
RUN pip install uv

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY . /app

# Create vitural environment and sync dependencies
RUN uv venv && uv sync
RUN uv pip list

# Expose Dagster dev UI port
EXPOSE 80

# Start Dagster dev server
# CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]