FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /workspace

# Copy requirements and install dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install QuixStreams in development mode
RUN pip install -e /workspace[all,crypto]

# Copy test utilities
COPY test-utils/ /test-utils/
ENV PYTHONPATH="/test-utils:${PYTHONPATH}"

# Create results directory
RUN mkdir -p /test-results

# Set default command
CMD ["python", "-m", "pytest", "-v", "--tb=short"]