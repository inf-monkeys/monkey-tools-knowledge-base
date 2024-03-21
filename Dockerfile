FROM nvidia/cuda:12.0.0-cudnn8-runtime-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai
RUN apt-get update && \
    apt-get install git default-jre pandoc python3 python3-pip vim wget curl git git-lfs  -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
RUN pip3 install --upgrade pip && \
    pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt && rm -rf /root/.cache/pip

# Copy the rest of the files
COPY . .

# Expose port 8899
EXPOSE 8899

# Run the app
CMD [ "python3", "main.py" ]
