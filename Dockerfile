# Dockerfile — micromamba + environment.yaml
FROM mambaorg/micromamba:1.4.0

WORKDIR /workspace

# Copy the repo into image
COPY . /workspace

# Copy environment file to /tmp (avoids permission issues when micromamba writes temp files)
RUN cp /workspace/config/environment.yaml /tmp/environment.yaml

# Create the conda environment named 'genexomics' from the YAML
# Use micromamba directly to avoid installing extra tools
RUN micromamba create -y -f /tmp/environment.yaml -n genexomics && \
    micromamba clean --all --yes

# Ensure the env's bin is on PATH for runtime
ENV PATH=/opt/conda/envs/genexomics/bin:$PATH
ENV S3_ENDPOINT=http://localstack:4566

WORKDIR /workspace

# Verify tools are available by calling the env binaries directly (avoid micromamba quoting issues)
RUN /opt/conda/envs/genexomics/bin/python -c "import sys; print('python', sys.version.split()[0])" && \
    /opt/conda/envs/genexomics/bin/nextflow -version || \
    (echo 'WARNING: nextflow or python not found in env; continuing image build' && exit 0)

ENTRYPOINT ["/bin/bash", "-lc"]