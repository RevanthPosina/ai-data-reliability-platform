ARG PYTHON_IMAGE
FROM ${PYTHON_IMAGE}
ARG TORCH_VERSION
ARG EMBED_MODEL
ARG EMBED_MODEL_REVISION
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HOME=/opt/hf HF_HUB_DISABLE_TELEMETRY=1
RUN useradd -m -u 10001 app && mkdir -p /state /opt/dagster_home /opt/hf && chown app /state /opt/dagster_home /opt/hf
# CPU-only torch wheel index: no CUDA payload.
RUN pip install torch==${TORCH_VERSION} --index-url https://download.pytorch.org/whl/cpu
# constraints.txt holds every transitive package to the >=14-day policy (scripts/pin_policy.py).
COPY docker/app/requirements.txt docker/app/constraints.txt /tmp/
RUN pip install -r /tmp/requirements.txt -c /tmp/constraints.txt
USER app
# Bake only the PyTorch weights + tokenizer/config of the pinned revision (not ONNX/OpenVINO/TF).
RUN python -c "from huggingface_hub import snapshot_download as d; \
print(d('${EMBED_MODEL}', revision='${EMBED_MODEL_REVISION}', \
allow_patterns=['*.json', '*.txt', 'model.safetensors', '1_Pooling/*', 'modules.json']))"
ENV HF_HUB_OFFLINE=1 EMBED_MODEL=${EMBED_MODEL} EMBED_MODEL_REVISION=${EMBED_MODEL_REVISION}
COPY --chown=app src /opt/app
ENV PYTHONPATH=/opt/app
WORKDIR /opt/app
