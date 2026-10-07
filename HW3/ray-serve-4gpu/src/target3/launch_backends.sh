#!/usr/bin/env bash
# Start four SGLang backends, one per GPU, ports 31000-31003.
# Radix cache stays ON (default). Logs go to logs/backend-<i>.log.
# 如需在 4 卡上复跑并记录显存比例，请在此显式传参。
set -euo pipefail
MODEL="${MODEL:-/root/autodl-tmp/models/Qwen3-0.6B}"
LOGDIR="${LOGDIR:-logs}"
SGL_PY="${SGL_PY:-/root/autodl-tmp/envs/sgl/bin/python}"
STARTED_PIDS=()

fail_startup() {
  local backend="$1"
  echo "ERROR: backend $backend did not become ready; check $LOGDIR/backend-${backend}.log" >&2
  if (( ${#STARTED_PIDS[@]} > 0 )); then
    for pid in "${STARTED_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  fi
  exit 1
}

# SGLang compiles CUDA kernels at startup; ninja lives next to the interpreter.
export PATH="$(dirname "$SGL_PY"):$PATH"
mkdir -p "$LOGDIR"
for i in 0 1 2 3; do
  port=$((31000 + i))
  if curl -s "http://127.0.0.1:${port}/get_server_info" >/dev/null 2>&1; then
    echo "backend $i already running on port ${port}"
    continue
  fi
  CUDA_VISIBLE_DEVICES=$i nohup "$SGL_PY" -m sglang.launch_server \
    --model "$MODEL" \
    --host 127.0.0.1 \
    --port "$port" \
    > "$LOGDIR/backend-${i}.log" 2>&1 &
  pid=$!
  STARTED_PIDS+=("$pid")
  echo "started backend $i on port ${port} (pid $pid)"
done
echo "waiting for /v1/models on all four ports ..."
BACKEND_READY_TIMEOUT_SECONDS="${BACKEND_READY_TIMEOUT_SECONDS:-1200}"
for i in 0 1 2 3; do
  port=$((31000 + i))
  ready=0
  ready_deadline=$((SECONDS + BACKEND_READY_TIMEOUT_SECONDS))
  while (( SECONDS < ready_deadline )); do
    if curl --fail --silent --max-time 5 "http://127.0.0.1:${port}/v1/models" | grep -q '"data"'; then
      ready=1
      echo "backend $i ready"
      break
    fi
    sleep 5
  done
  if [[ "$ready" -ne 1 ]]; then
    fail_startup "$i"
  fi
done
