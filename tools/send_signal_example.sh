#!/usr/bin/env bash
# lake -> lake-executor 웹훅 발송 예시 (상대 측 참고용).
#
# 사용:
#   LAKE_SIGNAL_SECRET='<shared secret>' tools/send_signal_example.sh signal.json [https://host/lake/signal]
#
# 규격 (docs/ARCHITECTURE.md §2):
#   - X-Signature = hex(HMAC-SHA256(secret, raw_body)) 소문자 64자 — "전송하는 바이트 그대로" 서명한다.
#   - X-Timestamp = 본문 ts (Unix ms) 와 정확히 일치, 수신 시각 ±60s.
#   - Content-Type: application/json, Content-Encoding 없음, 본문 ≤ 65536 바이트.
#   - 2xx 는 접수 확인일 뿐이며 실행 결과는 execution report 회신으로 온다.
#
# 주의: openssl 의 키 인자(hexkey)는 같은 호스트의 다른 사용자가 `ps` 로 볼 수 있다.
#       이 스크립트는 예시용이며, 공용 호스트에서는 HMAC 를 언어 라이브러리(hmac 모듈 등)로 계산할 것.
set -euo pipefail

FILE="${1:-}"
URL="${2:-http://127.0.0.1:8787/lake/signal}"
SECRET="${LAKE_SIGNAL_SECRET:-}"

if [[ -z "$FILE" || ! -f "$FILE" ]]; then
  echo "usage: LAKE_SIGNAL_SECRET=... $0 signal.json [url]" >&2
  exit 2
fi
if [[ -z "$SECRET" ]]; then
  echo "LAKE_SIGNAL_SECRET env var is required (never put it in the file or on the command line)" >&2
  exit 2
fi

# 현재 Unix ms (GNU date; macOS 등은 python 폴백)
if TS="$(date +%s%3N 2>/dev/null)" && [[ "$TS" =~ ^[0-9]{13}$ ]]; then :; else
  TS="$(python3 -c 'import time;print(int(time.time()*1000))')"
fi

# 본문에 ts / expires_at_ms(+15s) 를 넣는다. jq 가 있으면 그걸로, 없으면 파일의 ts 를 그대로 쓴다.
BODY_FILE="$(mktemp)"
trap 'rm -f "$BODY_FILE"' EXIT
if command -v jq >/dev/null 2>&1; then
  jq -c --argjson ts "$TS" '.ts = $ts | .expires_at_ms = ($ts + 15000)' "$FILE" > "$BODY_FILE"
else
  cp "$FILE" "$BODY_FILE"
  TS="$(grep -oE '"ts"[[:space:]]*:[[:space:]]*[0-9]+' "$FILE" | grep -oE '[0-9]+$' | head -n1 || true)"
  if [[ -z "$TS" ]]; then
    echo "jq not found and no numeric \"ts\" in $FILE" >&2
    exit 2
  fi
  echo "note: jq not found; using ts=$TS from the file as-is" >&2
fi

# 서명은 "전송할 바이트" 그대로 계산한다 (재직렬화 금지 → 같은 파일을 --data-binary 로 보낸다).
HEXKEY="$(printf '%s' "$SECRET" | od -An -v -tx1 | tr -d ' \n')"
SIG="$(openssl dgst -sha256 -mac HMAC -macopt "hexkey:$HEXKEY" -r < "$BODY_FILE" | cut -d' ' -f1 | tr 'A-F' 'a-f')"

echo "POST $URL  (ts=$TS, $(wc -c < "$BODY_FILE") bytes)" >&2
curl -sS -i -X POST "$URL" \
  -H "Content-Type: application/json" \
  -H "X-Timestamp: $TS" \
  -H "X-Signature: $SIG" \
  --data-binary @"$BODY_FILE"
echo
