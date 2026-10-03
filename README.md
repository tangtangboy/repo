# lake-executor — lake 웹훅 시그널 → Bybit 자동 실행기

lake(전략 측)가 **HTTPS POST 웹훅**으로 보내는 매매 신호(entry / add / partial_exit / full_exit / protection_update)를
받아 **서명·시각·만료·중복·순서 검증 → 영속 접수** 한 뒤, 단일 워커가 **Bybit USDT 무기한 BTCUSDT(헤지 모드)** 에
시장가 주문과 보호주문(SL/TP, 조건부 reduceOnly 시장가)을 넣고, **체결 회신(execution)** 과 **30초 전체 포지션
스냅샷(snapshot)** 을 lake 회신 URL로 HMAC 서명해 돌려보낸다.

고정 계약은 `docs/ARCHITECTURE.md`, 상대 측 규격은 `docs/lake_handoff/` 참고. 운영 절차는 `docs/RUNBOOK.md`.

## 흐름

```
lake ──POST /lake/signal (X-Signature / X-Timestamp)──▶ receiver.py ─persist─▶ store.signals(accepted)
       ◀── 202 accepted | 200 duplicate | 409/410/400/401/413/415/503                   │
                                                                                         │ claim (FIFO)
                                          executor.py (단일 워커) ◀───────────────────────┘
                                             │  게이트: mode / live.enabled / HALT / RECONCILE_REQUIRED / 수량·슬리피지 가드
                                             │  exchange.py  (BybitExchange | PaperExchange)
                                             │  lots 원장 갱신, fills 기록, 보호주문 생성·재설정·취소
                                             ▼
                                          reporter.py ─allocate(seq)─▶ store.reports(pending) ─deliver─▶ lake 회신 URL
                                          snapshot loop(30s): executor.reconcile → reporter.snapshot
```

- 2xx 는 **접수 확인**일 뿐이다. 실행 결과(submitted / filled / rejected …)는 회신(execution report)으로만 전달된다.
- 과거·재전송 신호는 새 매매로 처리하지 않는다 (`event_id` 영속, `position_id` 별 `event_sequence` 검증).
- `full_exit` 은 **그 lot 잔량만** 청산한다. 심볼 전량 청산으로 확대하지 않는다.

## 모듈

| 파일 | 역할 |
|---|---|
| `lake_executor/receiver.py` | FastAPI 수신기: `POST /lake/signal`, `GET /healthz`, 관리 엔드포인트(`/state`, `/admin/*`) |
| `lake_executor/auth.py` | HMAC-SHA256 서명 검증/생성 (원본 바이트 그대로, 재직렬화 금지) |
| `lake_executor/schemas.py` | 신호 스키마(pydantic, 미정의 필드 거부), 상태·reason_code 열거형 |
| `lake_executor/store.py` | SQLite 원장: signals / lots / orders / fills / reports / ingress_log / meta |
| `lake_executor/executor.py` | 단일 워커: 게이트 → 액션 처리 → 체결 수집 → 보호주문 → 대사(reconcile) |
| `lake_executor/exchange.py` | `BybitExchange`(pybit v5) / `PaperExchange`(메모리 시뮬레이터) 공통 인터페이스 |
| `lake_executor/reporter.py` | execution / snapshot 회신 생성(계약 스키마), sequence 직렬 전송, 재시도 |
| `lake_executor/ops.py` | 로깅(시크릿 마스킹), 알림(텔레그램), HALT 파일 |
| `lake_executor/config.py` | `config.json`(비밀 아님) + `.env`(비밀) 로더/검증 |
| `lake_executor/main.py` | CLI: `serve` / `check` / `simulate` / `sign` |
| `deploy/` | AWS EC2 + Caddy(HTTPS) + systemd 배포 스크립트 (`deploy/README.md`) |
| `tools/send_signal_example.sh` | 상대 측 참고용 서명·전송 예시 |

## 빠른 시작 (로컬)

```bash
python3 -m venv .venv && . .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt

cp config.example.json config.json                   # 비밀 아님. live.enabled=false 로 시작
cp .env.example .env                                 # 비밀. 시크릿은 각 32바이트 이상
# .env 에 최소 LAKE_SIGNAL_SECRET_TEST 를 채운다 (Bybit 키 없이도 TEST 는 동작)

python -m lake_executor check                        # 설정·키 유무·Bybit 읽기 전용 연결 확인 (주문 없음)
python -m lake_executor serve                        # 127.0.0.1:8787 수신 + 실행기 + 회신기
python -m lake_executor simulate                     # 다른 터미널: TEST 신호 entry→add→partial_exit→protection_update→full_exit
python -m lake_executor sign --file signal.json --mode test   # 임의 신호 파일에 서명 헤더 + curl 예시
python -m pytest -q                                  # 네트워크 없이 전부 통과해야 함
```

`config.json` 의 `"test": {"simulate_fills": true}` 로 두면 TEST 신호가 `PaperExchange` 로 체결되어
회신·스냅샷 흐름까지 확인할 수 있다 (실거래소 호출 없음). `false` 면 TEST 는 **기록 전용**(`acknowledged` 회신 하나만).

## 모드와 안전 게이트

| 게이트 | 효과 |
|---|---|
| 신호 `mode: test` | 실거래소를 절대 건드리지 않음. 기록 전용 또는 PaperExchange 시뮬레이션 |
| 신호 `mode: live` | `live.enabled=true` **그리고** 실제 Bybit 키 **그리고** `LAKE_SIGNAL_SECRET_LIVE` 가 있어야 실행. 아니면 접수(202)는 하되 `rejected/LIVE_DISABLED` 회신 |
| `state/HALT` 파일 | 새 신호를 전부 `rejected/OPERATOR_HALT`. 거래소의 기존 보호주문(SL/TP)은 그대로 유지. 스냅샷·회신은 계속 |
| 불일치 플래그 (`RECONCILE_REQUIRED`) | 거래소 포지션 ≠ 우리 lot 합계이면 해당 mode 의 `entry/add` 거부, 청산류는 허용, **스냅샷 전송 중단**(`complete:true` 를 보낼 수 없으므로) |
| 수량 가드 | `qtyStep` 내림 → `min_qty` 미만 `QTY_BELOW_MIN`, 주문 1건 > `guards.max_order_qty_btc` → `QTY_LIMIT`, 레그 합계 > `guards.max_leg_qty_btc` → `LEG_LIMIT` |
| 슬리피지 가드 (entry/add) | `|last − reference_price| / reference_price` > `guards.max_entry_slippage_pct` % → `SLIPPAGE_GUARD` (reference_price 가 null 이면 생략) |
| 플레이스홀더 키 | `.env` 의 Bybit 키가 비어 있거나 `PUT_` 플레이스홀더이면 live 실행 불가 |
| 멱등 주문 | 모든 주문은 `orderLinkId = f(mode, event_id)` 로 멱등. 재시작 시 `processing` 신호는 재실행하지 않고 거래소 조회로 마무리 |

시크릿은 방향·모드별로 분리한다: 수신 검증 `LAKE_SIGNAL_SECRET_{TEST,LIVE}`, 회신 서명 `LAKE_REPORT_SECRET_{TEST,LIVE}`,
회신 URL `LAKE_REPORT_URL_{TEST,LIVE}`. 해당 모드의 수신 키가 없으면 그 모드 신호는 **503**.
회신 URL/키가 없으면 회신은 `unsent` 로 저장만 된다.

## 배포

AWS EC2(서울) + Elastic IP + Caddy(자동 HTTPS 443) → `127.0.0.1:8787` lake-executor(systemd).
절차·명령은 **`deploy/README.md`** 참고 (`provision.py` → `push.py` → `.env`/`config.json` 작성 → `finalize.py`).
Bybit API 키는 Elastic IP 로 IP 제한하므로 로컬에서 `check` 가 Bybit 단계에서 실패하는 것이 정상이다.

## 절대 커밋 금지 (`.gitignore` 처리됨)

`.env`(API 키·시크릿·회신 URL), `config.json`, `state/`(SQLite 원장·로그·HALT), `*.db*`, `*.log`, `*.pem`(EC2 SSH 키),
`deploy/aws_state.json`(인스턴스 ID·IP·키 경로). 시크릿은 문서·대화·소스·로그 어디에도 넣지 않는다 (로그 포맷터가
알려진 시크릿 값을 `***` 로 마스킹하지만 안전망일 뿐이다).
