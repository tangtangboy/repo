# lake-executor 아키텍처 (모듈 계약)

> 구현 에이전트/개발자가 따르는 **고정 계약**입니다. 근거 문서: `docs/lake_handoff/*` (lake 측 협의안·회신 수신 규격).
> 이미 구현·검증된 기반 모듈: `lake_executor/{util,config,auth,schemas,store}.py` — 인터페이스를 바꾸지 말고 사용하세요.

## 0. 한 줄 요약
lake(전략 측)가 **HTTPS POST 웹훅**으로 신호(entry/add/partial_exit/full_exit/protection_update)를 보내면,
우리는 **서명·시각·만료·중복·순서 검증 → 영속 접수(2xx)** → 워커가 **Bybit linear BTCUSDT(헤지 모드)** 에 주문/보호주문을 넣고,
**체결 회신(execution)** 과 **30초 전체 포지션 스냅샷(snapshot)** 을 lake 회신 URL로 HMAC 서명해 보낸다. 운영은 AWS EC2(서울)+Caddy(HTTPS)+systemd.

```
lake ──POST /lake/signal (X-Signature/X-Timestamp)──▶ receiver.py ─persist─▶ store.signals(accepted)
                                                                              │
                                   executor.py (단일 워커, FIFO) ◀─claim──────┘
                                      │ exchange.py (BybitExchange | PaperExchange)
                                      │ lots 원장 갱신, fills 기록
                                      ▼
                                   reporter.py ─allocate(seq)─▶ store.reports(pending) ─deliver thread─▶ lake 회신 URL
                                   snapshot loop(30s): executor.reconcile → reporter.snapshot
```

## 1. 모드와 게이트 (가장 중요)
| 신호 `mode` | 처리 |
|---|---|
| `test` | **기록만**. 접수 후 `acknowledged` 회신(TEST 회신 키/URL 있을 때). `settings.test_simulate_fills=true` 면 **PaperExchange** 로 실행해 체결 회신·스냅샷까지 TEST 모드로 보냄. 실거래소는 절대 건드리지 않음. |
| `live` | `settings.live_execution_possible()` 가 `(True,"")` 일 때만 **BybitExchange** 실행. 아니면 접수는 하되(202) 실행기에서 `rejected` + `reason_code=LIVE_DISABLED` 회신. |
| 공통 | `state/HALT` 파일 존재 → 실행기는 새 신호를 `rejected/OPERATOR_HALT` 로 처리(기존 보호주문 유지). `store.is_inconsistent(mode)` 면 `entry/add` 는 `rejected/RECONCILE_REQUIRED`, 청산류는 허용. |

시크릿은 모드별로 분리: 수신 검증 `LAKE_SIGNAL_SECRET_{TEST,LIVE}`, 회신 서명 `LAKE_REPORT_SECRET_{TEST,LIVE}`, 회신 URL `LAKE_REPORT_URL_{TEST,LIVE}`. 해당 모드 수신 키가 없으면 그 모드 신호는 **503**.

## 2. HTTP 수신 규격 (receiver.py) — 상대에게 회신할 우리 쪽 스펙
- `POST {signal_path}` (기본 `/lake/signal`), `Content-Type: application/json`, 본문 ≤ `max_body_bytes`(65536), `Content-Encoding` 불허.
- 헤더 `X-Signature` = hex(HMAC-SHA256(secret, raw_body)) 64자 소문자, `X-Timestamp` = Unix ms = 본문 `ts`, 수신 시각 ±60s.
- 검증 순서: 크기/인코딩(413/415) → JSON 파싱 후 `mode` 로 시크릿 선택(파싱 불가 400) → `auth.verify`(401) → `schemas.Signal`(400) → `symbol`/`position_idx`∈`allowed_position_idx()` 등 의미 검증(400) → 만료 `now_ms > expires_at_ms`(**410**) → `store.insert_signal`.
- `insert_signal` 결과: `new`→**202** `{"accepted":true,"event_id":...}` / `duplicate`→**200** `{"accepted":true,"duplicate":true}` / `conflict`·`sequence_conflict`→**409** `{"error":"CONFLICT","code":...}`.
- 2xx 는 **접수 확인**일 뿐이며 실행 결과는 회신(execution report)으로만 전달.
- 접수 거부(401/400/409/410/413/415)는 `store.log_ingress(code, event_id, body)` 로 남긴다(원본 본문은 저장하지 않고 sha256 만).
- 응답 본문에 시크릿·내부 예외 원문을 넣지 않는다. 5초 안에 응답(영속 접수 외의 작업은 하지 않음).
- `GET /healthz` → 200 `{"ok":true,"halted":bool,"inconsistent":{"test":..,"live":..}}` (인증 없음, 내용 최소).
- 관리(모두 `X-Admin-Token` 헤더 필요, `ADMIN_TOKEN` 미설정 시 404): `GET /state`(최근 신호/회신/오픈 lot/거래소 포지션 요약), `POST /admin/halt`, `POST /admin/resume`, `POST /admin/reconcile?mode=live`.
- 앱 팩토리: `create_app(settings, store, services) -> FastAPI` 로 테스트 가능해야 함(`services` 는 executor/reporter 핸들을 담은 간단한 객체; 수신 경로는 executor 를 직접 호출하지 않고 DB 에만 쓴다).

## 3. 실행기 (executor.py)
`Executor(settings, store, exchanges: dict[str, ExchangeBase|None], reporter, alerts)` — `exchanges["live"]` 는 BybitExchange 또는 None, `exchanges["test"]` 는 PaperExchange(simulate_fills 일 때) 또는 None.

- `run_once() -> bool`: `store.claim_next_signal()` 로 하나 집어 `process(row)`. `run_forever(stop_event)` 는 0.2s 폴링.
- `process(row)`: `raw_body` 를 `Signal` 로 복원 → 게이트 판단 → 액션 처리 → `store.set_signal_result(...)`.
- **시작 시 `recover_processing()`**: `status=processing` 신호는 재실행하지 않는다. 그 신호의 `orders`(purpose=`entry|add|exit`) 를 찾아 거래소에서 `get_order(order_link_id)` 조회: 주문이 있으면 체결 수집을 이어서 마무리, 없으면 `error/UNKNOWN_STATE` 로 닫고 알림.
- 주문 멱등 키: `util.order_link_id(mode, event_id)`(진입/추가/청산), 보호주문은 `util.order_link_id(mode, position_id, "sl"|"tp", str(revision), str(i))`.
- 수량: `exchange.instrument()` 의 `qty_step` 으로 `util.floor_step`, `min_qty` 미만이면 `rejected/QTY_BELOW_MIN`. `max_order_qty_btc` 초과 `QTY_LIMIT`, 레그 합계(해당 `position_idx` 의 open lots 합 + 주문량) > `max_leg_qty_btc` 면 `LEG_LIMIT`.
- 슬리피지 가드(entry/add 만): `|last_price − reference_price| / reference_price * 100 > max_entry_slippage_pct` → `SLIPPAGE_GUARD` (reference_price 가 null 이면 생략).

액션별 규칙 (lot = `store.get_lot(mode, position_id)`):
| action | 전제 | 동작 |
|---|---|---|
| entry | lot 없음 또는 closed (open 이면 `POSITION_EXISTS`) | 시장가 `sig.side()` qty, positionIdx 신호값 → 체결 수집 → lot 생성(qty=체결합, avg_entry=가중평균) → 신호에 SL/TP 있으면 보호주문 생성 |
| add | lot open (아니면 `POSITION_NOT_FOUND`) | 시장가 같은 방향 → lot qty/avg 갱신 → 보호주문 수량 재설정(취소 후 재생성, revision 유지) |
| partial_exit | lot open, `qty_btc ≤ lot.qty + step/2` (초과 `QTY_EXCEEDS_LOT`) | reduceOnly 시장가 `sig.close_side()` → lot qty 감소 → 보호주문 수량 재설정; qty→0 이면 lot closed + 보호주문 취소 |
| full_exit | lot open | reduceOnly 시장가 **lot 잔량만**(심볼 전량 아님) → 보호주문 전부 취소 → lot closed |
| protection_update | lot open, `protection_revision > lot.protection_revision` (아니면 `STALE_PROTECTION_REVISION`) | 기존 보호주문 취소 → `stop_loss`(null=손절 없음) / `take_profit`(null 또는 []=익절 없음, 배열=각 가격에 lot 수량 균등 분할, 마지막에 나머지) 로 재생성 → lot 갱신 → 회신 `protection_updated` |

- 체결 수집: `place_market` 후 `fill_poll_timeout_s` 동안 `get_order` 폴링 → 종결 상태면 `executions(order_id)` 로 체결 목록 → `store.insert_fill`(새 것만) → 각 체결마다 `reporter.execution(status=partially_filled|filled, qty=그 체결 수량, fill_price, order_id, fill_id)`. 마지막 체결(누적=주문량)만 `filled`, 그 전은 `partially_filled`. 주문이 `Rejected` → `rejected/EXCHANGE_REJECTED`, 체결 0 으로 `Cancelled`(IOC) → `cancelled`, 타임아웃 → `error/EXCHANGE_TIMEOUT` + 알림(주문은 남아 있을 수 있으니 recover 로 재확인).
- `expected_qty_btc_after` 가 있고 처리 후 lot.qty 와 `qty_eq` 가 아니면 알림 + 신호 note 에 `QTY_MISMATCH` (회신 status 는 실제 체결 그대로).
- 회신 순서(한 신호당): `acknowledged`(claim 직후) → `submitted`(주문 ID 확보) → 체결들 → (실패 시 `rejected|cancelled|error`) → **스냅샷**(`reporter.snapshot`, 체결·보호가격 변경 직후 필수). test 기록전용 모드는 `acknowledged` 뒤 `rejected/TEST_RECORD_ONLY` 가 아니라 **`acknowledged` 하나만** 보낸다.
- 보호주문 = 우리가 내는 **조건부 reduceOnly 시장가 주문**(`place_conditional`). SL: 롱 lot 은 `trigger_direction=2`(하락), 숏 lot 은 1(상승); TP 는 반대. `trigger_by=settings.protection_trigger_by`. `lot.protection_orders = {"sl": {"order_link_id","order_id","price","qty"}|None, "tp":[{...}]}`.
- `reconcile(mode) -> bool`(스냅샷 전·주기적): ① lot 의 보호주문 상태를 `get_order` 로 조회해 `Filled|PartiallyFilled` 면 체결을 수집, lot qty 차감, 회신(event_id=`auto:sl:{position_id}:{revision}` / `auto:tp:{position_id}:{revision}:{i}`, action=`partial_exit`|`full_exit`, reason_code=`STOP_LOSS_TRIGGERED|TAKE_PROFIT_TRIGGERED`), qty→0 이면 lot closed + 형제 보호주문 취소. ② `exchange.positions()` 의 positionIdx 별 size 와 open lots 합을 `util.qty_eq(…, qty_step)` 로 비교. 불일치 → `store.set_inconsistent(mode, True, note)` + 알림 + **스냅샷 보내지 않음**, 일치 → `set_inconsistent(mode, False)` 후 True.
- 시작 시 `exchange.ensure_account_setup(position_mode, leverage, margin_mode)` (live 에서만).

## 4. 거래소 래퍼 (exchange.py)
공통 인터페이스 `ExchangeBase` (두 구현 모두 동일 시그니처, 수량/가격은 float, 문자열 변환은 내부에서 `util.fmt_step`):
```python
class ExchangeError(Exception): code: str            # EXCHANGE_ERROR | EXCHANGE_TIMEOUT
class ExchangeRejected(ExchangeError): code = "EXCHANGE_REJECTED"; ret_code: int|None
class ExchangeBase:
    name: str                                         # "bybit" | "paper"
    def instrument(self) -> dict                      # {"qty_step","min_qty","max_qty","tick"} (캐시)
    def last_price(self) -> float
    def mark_price(self) -> float | None
    def ensure_account_setup(self, position_mode: str, leverage: int, margin_mode: str) -> None
    def positions(self) -> dict[int, dict]            # {position_idx: {"size","side","avg_price","mark_price","updated_time_ms"}} size>0 만
    def place_market(self, side, qty, position_idx, reduce_only, order_link_id) -> dict   # {"order_id"}
    def place_conditional(self, side, qty, position_idx, trigger_price, trigger_direction, order_link_id, trigger_by) -> dict  # reduceOnly 시장가 스탑; {"order_id"}
    def cancel_order(self, order_link_id) -> bool     # 취소됨/이미 없음 → True
    def get_order(self, order_link_id) -> dict | None # {"order_id","status","qty","cum_qty","avg_price","trigger_price"}; status ∈ New|PartiallyFilled|Filled|Cancelled|Rejected|Untriggered|Triggered|Deactivated
    def executions(self, order_id) -> list[dict]      # [{"exec_id","qty","price","exec_time_ms","order_id"}]
    def open_conditional_orders(self, position_idx) -> list[dict]  # [{"order_link_id","order_id","trigger_price","qty","side"}]
```
- `BybitExchange(settings)` = pybit `unified_trading.HTTP(testnet, api_key, api_secret)`; `category=linear`, `symbol`. 매핑: `place_order(category,symbol,side,orderType="Market",qty,positionIdx,reduceOnly,orderLinkId,timeInForce="IOC")`; 조건부: `place_order(..., orderType="Market", triggerPrice, triggerDirection, triggerBy, reduceOnly=True, closeOnTrigger=True, orderLinkId, positionIdx)`; `cancel_order(category,symbol,orderLinkId)`; `get_open_orders(category,symbol,orderLinkId)` → 없으면 `get_order_history(category,symbol,orderLinkId)`; `get_executions(category,symbol,orderId,limit=100)`; `get_positions(category,symbol)`; `switch_position_mode(category,symbol,mode=3|0)`, `set_leverage`, `set_margin_mode`(UTA, "ISOLATED_MARGIN"|"REGULAR_MARGIN"); "not modified"(110043/110026 등) 는 무시. 거래소 예외 원문은 로그(WARNING)에만, 회신에는 코드만.
- `PaperExchange(settings, price: float = 85000.0)` = 메모리 시뮬레이터: `set_price(p)` 로 시세 갱신 시 조건부 주문 트리거 → 체결 생성, 시장가는 즉시 전량 체결(`exec_id` 는 증가 번호), positions 합산/평균가 계산, reduceOnly 가 포지션 초과면 포지션만큼만 체결. 테스트와 `test_simulate_fills` 에 사용.

## 5. 회신 (reporter.py) — 계약: `docs/lake_handoff/lake_execution_contract.json` `incoming_execution_reports`
```python
class Reporter:
    def __init__(self, settings, store, alerts, client=None)   # client: httpx.Client 호환(테스트 주입)
    def execution(self, mode, *, event_id, position_id, strategy, leg, position_idx, action, status,
                  qty=None, fill_price=None, order_id=None, fill_id=None, reason_code=None, observed_at_ms=None) -> dict
    def snapshot(self, mode, positions: list[dict], observed_at_ms: int) -> dict
    def deliver_pending(self, mode) -> int          # sequence 순서대로 전송, 보낸 개수
    def run_forever(self, stop_event)               # 두 모드 pending 을 0.5s 주기로 drain
    @staticmethod
    def pseudonym(kind: str, real_id: str | None) -> str | None   # "o-"/"f-" + sha256 24자
```
- 본문은 **계약 스키마 그대로**(추가 필드 금지, `schema_version:1`, `exchange:"Bybit"`, `category:"linear"`, `symbol:"BTCUSDT"`). 체결 외 상태는 `qty/fill_price/fill_id = null`. `protection_updated` 는 `action:"protection_update"`. snapshot 은 `complete:true`, `account_scope:"lake_dedicated_BTCUSDT"`, 각 position 의 `updated_at_ms ≤ observed_at_ms`, `take_profit` 은 `[]`(확인된 익절 없음) / 가격 배열 / `null`(미확인), `stop_loss` 는 확인된 가격 또는 `null`.
- `store.allocate_report(mode, kind, observed_at_ms, build, util.canonical_json)` 로 sequence·report_id·ts·observed 클램프를 받아 본문을 만든다. **본문 바이트는 저장된 그대로 전송**(재직렬화 금지).
- 전송: `POST report_url[mode]`, 헤더 `auth.headers_for(body, report_secret[mode], ts)`(ts = 본문 ts), timeout `report_http_timeout_s`. 202/200 → `sent`(200 은 note=duplicate), 409 → `conflict`+알림, 그 외 4xx → `failed`+알림, 5xx/타임아웃/연결오류 → 재시도(최대 `report_max_attempts`, 첫 생성 후 `report_attempt_window_ms` 안에서만; 창을 넘기면 `failed`+알림 — ts 가 오래돼 서명 창을 벗어나기 때문). URL 또는 키 미설정 → `unsent` 로 표시만.
- 같은 mode 의 전송은 sequence 오름차순으로 직렬화하고, 앞 번호가 재시도 대기 중이면 뒤 번호를 먼저 보내지 않는다.

## 6. 운영 유틸 (ops.py)
- `setup_logging(settings)`: stdout + `log_file`(있으면) ; 로그에 시크릿·원본 본문·거래소 오류 원문 전체를 남기지 않는다(원문은 WARNING 에서 200자 절단).
- `Alerts(settings)`: `.send(text)` → 텔레그램(설정 시) + 로그 ERROR. 예외는 삼킨다. 같은 메시지 60초 내 반복은 1회만.
- `halted(settings) -> bool` (HALT 파일 존재), `halt(settings)`, `resume(settings)`.

## 7. CLI (main.py, `python -m lake_executor …`)
- `serve [--config config.json] [--env .env]`: store 열기 → exchanges 준비(live 가능하면 Bybit, test_simulate_fills 면 Paper) → `executor.recover_processing()` → 스레드: executor.run_forever, reporter.run_forever, snapshot loop(모드별 `snapshot_interval_ms` 마다 `reconcile` 후 `reporter.snapshot`) → uvicorn(listen host/port). SIGTERM 시 정상 종료.
- `check`: 설정 로드, 키 유무, Bybit 연결(잔고·instrument·positions 읽기 전용), 회신 URL 설정 여부 출력. 주문 없음.
- `simulate [--base-url http://127.0.0.1:8787]`: TEST 시크릿으로 서명한 합성 신호를 entry→add→partial_exit→protection_update→full_exit 순으로 전송하고 응답 코드 출력(서버가 `test_simulate_fills=true` 면 체결 회신까지 흐름 확인).
- `sign --file signal.json --mode test|live`: 파일 본문에 현재 ts 를 넣고 서명 헤더를 출력(상대 테스트용 curl 예시 포함). `lake_executor/__main__.py` 에서 `main.main()` 호출.

## 8. 배포 (deploy/) — autotrading 레포의 방식을 그대로 따른다
- `provision.py`(boto3, `~/.aws/credentials` 프로필 `AWS_PROFILE` 환경변수 또는 기본): ap-northeast-2, t3.micro Ubuntu 24.04 amd64, 16GB gp3, 키페어 `lake-executor`(로컬 .pem 저장), SG: 22 ← 배포 PC /32, 80/443 ← 0.0.0.0/0, Elastic IP → `deploy/aws_state.json` 에 저장(gitignore). 이미 있으면 중단.
- `push.py`: SFTP 로 소스 업로드 + `setup-server.sh` 실행(venv, 의존성, **Caddy** 설치, systemd 유닛 설치, 서비스는 아직 시작 안 함). `finalize.py`: `.env`/`config.json` 업로드(chmod 600) → `python -m lake_executor check` → `systemctl enable --now lake-executor caddy` → 상태/로그 출력. `ssh_run.py "<cmd>"`.
- `Caddyfile`: `{$PUBLIC_HOST}` 블록이 `reverse_proxy 127.0.0.1:8787` (자동 HTTPS). `PUBLIC_HOST` 는 `/etc/caddy/env` 에서 읽음; 기본값은 `<EIP를 -로 바꾼 값>.sslip.io` (도메인 없을 때), 도메인이 있으면 그걸로 교체. `lake-executor.service`: `User=ubuntu`, `WorkingDirectory=/home/ubuntu/lake-executor`, `ExecStart=/home/ubuntu/lake-executor/.venv/bin/python -m lake_executor serve`, `Restart=always`, `RestartSec=5`.
- 서버 바인딩은 127.0.0.1:8787 만, 외부는 Caddy 443 만. Bybit API 키 IP 화이트리스트 = Elastic IP.

## 9. 테스트 (tests/, pytest)
- `test_auth.py`: 정상/서명 오류/시각 창/헤더-본문 ts 불일치/시크릿 미설정.
- `test_receiver.py`(FastAPI TestClient + 임시 store): 202/200 dup/409 conflict/409 seq/410 expired/400 schema/401/413/415/503(키 없음), position_idx 모드 불일치 400, 응답에 시크릿 노출 없음, healthz, admin 토큰.
- `test_executor_paper.py`: PaperExchange 로 entry→add→partial_exit→protection_update→full_exit 전체, lot 수량/평균가, 보호주문 생성·재설정·취소, `QTY_EXCEEDS_LOT`, `STALE_PROTECTION_REVISION`, `POSITION_EXISTS`, `LIVE_DISABLED`, HALT, SL 트리거 후 reconcile 이 lot 을 줄이고 auto 이벤트 회신을 만드는지, 불일치 시 스냅샷 생략.
- `test_reporter.py`: 본문이 계약 스키마와 일치(`jsonschema` 없이 필드/타입 직접 검사), sequence 연속·재시작 유지, observed 단조, 체결 외 null 규칙, 전송 상태 전이(가짜 client 주입: 202/200/409/500→재시도/타임아웃).
- 전부 네트워크 없이 통과해야 하며 `python -m pytest -q` 로 실행.

## 10. 금지/주의
- 실거래소 호출은 `BybitExchange` 안에서만. 테스트는 절대 실키를 쓰지 않는다.
- 원본 본문·시크릿·거래소 오류 원문을 회신/응답/INFO 로그에 넣지 않는다.
- `close` 류 신호를 **심볼 전량 청산으로 확대하지 않는다**(lot 잔량만).
- 과거/재전송 신호를 새 매매로 처리하지 않는다(event_id 영속, sequence 검증).
