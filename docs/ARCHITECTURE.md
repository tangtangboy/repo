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
- `POST {signal_path}` (기본 `/lake/signal`), `Content-Type: application/json` **필수**(없거나 다르면 415), 본문 ≤ `max_body_bytes`(65536), `Content-Encoding` 불허.
  본문은 스트리밍으로 읽어 상한을 넘는 순간 413 (Content-Length 없는 chunked 요청도 메모리 상한 적용). 검증·접수는 스레드풀에서 실행(이벤트 루프 비차단).
- 헤더 `X-Signature` = hex(HMAC-SHA256(secret, raw_body)) 64자 소문자, `X-Timestamp` = Unix ms = 본문 `ts`, 수신 시각 ±60s.
- 검증 순서: 크기/인코딩(413/415) → JSON 파싱 후 `mode` 로 시크릿 선택(파싱 불가·과도한 중첩 400) → `auth.verify`(401; 본문 `ts` 가 정수가 아니면 서명 확인 뒤 401 `TIMESTAMP_MISMATCH`) → `schemas.Signal`(400) → `symbol`/`position_idx`∈`allowed_position_idx()` 등 의미 검증(400) → 만료 `now_ms > expires_at_ms`(**410**) → `store.insert_signal`.
- `insert_signal` 결과: `new`→**202** `{"accepted":true,"event_id":...}` / `duplicate`→**200** `{"accepted":true,"duplicate":true}` / `conflict`·`sequence_conflict`→**409** `{"error":"CONFLICT","code":...}`.
  **`event_id` 유일성은 mode 별**(`signals` PK = `(mode, event_id)`): TEST 키 보유자가 LIVE 의 event_id 를 선점할 수 없다.
- 2xx 는 **접수 확인**일 뿐이며 실행 결과는 회신(execution report)으로만 전달.
- 접수 거부 기록: **서명을 통과한 뒤의 거부**(400 SCHEMA/의미, 401 TIMESTAMP_MISMATCH, 409, 410)만 `store.log_ingress(code, event_id, body)` 로 남긴다(원본 본문은 저장하지 않고 sha256 만). 서명 통과 뒤의 `duplicate`(200) 도 `DUPLICATE` 행으로 남긴다(대시보드에서 재전송 추적용; 실행은 여전히 한 번). **서명 전 거부**(400 BAD_JSON/BAD_MODE, 401, 413, 415, 503)는 DB 에 쓰지 않고 메모리 카운터(`/state` 의 `ingress_rejections`)로만 센다 — 무인증 요청 폭주가 디스크/DB 락을 소모하지 못하게. `ingress_log` 는 실행기가 7일/5만 행 기준으로 정리한다. 로그에 찍는 event_id 는 ID 패턴을 통과한 값만(로그 주입 방지).
- 응답 본문에 시크릿·내부 예외 원문을 넣지 않는다. 예상 못 한 검증 예외도 500 이 아니라 400 으로 닫는다. 5초 안에 응답(영속 접수 외의 작업은 하지 않음).
- `GET /healthz` → 200 `{"ok":true,"halted":bool,"inconsistent":{"test":..,"live":..},"protection_missing":{"test":n,"live":n}}` (인증 없음, 내용 최소; `protection_missing` = 보호가격이 설정됐는데 보호주문이 빠진 open lot 수).
- 관리(모두 `X-Admin-Token` 헤더 필요, `ADMIN_TOKEN` 미설정 시 404, **`ADMIN_TOKEN` 은 32바이트 이상**(config 검증), 같은 IP 의 실패 10회/60초 뒤에는 비교 없이 401): `GET /state`(최근 신호/회신/오픈 lot/거래소 포지션 요약/`protection_missing`/`ingress_rejections`), `POST /admin/halt`, `POST /admin/resume`, `POST /admin/reconcile?mode=live`. 배포에서는 Caddy 가 `/state`, `/admin/*` 를 `ADMIN_ALLOW_CIDR`(기본 서버 로컬만) 밖에서는 404 로 막는다.
- 앱 팩토리: `create_app(settings, store, services) -> FastAPI` 로 테스트 가능해야 함(`services` 는 executor/reporter 핸들을 담은 간단한 객체; 수신 경로는 executor 를 직접 호출하지 않고 DB 에만 쓴다).
  관리 동작 본체는 `app.state.admin_ops`(`halt(source)`, `resume(source)`, `reconcile(mode, account) -> (status, body)`) 로 노출되어 JSON 엔드포인트와 대시보드가 공유한다. `app.state.admin_throttle` 도 공유.

## 2a. 운영 대시보드 (web.py)
- `mount(app, settings, store, services, admin_throttle)` 를 `create_app` 마지막 줄에서 호출한다(함수 안 import, 순환 import 회피). stdlib + fastapi/starlette 만, 템플릿·JS·CDN 없음. `services.paths`(`env`/`config` 경로) 와 `services.started_ms` 는 `main.cmd_serve` 가 넣는다 — 없으면 쓰기 폼은 409 `ENV_PATH_UNKNOWN`, 읽기 페이지는 그대로.
- 게이트 순서(모든 `/ui*`): `ADMIN_TOKEN` 미설정 → 404 `{"error":"NOT_FOUND"}` → 세션 쿠키 `lake_ui`(`sid.exp_ms.sig`, 키 = HMAC(ADMIN_TOKEN); GET 은 `/ui/login` 으로 303, POST 는 401) → POST 는 `_csrf`(세션 파생, 실패는 `admin_throttle.fail(ip)`) → 핸들러. 로그인 실패는 `/admin/*` 와 같은 IP 별 예산. 폼은 `application/x-www-form-urlencoded` ≤ 16KB.
- 페이지: Overview(거래소 호출 없음) / Signals(+상세: 원본 페이로드, run, lot, 주문·체결, event_id 를 실은 회신, ingress 행) / Orders(`raw` 제외) / Reports(본문 길이만) / Ingress / Accounts(키 저장·연결 Check) / Secrets / Controls(HALT·재개·대사·live.enabled·재시작·최근 20건).
- **핫스왑 금지 규칙**: 거래소 래퍼·`Executor.exchanges`·스냅샷 스트림·`Alerts` 토큰·`RedactingFormatter` 는 기동 시 고정이므로 프로세스 내 교체를 하지 않는다. 디스크의 `.env`/`config.json` 이 단일 진실(검증: `config.load(env_override=…)` + 거래소 생성자 드라이런 → `util.write_env_file`(원자적, `.env.bak`, 0600) → 재검증, 실패 시 복원), 적용은 `RestartGuard`(기동 60초 뒤, 60초 간격, 5분 3회; 이력은 meta `ui_restarts`) 의 SIGTERM → systemd 재시작뿐. 유일한 예외는 `settings.live_enabled = False`(즉시, 보수적 방향).
- 값은 어디에도 싣지 않는다: `mask()`(앞 4자 + 길이), URL 은 호스트만, 거래소 예외는 코드/타입 이름만, 플래시·알림·감사는 키 이름만. 응답 헤더 `Cache-Control: no-store`, CSP `default-src 'none'`, `X-Frame-Options: DENY`.

## 3. 실행기 (executor.py)
`Executor(settings, store, exchanges: dict[str, ExchangeBase|None], reporter, alerts)` — `exchanges["live"]` 는 BybitExchange 또는 None, `exchanges["test"]` 는 PaperExchange(simulate_fills 일 때) 또는 None.

- `run_once() -> bool`: `store.claim_next_signal()` 로 하나 집어 `process(row)`. `run_forever(stop_event)` 는 0.2s 폴링.
- `process(row)`: `raw_body` 를 `Signal` 로 복원 → 게이트 판단 → 액션 처리 → `store.set_signal_result(...)`.
- **시작 시 `recover_processing()`**: `status=processing` 신호는 재실행하지 않는다. 그 신호의 `orders`(purpose=`entry|add|exit`) 를 찾아 거래소에서 `get_order(order_link_id)` 조회: 주문이 있으면 체결 수집을 이어서 마무리, 없으면 `error/UNKNOWN_STATE` 로 닫고 알림(주문 행은 `unknown` 으로 남겨 reconcile 재확인 대상). `protection_update` 중 죽었고 lot 이 이미 그 revision 이면 `protection_orders.failed=true` 로 표시해 reconcile 이 보호주문을 다시 만든다.
- **실행 시점 만료**: `acknowledged` 직후 `now_ms > expires_at_ms` 면 `rejected/EXPIRED` (접수 뒤 재시작·백로그·HALT 해제로 늦어진 신호를 현재가로 실행하지 않는다).
- 단방향(`position_idx 0`) 에서는 반대 방향 lot 이 열려 있으면 `entry/add` 를 `rejected/OPPOSING_LEG` (거래소가 네팅해 버리므로).
- `ensure_account_setup` 은 같은 설정 서명을 `meta` 에 기록해 재기동마다 거래소 쓰기를 반복하지 않는다.
- 주문 멱등 키: `util.order_link_id(mode, event_id)`(진입/추가/청산), 보호주문은 `util.order_link_id(mode, position_id, "sl"|"tp", str(revision), str(i))`(한 revision 의 첫 생성). 같은 revision 안에서 수량 재설정으로 취소 후 **재생성**할 때는 거래소의 orderLinkId 중복 거부(110072)를 피하기 위해 마지막 인자로 `"g<n>"`(n = 그 revision 에서 이미 낸 생성 횟수, `lot.protection_orders["gen"]`) 을 덧붙인다.
- 수량: `exchange.instrument()` 의 `qty_step` 으로 `util.floor_step`, `min_qty` 미만이면 `rejected/QTY_BELOW_MIN`. `max_order_qty_btc` 초과 `QTY_LIMIT`, 레그 합계(해당 `position_idx` 의 open lots 합 + 주문량) > `max_leg_qty_btc` 면 `LEG_LIMIT`.
- 슬리피지 가드(entry/add 만): `|last_price − reference_price| / reference_price * 100 > max_entry_slippage_pct` → `SLIPPAGE_GUARD` (reference_price 가 null 이면 생략).

액션별 규칙 (lot = `store.get_lot(mode, position_id)`):
| action | 전제 | 동작 |
|---|---|---|
| entry | lot 없음 또는 closed (open 이면 `POSITION_EXISTS`) | 시장가 `sig.side()` qty, positionIdx 신호값 → 체결 수집 → lot 생성(qty=체결합, avg_entry=가중평균) → 신호에 SL/TP 있으면 보호주문 생성 |
| add | lot open (아니면 `POSITION_NOT_FOUND`) | 시장가 같은 방향 → lot qty/avg 갱신 → 보호주문 수량 재설정(취소 후 재생성, revision 유지) |
| partial_exit | lot open, `qty_btc ≤ lot.qty + step/2` (초과 `QTY_EXCEEDS_LOT`) | reduceOnly 시장가 `sig.close_side()` → lot qty 감소 → 보호주문 수량 재설정; qty→0 이면 lot closed + 보호주문 취소 |
| full_exit | lot open | reduceOnly 시장가 **lot 잔량만**(심볼 전량 아님) → 보호주문 전부 취소 → lot closed |
| protection_update | lot open, `protection_revision > lot.protection_revision` (아니면 `STALE_PROTECTION_REVISION`; 단 **같은 revision 의 보호주문이 실패해 비어 있으면(`protection_orders.failed`) 같은 revision 재전송을 멱등 재시도로 받는다**) | 기존 보호주문 취소(체결 경합 확인) → lot 에 새 `stop_loss`(null=손절 없음) / `take_profit`(null 또는 []=익절 없음, 배열=각 가격에 lot 수량 균등 분할, 마지막에 나머지) / revision 기록 → 재생성 → 회신 `protection_updated`. 재생성 실패 시 `error/PROTECTION_FAILED` + 알림, lot 은 새 의도를 유지하고 reconcile 이 다시 만든다 |

- 체결 수집: `place_market` 후 `fill_poll_timeout_s` 동안 `get_order` 폴링 → 종결 상태(`Filled|Rejected|Cancelled|Deactivated|PartiallyFilledCanceled`)면 `executions(order_id)` 로 체결 목록 → `store.insert_fill`(새 것만, applied=0) → lot 반영(applied=1) → 각 체결마다 `reporter.execution(status=partially_filled|filled, qty=그 체결 수량, fill_price, order_id, fill_id, mark_fill_reported=exec_id)` (회신 행 생성과 `fills.reported=1` 이 한 트랜잭션 — 재시작 뒤 같은 fill_id 이중 보고 없음, 기록만 되고 반영 안 된 체결은 복구 때 반영). 마지막 체결(누적=주문량)만 `filled`, 그 전은 `partially_filled`. 주문이 `Rejected` → `rejected/EXCHANGE_REJECTED`, 체결 0 으로 `Cancelled`(IOC) → `cancelled`, 부분 체결 뒤 잔량 취소(`PartiallyFilledCanceled`) → 체결 회신 + `cancelled` + note `IOC_PARTIAL`, 타임아웃 → `error/EXCHANGE_TIMEOUT` + 알림. 타임아웃/불명(`place_market` 네트워크 오류 뒤 조회 실패)으로 끝난 주문은 `orders.status ∈ {unknown, submitted, New, PartiallyFilled}` 로 남고 **reconcile 이 종결될 때까지 재확인**한다: 종결되면 체결을 수집·lot 반영·보호주문 생성 후 신호를 `done`(note `LATE_VERIFIED`) 으로 바꾸고 알림, 20회 재확인에도 거래소에 없으면 `absent` 로 닫는다.
- **체결과 보호주문의 분리**: 시장가가 체결된 신호의 회신 상태는 체결이 결정한다. 그 뒤 보호주문 생성/취소가 실패하면 같은 event_id 로 `rejected/error` 를 보내지 않고 신호 `done` + note `PROTECTION_FAILED` + 알림으로 남기며, 직후 스냅샷은 `stop_loss:null` 로 정직하게 나가고 reconcile 이 `lot.stop_loss/take_profit` 로 다시 만든다(자가 복구).
- **트리거가 이미 지난 보호가격**(거래소 거부 110092/110093, "expected Rising/Falling"): 조건부 주문 대신 **지금 reduceOnly 시장가로 그 보호를 실행**한다 — SL 은 lot 전량(`auto:sl` `full_exit`), TP[i] 는 그 레벨 수량(`auto:tp` `partial_exit`, `tp_done` 기록) 후 잔량으로 보호주문 재설정. 포지션을 보호 없이 두지 않는다.
- **취소 vs 체결 경합**: 보호주문을 취소할 때 취소 응답("too late"/110001 포함)과 무관하게 `get_order` 로 최종 상태를 읽어 `Filled|PartiallyFilled|PartiallyFilledCanceled` 면 체결로 처리(auto 회신, lot 차감)한 뒤 항목을 지운다. 취소가 확인되지 않으면(`Triggered` 지속 등) `_ProtectionError`.
- lot 종료(`_close_lot`)는 `closed` 를 **먼저** 기록하고 보호주문을 취소한다. 취소가 실패하면 항목을 남겨 두고 reconcile 이 취소될 때까지 재시도(알림).
- `expected_qty_btc_after` 가 있고 처리 후 lot.qty 와 `qty_eq` 가 아니면 알림 + 신호 note 에 `QTY_MISMATCH` (회신 status 는 실제 체결 그대로).
- 회신 순서(한 신호당): `acknowledged`(claim 직후) → `submitted`(주문 ID 확보) → 체결들 → (실패 시 `rejected|cancelled|error`) → **스냅샷**(`reporter.snapshot`, 체결·보호가격 변경 직후 필수). test 기록전용 모드는 `acknowledged` 뒤 `rejected/TEST_RECORD_ONLY` 가 아니라 **`acknowledged` 하나만** 보낸다.
- 보호주문 = 우리가 내는 **조건부 reduceOnly 시장가 주문**(`place_conditional`). SL: 롱 lot 은 `trigger_direction=2`(하락), 숏 lot 은 1(상승); TP 는 반대. `trigger_by=settings.protection_trigger_by`. `lot.protection_orders = {"sl": {"order_link_id","order_id","price","qty"}|None, "tp":[{...}], "gen", "tp_done", "skipped", "failed"}`. `place_conditional` 이 네트워크/타임아웃으로 실패하면 `get_order(link)` 로 들어갔는지 확인해 있으면 그대로 채택한다(고아·중복 스탑 방지).
- test 모드(PaperExchange)는 시세 피드가 없으므로 신호의 `reference_price` 를 모의 시세로 반영한다(`Executor.follow_reference_price_on_paper`).
- `reconcile(mode) -> bool`(스냅샷 전·주기적). ①~④ 의 실패는 알림만 하고 ⑤ 를 막지 않는다(보호주문 재생성 실패가 스냅샷을 멈추지 않는다):
  ① 보호주문: lot 의 각 항목을 `get_order` 로 조회해 `Filled|PartiallyFilled|PartiallyFilledCanceled` 면 체결을 수집, lot qty 차감, 회신(event_id=**`auto:sl:{position_id}:{revision}:L{opened_at_ms}`** / **`auto:tp:{position_id}:{revision}:{i}:L{opened_at_ms}`** — `L…` 은 lot 인스턴스, 같은 position_id 재진입과 구분; action=`partial_exit`|`full_exit` 는 첫 보고에서 `meta` 에 고정, reason_code=`STOP_LOSS_TRIGGERED|TAKE_PROFIT_TRIGGERED`), qty→0 이면 lot closed + 형제 보호주문 취소. `Cancelled|Deactivated|Rejected` 또는 두 번 연속 조회 불가면 항목 제거 + 알림. 그 뒤 open lot 에 설정된 `stop_loss/take_profit` 대비 빠진 항목(`failed` 포함, min_qty 로 건너뛴 `skipped` 제외)이 있으면 `_reset_protections` 로 다시 만든다(자가 복구; `protection_missing(mode)` 로 노출).
  ② 미확정 시장가 주문 재확인(위 체결 수집 항목). ③ 거래소의 열린 조건부 주문 중 우리 `orders` 에 sl/tp 로 있으나 어떤 lot 도 참조하지 않는 것은 취소(고아 스탑), 우리 기록에 없는 것은 1회 알림. ④ 닫힌 lot 의 잔여 보호주문 취소 재시도.
  ⑤ `exchange.positions()` 의 positionIdx 별 size 와 open lots 합을 `util.qty_eq(…, qty_step)` 로 비교. 불일치 → `store.set_inconsistent(mode, True, note)` + 알림 + **스냅샷 보내지 않음**, 일치 → `set_inconsistent(mode, False)` 후 True. `snapshot_now` 의 `observed_at_ms` 는 락을 잡기 전에 찍는다.
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
- 본문은 **계약 스키마 그대로**(추가 필드 금지, `schema_version:1`, `exchange:"Bybit"`, `category:"linear"`, `symbol:"BTCUSDT"`). 체결 외 상태는 `qty/fill_price/fill_id = null`. `protection_updated` 는 `action:"protection_update"`. snapshot 은 `complete:true`, `account_scope:"lake_dedicated_BTCUSDT"`, 각 position 의 `updated_at_ms ≤ observed_at_ms`, `take_profit` 은 `[]`(확인된 익절 없음) / 가격 배열 / `null`(미확인), `stop_loss` 는 확인된 가격 또는 `null`. idx 0 과 1/2 혼용, idx 0 의 롱/숏 동시 보유는 `ReportError`.
- `store.allocate_report(mode, kind, observed_at_ms, build, util.canonical_json, mark_fill_reported=None)` 로 sequence·report_id·ts·observed 클램프를 받아 본문을 만든다(체결 회신은 같은 트랜잭션에서 `fills.reported=1`). **본문 바이트는 저장된 그대로 전송**(재직렬화 금지).
- 전송: `POST report_url[mode]`, 헤더 `auth.headers_for(body, report_secret[mode], ts)`(ts = 본문 ts), timeout `report_http_timeout_s`. 202/200 → `sent`(200 은 note=duplicate), 409 → `conflict`+알림, 그 외 4xx → `failed`+알림, 5xx/타임아웃/연결오류 → 재시도(최대 `report_max_attempts`, 첫 생성 후 `report_attempt_window_ms` 안에서만; 창을 넘기면 `failed`+알림 — ts 가 오래돼 서명 창을 벗어나기 때문). URL 또는 키 미설정 → `unsent` 로 표시만.
- 같은 mode 의 전송은 sequence 오름차순으로 직렬화하고, 앞 번호가 재시도 대기 중이면 뒤 번호를 먼저 보내지 않는다.

## 6. 운영 유틸 (ops.py)
- `setup_logging(settings)`: stdout + `log_file`(있으면) ; 로그에 시크릿·원본 본문·거래소 오류 원문 전체를 남기지 않는다(원문은 WARNING 에서 200자 절단).
- `Alerts(settings)`: `.send(text)` → 텔레그램(설정 시) + 로그 ERROR. 예외는 삼킨다. 같은 메시지 60초 내 반복은 1회만.
- `halted(settings) -> bool` (HALT 파일 존재), `halt(settings)`, `resume(settings)`.

## 7. CLI (main.py, `python -m lake_executor …`)
- `serve [--config config.json] [--env .env]`: store 열기 → exchanges 준비(live 가능하면 Bybit, test_simulate_fills 면 Paper) → `executor.recover_processing()` → 스레드: executor.run_forever, reporter.run_forever, snapshot loop(모드별 `snapshot_interval_ms` 마다 `reconcile` 후 `reporter.snapshot`; 다음 슬롯은 호출이 끝난 뒤 잡아 락 대기로 간격이 두 배가 되지 않게) → uvicorn(listen host/port). SIGTERM 시 정상 종료. 설정 오류는 exit 2(systemd 는 재시작하지 않음).
- `check`: 설정 로드, 키 유무, Bybit 연결(잔고·instrument·positions 읽기 전용), 회신 URL 설정 여부 출력. 주문 없음.
- `simulate [--base-url http://127.0.0.1:8787]`: TEST 시크릿으로 서명한 합성 신호를 entry→add→partial_exit→protection_update→full_exit 순으로 전송하고 응답 코드 출력(서버가 `test_simulate_fills=true` 면 체결 회신까지 흐름 확인).
- `sign --file signal.json --mode test|live`: 파일 본문에 현재 ts 를 넣고 서명 헤더를 출력(상대 테스트용 curl 예시 포함). `lake_executor/__main__.py` 에서 `main.main()` 호출.

## 8. 배포 (deploy/) — autotrading 레포의 방식을 그대로 따른다
- `provision.py`(boto3, `~/.aws/credentials` 프로필 `AWS_PROFILE` 환경변수 또는 기본): ap-northeast-2, t3.micro Ubuntu 24.04 amd64, 16GB gp3, 키페어 `lake-executor`(로컬 .pem 저장), SG: 22 ← 배포 PC /32, 80/443 ← 0.0.0.0/0, Elastic IP → `deploy/aws_state.json` 에 저장(gitignore). 이미 있으면 중단.
- `push.py`: SFTP 로 소스 업로드 + `setup-server.sh [PUBLIC_HOST] [ADMIN_ALLOW_CIDR]` 실행(venv, 의존성, **Caddy** 설치, systemd 유닛 설치, 서비스는 아직 시작 안 함). `finalize.py`: `.env`/`config.json` 업로드(chmod 600) → `python -m lake_executor check` → `systemctl enable --now lake-executor caddy` → 상태/로그 출력. `ssh_run.py "<cmd>"`.
  SSH 는 공용 `deploy/sshutil.py` 로 연결하며 **호스트 키를 `deploy/known_hosts` 에 고정**한다(AutoAddPolicy 금지): 첫 접속은 지문을 보여 주고 확인(`--trust-new-host-key` / `LAKE_TRUST_NEW_HOST_KEY=1`) 뒤 기록, 이후 불일치는 접속 거부.
- `Caddyfile`: `{$PUBLIC_HOST}` 블록이 `reverse_proxy 127.0.0.1:8787` (자동 HTTPS). `/state`, `/admin/*` 는 `{$ADMIN_ALLOW_CIDR:127.0.0.1/32}` 밖에서 404. `PUBLIC_HOST`/`ADMIN_ALLOW_CIDR` 는 `/etc/caddy/env` 에서 읽음; 기본값은 `<EIP를 -로 바꾼 값>.sslip.io` (도메인 없을 때), 도메인이 있으면 그걸로 교체. `lake-executor.service`: `User=ubuntu`, `WorkingDirectory=/home/ubuntu/lake-executor`, `ExecStart=/home/ubuntu/lake-executor/.venv/bin/python -m lake_executor serve`, `Restart=always`, `RestartSec=5`, `StartLimitIntervalSec=300`/`StartLimitBurst=5`(지속 실패 시 무한 재시작 금지), `RestartPreventExitStatus=2`(설정 오류).
- 서버 바인딩은 127.0.0.1:8787 만, 외부는 Caddy 443 만. Bybit API 키 IP 화이트리스트 = Elastic IP.

## 9. 테스트 (tests/, pytest)
- `test_auth.py`: 정상/서명 오류/시각 창/헤더-본문 ts 불일치/시크릿 미설정.
- `test_receiver.py`(FastAPI TestClient + 임시 store): 202/200 dup/409 conflict/409 seq/410 expired/400 schema/401/413/415/503(키 없음), position_idx 모드 불일치 400, 응답에 시크릿 노출 없음, healthz, admin 토큰.
- `test_executor_paper.py`: PaperExchange 로 entry→add→partial_exit→protection_update→full_exit 전체, lot 수량/평균가, 보호주문 생성·재설정·취소, `QTY_EXCEEDS_LOT`, `STALE_PROTECTION_REVISION`, `POSITION_EXISTS`, `LIVE_DISABLED`, HALT, SL 트리거 후 reconcile 이 lot 을 줄이고 auto 이벤트 회신을 만드는지, 불일치 시 스냅샷 생략.
- `test_reporter.py`: 본문이 계약 스키마와 일치(`jsonschema` 없이 필드/타입 직접 검사), sequence 연속·재시작 유지, observed 단조, 체결 외 null 규칙, 전송 상태 전이(가짜 client 주입: 202/200/409/500→재시도/타임아웃).
- `test_executor_resilience.py`(결함 주입 PaperExchange): 취소 vs 체결 경합, 트리거 이미 지난 SL/TP 즉시 실행, 체결 뒤 보호주문 실패(`PROTECTION_FAILED`) + reconcile 자가 복구, 조건부 타임아웃 채택·고아 스탑 정리, 실행 시점 `EXPIRED`, protection_update 실패 후 같은 revision 재시도, 불명 주문 늦은 확인/`absent`, 취소된 보호주문 재생성, 보호주문 단계 오류가 스냅샷을 막지 않음, `PartiallyFilledCanceled`, lot 종료 취소 실패, `OPPOSING_LEG`, auto 이벤트 ID 인스턴스, 모의 시세 추종.
- `test_receiver.py` 추가: 서명 전 거부는 DB 미기록(카운터), mode 별 event_id, chunked 413, Content-Type 누락 415, 깊은 JSON 400, 문자열 ts 401, event_id 로그 주입 차단, 관리 토큰 무차별 대입 제한, 짧은 `ADMIN_TOKEN` 설정 오류.
- `test_web.py`(대시보드): ADMIN_TOKEN 미설정 404, 로그인 페이지 무값, 잘못된 토큰 401 + `/admin/*` 와 공유되는 IP 예산, 쿠키 속성(`HttpOnly/SameSite=Strict/Path=/ui/Max-Age`, https 면 `Secure`), 쿼리/헤더 토큰 무시, 쿠키 변조·만료·다른 키 거부, 비로그인 POST 401, CSRF 403, 폼 415/413, HALT/재개, 대사(400/429), 개요·신호 목록/상세·필터/페이지·주문(`raw` 비노출)·회신(본문 비노출), DUPLICATE ingress 행, 시크릿 마스킹, 키 저장(`.env` 갱신·`.bak`·0600·pending 배너·알림에 이름만), 잘못된 후보 거부(파일 불변), BAD_KEY, 빈 칸 유지, ADMIN_TOKEN 회전 확인어, live.enabled 토글(끄기 즉시/켜기 재시작/검증 실패 500), RestartGuard(kill 기록기), 계정 Check(디스크 키·10초 제한·오류 코드만), 로그아웃 revoke, paths 없음 409, 전 페이지 시크릿 유출 전수 검사.
- `test_util_env.py`: `.env` 쓰기 왕복(주석·순서 보존, 제자리 교체, 중복 제거, 주석 템플릿 활성화, 추가), 따옴표 규칙, 원자적 교체(`.bak`, 임시 파일 정리, POSIX 0600), `set_json_value`(중첩 생성, 불리언 유지, 검증 실패 시 원본 유지).
- 전부 네트워크 없이 통과해야 하며 `python -m pytest -q` 로 실행.

## 10. 금지/주의
- 실거래소 호출은 `BybitExchange` 안에서만. 테스트는 절대 실키를 쓰지 않는다.
- 원본 본문·시크릿·거래소 오류 원문을 회신/응답/INFO 로그에 넣지 않는다.
- `close` 류 신호를 **심볼 전량 청산으로 확대하지 않는다**(lot 잔량만).
- 과거/재전송 신호를 새 매매로 처리하지 않는다(event_id 영속, sequence 검증).
- 대시보드(`/ui`)는 시크릿 값을 절대 렌더링하지 않는다(마스킹만). 새 값의 적용 경로는 재시작뿐이며, 대시보드는 신호를 만들거나 주문을 내지 않는다.
