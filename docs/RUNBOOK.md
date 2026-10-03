# RUNBOOK — lake-executor 운영

프로덕션은 AWS EC2 의 systemd 서비스 `lake-executor` 하나뿐이다. **로컬에서 `serve` 를 같은 키로 돌리지 않는다**
(Bybit 키가 Elastic IP 로 제한되어 있어 로컬 실주문은 어차피 실패하지만, 원장·회신 sequence 가 갈라진다).
서버 명령은 레포 루트에서 `python deploy/ssh_run.py "<cmd>"`. 배포 절차는 `deploy/README.md`.

경로(서버): 작업 디렉터리 `/home/ubuntu/lake-executor`, 원장 `state/lake.db`, 로그 `state/lake-executor.log`, 정지 파일 `state/HALT`.

## 1. 시작 / 정지 / 중지(HALT) / 재개

| 목적 | 명령 | 효과 |
|---|---|---|
| 상태 | `ssh_run.py "systemctl is-active lake-executor"` | active / inactive |
| 시작 | `ssh_run.py "sudo systemctl start lake-executor"` | `recover_processing` 후 수신·실행·회신·스냅샷 재개 |
| 정지 | `ssh_run.py "sudo systemctl stop lake-executor"` | SIGTERM → 정상 종료. 수신(HTTPS 는 502)·실행·회신 모두 멈춤. **거래소 포지션·보호주문은 건드리지 않음** |
| 재시작 | `ssh_run.py "sudo systemctl restart lake-executor"` | 설정/코드 반영 |
| **HALT (권장 1순위)** | `ssh_run.py "touch /home/ubuntu/lake-executor/state/HALT"` 또는 `POST /admin/halt` | 새 신호를 전부 `rejected/OPERATOR_HALT` 로 회신. 기존 SL/TP 유지. 스냅샷·회신·대사는 계속 |
| 재개 | `ssh_run.py "rm -f /home/ubuntu/lake-executor/state/HALT"` 또는 `POST /admin/resume` | 즉시 효력 (재시작 불필요) |
| 설정 반영 | 로컬에서 `.env`/`config.json` 수정 → `python deploy/finalize.py` | 업로드(600) → `check` → 재시작 |

HALT 중 접수된 신호는 202 로 받아들여지지만 실행기에서 즉시 거부 회신된다 — lake 는 그 거부를 보고 판단해야 하므로
**HALT 걸기 전에 lake 에 알리는 것**이 원칙이다. 정지(`stop`) 중에 lake 가 보낸 신호는 Caddy 502 로 접수되지 않는다
(lake 측 재전송 정책 합의 참고, `docs/REPLY_TO_LAKE.md`).

관리 엔드포인트는 `.env` 의 `ADMIN_TOKEN`(32바이트 이상) 이 있어야 켜지고(없으면 404), 헤더 `X-Admin-Token` 으로 호출한다.
Caddy 는 `/state`, `/admin/*` 를 `ADMIN_ALLOW_CIDR`(기본 `127.0.0.1/32`) 밖에서 404 로 막으므로 기본값에서는 서버 로컬에서
`ssh_run.py "curl -s -H 'X-Admin-Token: …' -X POST http://127.0.0.1:8787/admin/halt"` 로 쓴다. 외부에서 쓰려면
`python deploy/push.py --admin-cidr <운영자IP>/32` 로 허용 CIDR 을 넣고 HTTPS 로만 호출하며, 토큰을 쉘 히스토리에 남기지 않는다.
같은 IP 에서 토큰 실패가 60초에 10회를 넘으면 그 창이 지날 때까지 401 만 돌아온다.

## 2. 로그

```bash
python deploy/ssh_run.py "journalctl -u lake-executor -n 100 --no-pager"
python deploy/ssh_run.py "journalctl -u lake-executor -f"            # 실시간, Ctrl+C
python deploy/ssh_run.py "tail -n 200 /home/ubuntu/lake-executor/state/lake-executor.log"
python deploy/ssh_run.py "sudo journalctl -u caddy -n 50 --no-pager" # HTTPS/인증서
```

- 로그에는 시크릿·원본 본문·거래소 오류 원문 전체가 남지 않는다(원문은 WARNING 에서 200자 절단, 시크릿은 `***`).
- 알림(`ALERT:` 로그 + 텔레그램 설정 시 전송)은 같은 문구를 60초에 1회만 보낸다. 알림이 오는 경우:
  `EXCHANGE_TIMEOUT`, `UNKNOWN_STATE`, `EXCHANGE_REJECTED`, `RECONCILE_REQUIRED`, `QTY_MISMATCH`, SL/TP 트리거,
  회신 전송 실패(`failed`/`conflict`), 기동 시 `ensure_account_setup`/`recover_processing` 실패.
- 접수 거부(400/401/409/410/413/415/503)는 `ingress_log` 테이블에 코드 + 본문 sha256 만 남는다.

## 3. `/healthz` 와 `/state` 읽기

```bash
python deploy/ssh_run.py "curl -s https://<host>/healthz"
# {"ok":true,"halted":false,"inconsistent":{"test":false,"live":false},"protection_missing":{"test":0,"live":0}}
python deploy/ssh_run.py "curl -s -H 'X-Admin-Token: <token>' http://127.0.0.1:8787/state"
```

`protection_missing` 이 0 이 아니면 보호가격이 설정된 open lot 에 보호주문이 빠져 있다는 뜻이다(생성 실패/거래소 쪽 취소).
30초 대사가 자동으로 다시 만들며 실패하면 `PROTECTION_FAILED` 알림이 반복된다 → Bybit 화면에서 조건부 주문을 확인한다.

`/state` 주요 키:

| 키 | 뜻 |
|---|---|
| `halted` | HALT 파일 존재 |
| `live_execution_possible` / `live_block_reason` | live 실행 가능 여부. `LIVE_DISABLED` 면 `live.enabled`, Bybit 키, `LAKE_SIGNAL_SECRET_LIVE` 중 하나가 빠짐 |
| `inconsistent{mode}` / `inconsistent_note` | 대사 불일치 플래그와 이유 (`idx1: lots=0.004 exchange=0.006` 형식) |
| `signals` | 최근 신호 30건: `status`(accepted→processing→done/rejected/error), `reason_code` |
| `reports{mode}` | 최근 회신 15건: `state`(pending/sent/duplicate/failed/conflict/unsent), `http_status`, `attempts` |
| `open_lots{mode}` | 우리 원장의 열린 lot: `qty`, `avg_entry`, `stop_loss`, `take_profit`, `protection_revision`, `protection_orders` |
| `protection_missing{mode}` | 보호주문이 빠진 open lot 의 position_id 목록 (대사가 재생성 중) |
| `ingress_rejections` | 서명 전 거부(401/400 BAD_JSON/413/415 등) 누적 횟수 — DB 에는 남기지 않는다 |
| `snapshot_positions{mode}` | 다음 스냅샷에 실릴 포지션 목록 (확인된 SL/TP 포함) |
| `exchange_positions{mode}` | 거래소 실제 포지션 `{position_idx: {size, side, avg_price, mark_price}}` (PaperExchange 면 시뮬레이터 값) |

정상 상태의 체크 포인트: `inconsistent` 전부 false, `reports` 에 `pending` 이 쌓이지 않음(`sent`/`duplicate`),
`signals` 에 `processing` 이 오래 머무르지 않음, `open_lots` 합계 = `exchange_positions` size.

## 4. 장애별 대응

### 4.1 `EXCHANGE_TIMEOUT` (회신 status `error`)
시장가를 냈지만 `exchange.fill_poll_timeout_s`(기본 10초) 안에 종결 상태를 확인하지 못했다(또는 주문 전송 자체가 네트워크
오류였고 조회도 안 됐다). **주문은 거래소에 남아 있거나 이미 체결됐을 수 있다.** 실행기는 체결된 만큼만 lot 에 반영하고 알림을
보내며, 그 주문은 `orders` 에 미확정으로 남아 **30초 대사마다 거래소에서 재확인**된다.

1. 재확인에서 종결(체결)이 확인되면 자동으로 체결 회신 → lot 반영 → 보호주문 생성 → 신호 `done`(note `LATE_VERIFIED`) + 알림
   "late fills verified". 그 사이 포지션과 lot 이 어긋나 `RECONCILE_REQUIRED` 가 떴더라도 같은 대사에서 내려간다.
2. 거래소에 끝내 없으면(20회 ≈ 10분) 주문은 `absent` 로 닫히고 알림이 온다 → lake 에 "미실행" 으로 알린다.
3. 반복되면 Bybit 상태/네트워크 문제. HALT 를 걸고 lake 에 알린다.

### 4.2 `UNKNOWN_STATE`
재시작 복구에서 `processing` 이던 신호의 주문을 거래소에서 찾지 못했거나(`get_order` 실패/없음), `protection_update`
처리 도중 죽었거나, 처리 중 예기치 않은 예외가 났다. 실행기는 그 신호를 `error/UNKNOWN_STATE` 로 닫고 알림을 보내며
**재실행하지 않는다**(이중 주문 방지).

1. `/state` 의 `signals` 에서 해당 `event_id` 의 `note` 를 본다 (`recovered: order not found on exchange` 등).
2. Bybit 앱/웹에서 해당 시각 주문·체결 내역을 확인한다. 주문이 없었다면 lake 에 "미실행" 으로 알린다 — lake 가 새
   `event_id`/`event_sequence` 로 재발신해야 한다(같은 event_id 는 200 duplicate 로 접수만 되고 재실행되지 않는다).
3. 체결이 있었는데 lot 에 없다면 §4.1 의 자동 재확인을 기다리거나(주문 행이 `unknown` 이면 대사가 재확인한다) §4.3 대로 대사한다.
   protection_update 였다면 lot 이 이미 새 revision 이면 대사가 빠진 보호주문을 자동으로 다시 만들고(`protection_missing` 확인),
   아니면 lake 가 같은 revision 을 재전송하면 된다.

### 4.2a `PROTECTION_FAILED` 알림
체결은 됐는데 SL/TP 조건부 주문 생성(또는 취소)에 실패했다. 신호는 체결대로 `done`(note `PROTECTION_FAILED`) 로 끝나고,
`/healthz` 의 `protection_missing` 이 올라간다. 30초 대사가 `lot.stop_loss/take_profit` 로 다시 만든다 — 알림이 반복되면
원인(수량 반올림, 레이트리밋, 거래소 장애)을 보고 필요하면 Bybit 화면에서 수동으로 스탑을 넣고 HALT 를 건다.
보호가격이 이미 지난 경우(예: 진입 직후 급락)는 실패가 아니라 즉시 reduceOnly 시장가로 청산되며 `auto:sl`/`auto:tp` 회신과
"already crossed" 알림이 온다.

### 4.3 불일치 플래그 (`inconsistent=true`, `RECONCILE_REQUIRED`)
30초 대사에서 **positionIdx 별 거래소 size ≠ 열린 lot 수량 합**이면 플래그가 선다. 효과: 그 mode 의 `entry/add` 는
`rejected/RECONCILE_REQUIRED`, `partial_exit/full_exit/protection_update` 는 계속 처리, **스냅샷 전송 중단**
(lake 대시보드는 90초 뒤 "갱신 지연" 으로 바뀐다). 흔한 원인: Bybit 앱에서 수동 매매, 타임아웃/복구 중 유실된 체결,
같은 계정을 다른 봇과 공유.

수동 대사 절차:
1. `/state` 의 `inconsistent_note` 로 어느 레그가 얼마나 다른지 확인 (`idx1: lots=0.004 exchange=0.006`).
2. 원인을 찾는다: Bybit 체결 내역(시각·수량·orderLinkId `lk…` 여부) vs `open_lots`. 우리가 낸 주문은 전부 `lk` 로 시작하는
   orderLinkId 를 가진다. 그 외 체결은 외부(수동/타봇) 개입이다.
3. 선택지 (하나 고른다):
   - **거래소를 lot 에 맞춘다**: Bybit 에서 초과분을 reduceOnly 로 수동 청산(또는 부족분 수동 진입). 우리 봇이 모르는
     체결이므로 회신되지 않는다 — lake 에 수동 조정 내용을 알린다.
   - **lot 을 거래소에 맞춘다**: lake 에 `partial_exit`/`full_exit`/`add` 신호를 보내달라고 요청해 봇을 통해 맞춘다
     (청산류는 불일치 중에도 처리된다. `add` 는 막히므로 부족분은 거래소 쪽에서 맞추거나, 거래소 포지션을 먼저 줄인다).
   - **원장 직접 수정(최후)**: 서비스 정지 → `state/lake.db` 백업 → `lots.qty` 를 sqlite3 로 수정 → 시작.
     보호주문 수량은 다음 `protection_update` 또는 체결 시 재설정된다.
4. `POST /admin/reconcile?mode=live` 로 즉시 재대사. 응답 `consistent:true` 면 플래그가 내려가고 스냅샷이 재개된다
   (30초 주기 대사도 자동으로 내린다).

### 4.4 회신이 `failed` / `conflict` / `pending` 으로 쌓임
- `unsent`: `LAKE_REPORT_URL_<MODE>` / `LAKE_REPORT_SECRET_<MODE>` 미설정. 저장만 된다.
- `pending` 증가 + 5xx/타임아웃 로그: lake 회신 서버 장애. 같은 mode 의 뒤 번호는 앞 번호가 끝날 때까지 기다린다.
  생성 후 `report.attempt_window_ms`(50초) 또는 `max_attempts`(3) 를 넘기면 `failed` + 알림.
- `failed`: 그 sequence 는 **영구 결번**이다(재서명하지 않는다 — ts 가 서명 창을 벗어나기 때문). lake 는 다음 연속 번호의
  완전 스냅샷으로 잔량을 다시 확인한다. 빠진 체결은 `/state` 의 `reports`/`fills` 를 lake 와 수동 대조.
- `conflict`(409): lake 가 같은 report_id 다른 본문 / sequence 역전 / 중복 fill_id 로 판단. 양쪽 원장 대조가 필요하다.
  보통 lake 측 DB 초기화나 우리 `state/` 교체 후 sequence 가 어긋났을 때 생긴다 — `state/lake.db` 를 지우지 말 것.

### 4.5 lake 가 401 / 503 / 410 을 받는다고 함
- 401: `LAKE_SIGNAL_SECRET_<MODE>` 불일치, 또는 양쪽 시계 오차 > 60초 (`sudo timedatectl` 확인), 또는 lake 가 재직렬화한
  본문으로 서명. 응답 `code` 로 구분(`BAD_SIGNATURE` / `TIMESTAMP_SKEW` / `TIMESTAMP_MISMATCH`).
- 503: 그 mode 의 수신 시크릿이 `.env` 에 없음.
- 410: `expires_at_ms` 지남. lake 의 유효시간(예시 15초)이 네트워크 지연보다 짧거나 재전송이 늦음.

## 5. 키 교체 (rotation)

**공유 시크릿(lake 와 양방향)**: 새 값을 대역 외 채널로 교환 → 양쪽이 합의한 시각에 동시에 교체. 우리 쪽은 `.env` 수정 →
`python deploy/finalize.py`(재시작 수 초, 그 사이 신호는 502 → lake 재전송 정책). 교체 직후 lake 에 `mode:test` 신호 1건을
보내 202 를 확인하고, 우리 회신이 `sent` 로 바뀌는지 `/state` 로 확인한다. 순서: TEST 먼저, 문제 없으면 LIVE.

**Bybit API 키**: 포지션이 없는 시각(또는 HALT 상태)에 한다.
1. Bybit 에서 새 키 생성(§6 권한·IP), 2. `.env` 교체 → `finalize.py`(`check` 가 새 키로 잔고·포지션 조회에 성공해야 서비스가 뜬다),
3. `/state` 의 `exchange_positions` 와 `open_lots` 일치 확인, 4. 옛 키 삭제. 열린 보호주문은 계정 소속이므로 키 교체에 영향 없다.

**ADMIN_TOKEN / 텔레그램 토큰**: `.env` 교체 → `finalize.py`.

## 6. Bybit API 키 권한 · IP 화이트리스트

- lake 전용 **서브계정**(UTA) 을 쓴다. `account_scope: lake_dedicated_BTCUSDT` 로 회신하므로 **다른 봇·수동 매매와 계정을
  공유하면 안 된다**(공유하면 대사가 계속 불일치하고 `complete:true` 스냅샷을 보낼 수 없다).
- 권한: **Contract(통합거래) 주문·포지션 읽기/쓰기** 만. 출금·이체·서브계정 관리 권한은 **없음**.
- **IP 제한 = 서버 Elastic IP** 한 개. 로컬 PC IP 는 넣지 않는다(로컬 `check` 가 Bybit 단계에서 실패하는 게 정상).
- 기동 시 `ensure_account_setup` 이 헤지 모드(`switch_position_mode` 3)·레버리지·마진 모드를 맞춘다. "not modified"
  응답은 무시한다. 포지션이 열려 있으면 모드 전환이 거부될 수 있으므로 **첫 기동은 포지션 없는 상태**에서 한다.
- 키 만료일을 두었다면 만료 1주 전 §5 로 교체.

## 7. 실거래 전환 체크리스트 (go-live)

단계마다 `/state`·로그·lake 대시보드를 대조하고, 다음 단계로 넘어갈 때만 설정을 바꾼다.

1. **TEST 기록 전용** — `live.enabled=false`, `test.simulate_fills=false`, `.env` 에 `LAKE_SIGNAL_SECRET_TEST` +
   `LAKE_REPORT_SECRET_TEST`/`LAKE_REPORT_URL_TEST`.
   lake 가 정상/잘못된 서명/만료/본문 변경을 보내 202/401/410/409 가 맞게 나오는지, 우리 `acknowledged` 회신이 lake 에
   202 로 접수되는지 확인. `python -m lake_executor simulate --base-url https://<host>` 로도 같은 흐름을 재현할 수 있다.
2. **TEST simulate_fills + lake 실제 신호** — `test.simulate_fills=true` → `finalize.py`. lake 의 entry→add→partial_exit→
   protection_update→full_exit 와 재전송·순서 역전·누락·잘못된 수량·헤지 두 레그 시험(`docs/REPLY_TO_LAKE.md` §8).
   TEST 회신이 lake 의 LIVE 화면에 섞이지 않는지, 재시작 뒤 duplicate 가 유지되는지 확인.
3. **Bybit 테스트넷** — `testnet=true`, 테스트넷 키로 `live.enabled=true` + `LAKE_SIGNAL_SECRET_LIVE`(테스트넷 전용 값).
   실제 주문 경로(`place_order`/조건부/취소/`executions`)와 `ensure_account_setup` 을 검증. 끝나면 테스트넷 키 제거.
4. **LIVE 소액** — `testnet=false`, 실키(§6), `live.enabled=true`, `guards.max_order_qty_btc`/`max_leg_qty_btc` 를
   **처음엔 최소 수량 근처로**(예: 0.002 / 0.004), `max_entry_slippage_pct` 1.5 이하. `finalize.py` → `check` 통과 확인.
   lake 와 **시작 시각**을 합의하고 미래 신호부터 받는다(과거 백테스트 신호 금지). 첫 사이클을 사람이 지켜보며
   `/state` 와 Bybit 화면을 대조한 뒤 가드를 단계적으로 올린다.
5. 운영 중 상시: `/healthz` 외부 모니터링, 텔레그램 알림 수신 확인, 주 1회 `state/lake.db` 백업
   (`ssh_run.py "sqlite3 state/lake.db '.backup state/lake-$(date +%F).db'"`).

## 8. 재시작 복구 의미 (restart recovery)

- `accepted` 상태 신호: 재시작 뒤 그대로 FIFO 로 실행되지만 **실행 시점에 `expires_at_ms` 를 다시 검사**하므로 정지 중에
  만료된 신호는 `rejected/EXPIRED` 로만 회신되고 실행되지 않는다. 그래도 장시간 정지 뒤에는 **HALT 를 켠 채 시작**해 `/state` 를
  보고 재개하는 것이 안전하다(HALT 중 실행되면 `rejected/OPERATOR_HALT`).
- `processing` 상태 신호(실행 중 죽음): **재실행하지 않는다.** entry/add/partial_exit/full_exit 는 `orderLinkId` 로 거래소
  주문을 조회해 있으면 체결 수집을 이어 마무리(회신·lot 반영), 없으면 `error/UNKNOWN_STATE` + 알림(§4.2).
  `protection_update` 는 판정 불가 → `UNKNOWN_STATE`. test 기록 전용은 `TEST_RECORD_ONLY` 로 닫는다.
- 회신 `pending`: 재시작 뒤 같은 바이트로 이어서 전송(재서명 없음). 생성 후 50초를 넘긴 것은 `failed` 로 닫힌다.
  sequence 는 `meta` 에 영속되어 재시작 뒤에도 이어진다.
- 보호주문: 거래소에 그대로 남아 있다. 첫 대사(기동 2초 뒤 첫 스냅샷)에서 정지 중 체결된 SL/TP 를 수집해
  `auto:sl:…`/`auto:tp:…` 회신을 만들고 lot 을 줄인다.
- `systemd Restart=always, RestartSec=5` 이므로 크래시 시 자동 재시작되지만, 5분 안에 5회를 넘기면(`StartLimitBurst`) 멈추고,
  설정 오류(exit 2) 는 재시작하지 않는다. `journalctl` 로 원인 확인 → 수정 → `sudo systemctl reset-failed lake-executor && sudo systemctl start lake-executor`.
- 기동 시 `ensure_account_setup` 은 같은 설정을 이미 적용했으면(원장 `meta`) 거래소 쓰기를 반복하지 않는다. 포지션 모드/레버리지를
  거래소 화면에서 바꿨다면 `config.json` 값을 바꾸거나 `meta` 의 `account_setup:live` 를 지워 다시 적용시킨다.
- 배포 스크립트의 SSH 는 `deploy/known_hosts` 의 호스트 키만 신뢰한다. 인스턴스를 재생성해 키가 바뀌면 "HOST KEY MISMATCH" 로
  거부되므로 그 줄을 지우고 지문을 확인한 뒤 `--trust-new-host-key` 로 다시 기록한다.
