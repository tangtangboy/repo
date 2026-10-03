"""pytest 공용 픽스처/헬퍼 (ARCHITECTURE.md §9).

전부 오프라인·결정적이다. 실키/실네트워크는 절대 쓰지 않는다.
  - settings : 임시 config.json + .env 로 config.load 한 Settings (state_dir 은 tmp_path 안).
               1단계 형식(accounts 없음) → 계정 'bybit' 하나 (settings.accounts[0]).
  - store    : 임시 SQLite Store (스키마 v2, 계정 컬럼)
  - paper    : PaperExchange(settings.accounts[0], 초기가 86000)
  - paper_exchanges : {account_name: PaperExchange} — 단일 계정이면 {"bybit": paper}
  - executor : Executor(settings, store, {"test": {account: paper}, "live": {}}, reporter, alerts)
               (2단계: 모드별 → 계정 이름별 거래소. 실행기 리팩터링 시 이 모양에 맞추거나 여기만 바꾼다)
  - multi_settings : 3계정(bybit/okx/toobit, 전부 Paper 로 실행, routing fanout) Settings
  - multi_store / multi_paper_exchanges : 위 설정용 Store 와 계정별 PaperExchange dict
  - fake_client : 회신 전송용 가짜 httpx.Client (posted 본문/헤더 기록, 응답 코드/예외 큐)
  - reporter / alerts / executor / app / client
  - make_signal(**overrides) : 계약 example 기반 유효 신호 dict (fresh ts/expires, 고유 id)
  - sign_body(dict, secret)  : (raw_bytes, headers)
"""
from __future__ import annotations

import itertools
import json
import uuid
from types import SimpleNamespace
from typing import Any, Callable

import pytest
from fastapi.testclient import TestClient

from lake_executor import auth, config
from lake_executor.exchange import PaperExchange
from lake_executor.executor import Executor
from lake_executor.receiver import create_app
from lake_executor.reporter import Reporter
from lake_executor.schemas import Signal
from lake_executor.store import Store
from lake_executor.util import now_ms

# --------------------------------------------------------------------------- #
# 상수 (테스트 전용 값 — 실제 시크릿 아님)
# --------------------------------------------------------------------------- #
TEST_SIGNAL_SECRET = "test-signal-secret-0123456789abcdef-0123456789"
LIVE_SIGNAL_SECRET = "live-signal-secret-0123456789abcdef-0123456789"
TEST_REPORT_SECRET = "test-report-secret-0123456789abcdef-0123456789"
LIVE_REPORT_SECRET = "live-report-secret-0123456789abcdef-0123456789"
REPORT_URL_TEST = "http://lake.test/report"
ADMIN_TOKEN = "admin-token-for-tests-only-0123456789abcdef"
PAPER_PRICE = 86000.0
SIGNAL_PATH = "/lake/signal"

DEFAULT_CONFIG: dict[str, Any] = {
    "symbol": "BTCUSDT",
    "category": "linear",
    "position_mode": "hedge",
    "leverage": 5,
    "margin_mode": "isolated",
    "testnet": False,
    "listen": {"host": "127.0.0.1", "port": 8787},
    "signal_path": SIGNAL_PATH,
    "max_body_bytes": 65536,
    "max_clock_skew_ms": 60000,
    "live": {"enabled": False},
    "test": {"simulate_fills": True},
    "guards": {
        "max_order_qty_btc": 0.05,
        "max_leg_qty_btc": 0.2,
        "max_entry_slippage_pct": 1.5,
        "protection_trigger_by": "MarkPrice",
    },
    "report": {"snapshot_interval_ms": 30000, "http_timeout_s": 5, "max_attempts": 3, "attempt_window_ms": 50000},
    "exchange": {"fill_poll_timeout_s": 2, "fill_poll_interval_s": 0.05, "reconcile_interval_ms": 30000},
    "log_file": "",
}

DEFAULT_ENV: dict[str, str] = {
    "LAKE_SIGNAL_SECRET_TEST": TEST_SIGNAL_SECRET,
    "LAKE_SIGNAL_SECRET_LIVE": LIVE_SIGNAL_SECRET,
    "LAKE_REPORT_SECRET_TEST": TEST_REPORT_SECRET,
    "LAKE_REPORT_SECRET_LIVE": LIVE_REPORT_SECRET,
    "LAKE_REPORT_URL_TEST": REPORT_URL_TEST,
    "ADMIN_TOKEN": ADMIN_TOKEN,
}

ALL_SECRETS = (TEST_SIGNAL_SECRET, LIVE_SIGNAL_SECRET, TEST_REPORT_SECRET, LIVE_REPORT_SECRET, ADMIN_TOKEN)
DEFAULT_ACCOUNT = "bybit"

MULTI_ACCOUNTS: list[dict[str, Any]] = [
    {"name": "bybit", "exchange": "bybit", "enabled": True, "symbol": "BTCUSDT", "position_mode": "hedge",
     "leverage": 5, "margin_mode": "isolated", "testnet": False, "env_prefix": "BYBIT", "qty_multiplier": 1.0, "report": True},
    {"name": "okx", "exchange": "okx", "enabled": True, "symbol": "BTC-USDT-SWAP", "position_mode": "hedge",
     "leverage": 5, "margin_mode": "isolated", "testnet": False, "env_prefix": "OKX", "qty_multiplier": 1.0, "report": False},
    {"name": "toobit", "exchange": "toobit", "enabled": True, "symbol": "BTC-SWAP-USDT", "position_mode": "hedge",
     "leverage": 5, "margin_mode": "isolated", "testnet": False, "env_prefix": "TOOBIT", "qty_multiplier": 1.0, "report": False},
]

_counter = itertools.count(1)
_settings_counter = itertools.count(1)


# --------------------------------------------------------------------------- #
# Settings 빌더
# --------------------------------------------------------------------------- #
def _deep_merge(base: dict, over: dict | None) -> dict:
    out = {k: (dict(v) if isinstance(v, dict) else v) for k, v in base.items()}
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def build_settings(root, config_overrides: dict | None = None, env_overrides: dict | None = None,
                   drop_env: tuple[str, ...] = ()):
    """임시 디렉터리 root 에 config.json/.env 를 쓰고 config.load 로 Settings 를 만든다."""
    root.mkdir(parents=True, exist_ok=True)
    cfg = _deep_merge(DEFAULT_CONFIG, config_overrides)
    cfg["state_dir"] = str(root / "state")
    env = dict(DEFAULT_ENV)
    env.update(env_overrides or {})
    for k in drop_env:
        env.pop(k, None)
    cfg_path = root / "config.json"
    env_path = root / ".env"
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=1), encoding="utf-8")
    env_path.write_text("".join(f"{k}={v}\n" for k, v in env.items()), encoding="utf-8")
    return config.load(str(cfg_path), str(env_path))


@pytest.fixture
def settings_factory(tmp_path):
    """settings_factory(config_overrides=None, env_overrides=None, drop_env=()) → Settings (호출마다 새 디렉터리)."""

    def make(config_overrides: dict | None = None, env_overrides: dict | None = None, drop_env: tuple[str, ...] = ()):
        root = tmp_path / f"s{next(_settings_counter)}"
        return build_settings(root, config_overrides, env_overrides, drop_env)

    return make


@pytest.fixture
def settings(settings_factory):
    return settings_factory()


@pytest.fixture
def multi_settings(settings_factory):
    """3개 Paper 계정(bybit/okx/toobit, 모두 enabled, routing fanout). 실키 없음 → live 는 LIVE_DISABLED."""
    return settings_factory(config_overrides={"routing": "fanout", "accounts": [dict(a) for a in MULTI_ACCOUNTS]})


# --------------------------------------------------------------------------- #
# 가짜 객체
# --------------------------------------------------------------------------- #
class FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


class FakeClient:
    """httpx.Client 호환 최소 구현. post 호출을 기록하고, 큐에 넣은 상태코드/예외를 순서대로 돌려준다(비면 202)."""

    def __init__(self, default_status: int = 202):
        self.posts: list[dict] = []
        self.queue_items: list[Any] = []
        self.default_status = default_status
        self.closed = False

    def queue(self, *items: Any) -> "FakeClient":
        self.queue_items.extend(items)
        return self

    def post(self, url: str, content: bytes | None = None, headers: dict | None = None,
             timeout: float | None = None, **kw) -> FakeResponse:
        self.posts.append({"url": url, "content": content, "headers": dict(headers or {}), "timeout": timeout, **kw})
        item = self.queue_items.pop(0) if self.queue_items else self.default_status
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, type) and issubclass(item, BaseException):
            raise item("fake transport error")
        return FakeResponse(int(item))

    def close(self) -> None:
        self.closed = True


class AlertsStub:
    """ops.Alerts 대체: 메시지만 모은다."""

    def __init__(self):
        self.messages: list[str] = []

    def send(self, text: str) -> bool:
        self.messages.append(str(text))
        return True

    def contains(self, needle: str) -> bool:
        return any(needle in m for m in self.messages)


@pytest.fixture
def store(settings):
    s = Store(settings.db_path)
    yield s
    s.close()


@pytest.fixture
def paper(settings):
    return PaperExchange(settings.accounts[0], price=PAPER_PRICE)


@pytest.fixture
def paper_exchanges(settings, paper):
    """계정 이름 → PaperExchange. 단일 계정 설정이면 {"bybit": paper}."""
    out = {settings.accounts[0].name: paper}
    for a in settings.accounts[1:]:
        out[a.name] = PaperExchange(a, price=PAPER_PRICE)
    return out


@pytest.fixture
def multi_store(multi_settings):
    s = Store(multi_settings.db_path)
    yield s
    s.close()


@pytest.fixture
def multi_paper_exchanges(multi_settings):
    return {a.name: PaperExchange(a, price=PAPER_PRICE) for a in multi_settings.accounts}


@pytest.fixture
def fake_client():
    return FakeClient()


@pytest.fixture
def alerts():
    return AlertsStub()


@pytest.fixture
def reporter(settings, store, alerts, fake_client):
    r = Reporter(settings, store, alerts, client=fake_client)
    yield r
    r.close()


@pytest.fixture
def executor(settings, store, paper_exchanges, reporter, alerts):
    return Executor(settings, store, {"test": dict(paper_exchanges), "live": {}}, reporter, alerts)


@pytest.fixture
def services(executor, reporter, alerts):
    return SimpleNamespace(executor=executor, reporter=reporter, alerts=alerts)


@pytest.fixture
def app(settings, store, services):
    return create_app(settings, store, services)


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------- #
# 신호 생성 / 서명 / 접수 헬퍼
# --------------------------------------------------------------------------- #
def make_signal(**overrides) -> dict:
    """계약 outgoing_signal_proposal.example 기반 유효 신호. ts=now, expires=now+15s, event/position id 고유."""
    n = next(_counter)
    tag = uuid.uuid4().hex[:8]
    ts = now_ms()
    d: dict[str, Any] = {
        "schema_version": 1,
        "strategy_name": "lake(가명) strategt",
        "strategy": "overheat",
        "mode": "test",
        "event_id": f"event-{n}-{tag}",
        "event_sequence": 1,
        "ts": ts,
        "expires_at_ms": ts + 15000,
        "exchange": "Bybit",
        "category": "linear",
        "symbol": "BTCUSDT",
        "position_id": f"position-{n}-{tag}",
        "leg": "short",
        "position_idx": 2,
        "action": "entry",
        "qty_btc": 0.002,
        "expected_qty_btc_after": 0.002,
        "reference_price": 86000,
        "protection_revision": 1,
        "stop_loss": None,
        "take_profit": None,
    }
    d.update(overrides)
    return d


def sign_body(d: dict, secret: str) -> tuple[bytes, dict[str, str]]:
    """dict → (raw_bytes, 서명 헤더). X-Timestamp 는 본문 ts."""
    raw = json.dumps(d, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return raw, auth.headers_for(raw, secret, int(d["ts"]))


def ingest(store: Store, d: dict) -> str:
    """수신기를 거치지 않고 store 에 직접 접수 (insert_signal 결과 반환)."""
    raw, _ = sign_body(d, TEST_SIGNAL_SECRET)
    return store.insert_signal(Signal.model_validate(d), raw, now_ms())


def run_signal(executor: Executor, store: Store, d: dict) -> dict:
    """접수 → 실행기 run_once → 처리된 signals 행 반환."""
    res = ingest(store, d)
    assert res == "new", f"ingest returned {res}"
    assert executor.run_once() is True
    row = store.get_signal(d["event_id"])
    assert row is not None
    return row


def load_reports(store: Store, mode: str, account: str = DEFAULT_ACCOUNT) -> list[dict]:
    """(mode, account) 의 pending 회신 본문을 sequence 순으로 파싱 (전송하지 않은 상태에서 전체 이력 확인용)."""
    out = []
    for row in store.pending_reports(mode, account, limit=10000):
        body = row["body"]
        if isinstance(body, memoryview):
            body = body.tobytes()
        out.append(json.loads(bytes(body).decode("utf-8")))
    return out


def reports_after(store: Store, mode: str, seq: int, account: str = DEFAULT_ACCOUNT) -> list[dict]:
    return [r for r in load_reports(store, mode, account) if r["sequence"] > seq]


def last_seq(store: Store, mode: str, account: str = DEFAULT_ACCOUNT) -> int:
    rs = load_reports(store, mode, account)
    return rs[-1]["sequence"] if rs else 0


def execution_statuses(reports: list[dict]) -> list[str]:
    """회신 목록 → ['acknowledged','submitted','filled','snapshot', ...] 형태의 종류/상태 열."""
    out = []
    for r in reports:
        if r["kind"] == "snapshot":
            out.append("snapshot")
        else:
            out.append(r["execution"]["status"])
    return out


@pytest.fixture(name="make_signal")
def _make_signal_fixture() -> Callable[..., dict]:
    return make_signal


@pytest.fixture(name="sign_body")
def _sign_body_fixture() -> Callable[[dict, str], tuple[bytes, dict[str, str]]]:
    return sign_body
