"""설정 로더: config.json(비밀 아님) + .env(비밀). 검증 후 Settings 로 고정.

2단계(ARCHITECTURE_MULTI_EXCHANGE.md §1): 계정 목록(`accounts`) + 라우팅(`routing`).
  - `accounts` 가 없으면 최상위 symbol/position_mode/... + BYBIT_* env 로 `bybit` 계정 하나를 만든다 (1단계 호환).
  - 계정 키: `{env_prefix}_API_KEY/_API_SECRET/_API_PASSPHRASE`.
  - 회신 URL/시크릿: 기본 `LAKE_REPORT_URL_{MODE}` / `LAKE_REPORT_SECRET_{MODE}`,
    계정별 덮어쓰기 `LAKE_REPORT_URL_{MODE}_{NAME}` / `LAKE_REPORT_SECRET_{MODE}_{NAME}`
    (NAME = util.env_suffix(name): 대문자, 영숫자 외는 '_').
"""
from __future__ import annotations

import os
import re
import urllib.parse
from dataclasses import dataclass, field

from .util import env_suffix, parse_env_file, read_json

PLACEHOLDER = "PUT_"
MODES = ("test", "live")
EXCHANGES = ("bybit", "okx", "toobit")
EXCHANGE_DISPLAY = {"bybit": "Bybit", "okx": "OKX", "toobit": "Toobit"}
DEFAULT_SYMBOL = {"bybit": "BTCUSDT", "okx": "BTC-USDT-SWAP", "toobit": "BTC-SWAP-USDT"}
ROUTINGS = ("fanout", "by_exchange")
ACCOUNT_NAME_RE = re.compile(r"^[a-z0-9_-]{1,32}$")
ENV_PREFIX_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,31}$")
LAKE_SYMBOL = "BTCUSDT"   # lake 표준 심볼 (회신 symbol, 신호 symbol 검증)


class ConfigError(ValueError):
    pass


def _real_keys(key: str, secret: str) -> bool:
    return bool(key and secret) and PLACEHOLDER not in key and PLACEHOLDER not in secret


@dataclass
class Secrets:
    bybit_api_key: str = ""                              # 1단계 호환 (= BYBIT_* env). 계정별 키는 AccountSettings 에
    bybit_api_secret: str = ""
    signal_secret: dict = field(default_factory=dict)   # mode -> secret (수신 검증)
    report_secret: dict = field(default_factory=dict)   # mode -> secret (회신 서명, 기본값)
    report_url: dict = field(default_factory=dict)      # mode -> URL (비어 있으면 전송 안 함, 기본값)
    admin_token: str = ""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    database_url: str = ""                               # 비어 있으면 SQLite(state/lake.db); postgres://… 면 Postgres(Supabase)

    def has_real_bybit_keys(self) -> bool:
        return _real_keys(self.bybit_api_key, self.bybit_api_secret)


@dataclass
class AccountSettings:
    """실행 대상 계정 하나. 거래소 래퍼는 이 객체만 받는다 (build_exchange(account))."""
    name: str = "bybit"
    exchange: str = "bybit"               # bybit | okx | toobit
    enabled: bool = True
    symbol: str = "BTCUSDT"               # 거래소 네이티브 심볼
    position_mode: str = "hedge"          # hedge | one_way (toobit 은 hedge 만)
    leverage: int = 5
    margin_mode: str = "isolated"         # isolated | cross | ""(변경 안 함)
    testnet: bool = False
    env_prefix: str = "BYBIT"
    qty_multiplier: float = 1.0           # 신호 qty_btc × 배수 = 계정 수량 (expected_qty_btc_after 비교도 같은 배수)
    report: bool = True                   # False 면 회신을 unsent 로 저장만
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""              # OKX 필수
    report_url: dict = field(default_factory=dict)      # mode -> URL (기본값 + 계정별 덮어쓰기 적용 결과)
    report_secret: dict = field(default_factory=dict)   # mode -> secret

    category: str = "linear"              # Bybit v5 category (다른 거래소는 무시)
    seed_usdt: float | None = None        # 성과(ROI) 기준 시드. 없으면 첫 자산 스냅샷을 시드로 본다 (metrics.py)

    # ---- 파생 ----
    @property
    def display_name(self) -> str:
        """회신 본문 `exchange` 값: Bybit | OKX | Toobit."""
        return EXCHANGE_DISPLAY.get(self.exchange, self.exchange)

    @property
    def account_scope(self) -> str:
        """회신 snapshot `account_scope` (§5): bybit 는 1단계 값 그대로, 그 외 lake_dedicated_{EXCHANGE}_BTCUSDT."""
        if self.exchange == "bybit":
            return f"lake_dedicated_{LAKE_SYMBOL}"
        return f"lake_dedicated_{self.display_name.upper()}_{LAKE_SYMBOL}"

    def has_real_keys(self) -> bool:
        if not _real_keys(self.api_key, self.api_secret):
            return False
        if self.exchange == "okx":
            return bool(self.api_passphrase) and PLACEHOLDER not in self.api_passphrase
        return True

    def allowed_position_idx(self) -> set[int]:
        return {1, 2} if self.position_mode == "hedge" else {0}

    def matches_exchange(self, exchange_name: str | None) -> bool:
        """신호 exchange("Bybit"/"OKX"/"Toobit", 대소문자 무시) 가 이 계정의 거래소인가."""
        return bool(exchange_name) and str(exchange_name).lower() == self.exchange


@dataclass
class Settings:
    # 1단계 호환 최상위 값 (accounts 가 없을 때 bybit 계정의 바탕; 있으면 각 계정의 기본값)
    symbol: str = "BTCUSDT"
    category: str = "linear"
    position_mode: str = "hedge"          # hedge | one_way
    leverage: int = 5
    margin_mode: str = "isolated"         # isolated | cross | ""(변경 안 함)
    testnet: bool = False

    listen_host: str = "127.0.0.1"
    listen_port: int = 8787
    signal_path: str = "/lake/signal"
    max_body_bytes: int = 65536
    max_clock_skew_ms: int = 60000

    live_enabled: bool = False
    test_simulate_fills: bool = False

    max_order_qty_btc: float = 0.05
    max_leg_qty_btc: float = 0.2
    max_entry_slippage_pct: float = 1.5
    protection_trigger_by: str = "MarkPrice"

    snapshot_interval_ms: int = 30000
    report_http_timeout_s: float = 5.0
    report_max_attempts: int = 3
    report_attempt_window_ms: int = 50000

    fill_poll_timeout_s: float = 10.0
    fill_poll_interval_s: float = 0.5
    reconcile_interval_ms: int = 30000

    state_dir: str = "state"
    log_file: str = "state/lake-executor.log"

    routing: str = "fanout"               # fanout | by_exchange
    accounts: list = field(default_factory=list)   # list[AccountSettings]

    # 계정 트레이드 히스토리 적재 (history.py): 증분 동기화 주기, 자산 스냅샷 주기, 첫 동기화 때 거슬러 올라갈 일수
    history_enabled: bool = True
    history_sync_interval_s: int = 60
    history_equity_interval_s: int = 300
    history_backfill_days: int = 30

    db_schema: str = "lake_executor"      # Postgres 원장 스키마 (DATABASE_URL 이 있을 때만)
    # 만료(expires_at_ms 경과) 뒤 도착/처리되는 신호 중 그래도 실행할 action. 진입류(entry/add) 는 절대 넣지 않는 것을 권장:
    # 오래된 가격으로 새 포지션을 열면 안 되지만, 청산/보호가격 변경은 늦게라도 적용하는 편이 안전하다 (서버 끊김 뒤 재연결 복구).
    expired_actions_execute: list = field(default_factory=lambda: ["partial_exit", "full_exit", "protection_update"])

    secrets: Secrets = field(default_factory=Secrets)

    # ---- 파생 ----
    @property
    def db_path(self) -> str:
        return os.path.join(self.state_dir, "lake.db")

    @property
    def db_url(self) -> str:
        """DATABASE_URL (.env). 비어 있으면 SQLite."""
        return str(getattr(self.secrets, "database_url", "") or "").strip()

    @property
    def ledger_target(self) -> str:
        """Store() 에 넘길 대상: Postgres URL 또는 SQLite 경로."""
        return self.db_url or self.db_path

    @property
    def halt_file(self) -> str:
        return os.path.join(self.state_dir, "HALT")

    def allowed_position_idx(self) -> set[int]:
        """수신 검증용: enabled 계정들의 허용 position_idx 합집합 (없으면 전체 계정, 그것도 없으면 최상위 position_mode)."""
        accts = self.enabled_accounts() or list(self.accounts)
        if not accts:
            return {1, 2} if self.position_mode == "hedge" else {0}
        out: set[int] = set()
        for a in accts:
            out |= a.allowed_position_idx()
        return out

    # ---- 계정 ----
    def account(self, name: str) -> AccountSettings:
        for a in self.accounts:
            if a.name == name:
                return a
        raise KeyError(f"unknown account: {name}")

    def account_or_none(self, name: str) -> AccountSettings | None:
        return next((a for a in self.accounts if a.name == name), None)

    def enabled_accounts(self) -> list[AccountSettings]:
        return [a for a in self.accounts if a.enabled]

    def legacy_account_name(self) -> str:
        """1단계(단일 Bybit) 원장 행이 귀속될 계정 이름: 첫 bybit 계정, 없으면 첫 계정 (Store(legacy_account=) 에 넘긴다).
        1단계 DB 의 lots/orders/fills/reports 는 전부 그 Bybit 계정의 것이므로, 운영자가 그 계정을 'bybit' 가 아닌
        이름으로 올려도 행이 고아가 되지 않게 한다."""
        for a in self.accounts:
            if a.exchange == "bybit":
                return a.name
        return self.accounts[0].name if self.accounts else "bybit"

    def route_accounts(self, signal_exchange: str | None) -> list[AccountSettings]:
        """라우팅 규칙으로 고른 enabled 계정 (fanout: 전부, by_exchange: 신호 exchange 와 일치하는 계정만, 대소문자 무시)."""
        accts = self.enabled_accounts()
        if self.routing == "by_exchange":
            accts = [a for a in accts if a.matches_exchange(signal_exchange)]
        return accts

    def has_real_keys(self, account: "AccountSettings | str | None" = None) -> bool:
        """account 가 None 이면 enabled 계정 중 하나라도 실키가 있는가."""
        if account is None:
            return any(a.has_real_keys() for a in self.enabled_accounts())
        a = self.account(account) if isinstance(account, str) else account
        return a.has_real_keys()

    def live_execution_possible(self, account: "AccountSettings | str | None" = None) -> tuple[bool, str]:
        """실주문 가능 여부와 아니면 그 이유(reason_code).
        account=None 이면 'enabled 계정 중 하나라도 가능한가' (1단계 호출 호환)."""
        if not self.live_enabled:
            return False, "LIVE_DISABLED"
        if not self.secrets.signal_secret.get("live"):
            return False, "LIVE_DISABLED"
        if account is None:
            if not any(a.has_real_keys() for a in self.enabled_accounts()):
                return False, "LIVE_DISABLED"
            return True, ""
        a = self.account_or_none(account) if isinstance(account, str) else account
        if a is None:
            return False, "NO_TARGET_ACCOUNT"
        if not a.enabled:
            return False, "ACCOUNT_DISABLED"
        if not a.has_real_keys():
            return False, "LIVE_DISABLED"
        return True, ""


def _get(d: dict, path: str, default):
    cur = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _account_from_cfg(s: Settings, raw: dict, env: dict, sec: Secrets) -> AccountSettings:
    if not isinstance(raw, dict):
        raise ConfigError("accounts[] entries must be objects")
    exchange = str(raw.get("exchange", "bybit")).lower()
    name = str(raw.get("name", exchange))

    def _cast(field: str, conv, default):
        """숫자/불리언 필드 변환 실패를 ConfigError 로 (계정·필드 이름 포함). bool 은 JSON true/false 만 받는다."""
        v = raw.get(field, default)
        try:
            if conv is bool:
                if isinstance(v, bool):
                    return v
                if isinstance(v, (int, float)) and v in (0, 1):
                    return bool(v)
                raise ValueError("expected true|false")
            return conv(v)
        except (TypeError, ValueError) as e:
            raise ConfigError(f"account {name}: {field}={v!r} is not a valid {conv.__name__} ({e})") from None

    a = AccountSettings(
        name=name,
        exchange=exchange,
        enabled=_cast("enabled", bool, True),
        symbol=str(raw.get("symbol", DEFAULT_SYMBOL.get(exchange, s.symbol))),
        position_mode=str(raw.get("position_mode", s.position_mode)),
        leverage=_cast("leverage", int, s.leverage),
        margin_mode=str(raw.get("margin_mode", s.margin_mode) or ""),
        testnet=_cast("testnet", bool, s.testnet),
        env_prefix=str(raw.get("env_prefix", exchange.upper())),
        qty_multiplier=_cast("qty_multiplier", float, 1.0),
        report=_cast("report", bool, True),
        category=str(raw.get("category", s.category)),
        seed_usdt=(_cast("seed_usdt", float, 0.0) if raw.get("seed_usdt") not in (None, "") else None),
    )
    if a.seed_usdt is not None and a.seed_usdt <= 0:
        raise ConfigError(f"account {name}: seed_usdt must be > 0 (or omitted)")
    _fill_account_env(a, env, sec)
    return a


def _fill_account_env(a: AccountSettings, env: dict, sec: Secrets) -> None:
    p = a.env_prefix
    a.api_key = env.get(f"{p}_API_KEY", "")
    a.api_secret = env.get(f"{p}_API_SECRET", "")
    a.api_passphrase = env.get(f"{p}_API_PASSPHRASE", "")
    suffix = env_suffix(a.name)
    for m in MODES:
        up = m.upper()
        url = env.get(f"LAKE_REPORT_URL_{up}_{suffix}", "") or sec.report_url.get(m, "")
        secret = env.get(f"LAKE_REPORT_SECRET_{up}_{suffix}", "") or sec.report_secret.get(m, "")
        if url:
            a.report_url[m] = url
        if secret:
            a.report_secret[m] = secret


def load(config_path: str = "config.json", env_path: str = ".env", env_override: dict | None = None) -> Settings:
    cfg = read_json(config_path) if config_path and os.path.exists(config_path) else {}
    env = parse_env_file(env_path)
    if env_override:
        env.update(env_override)
    s = Settings()
    s.symbol = str(_get(cfg, "symbol", s.symbol))
    s.category = str(_get(cfg, "category", s.category))
    s.position_mode = str(_get(cfg, "position_mode", s.position_mode))
    s.leverage = int(_get(cfg, "leverage", s.leverage))
    s.margin_mode = str(_get(cfg, "margin_mode", s.margin_mode) or "")
    s.testnet = bool(_get(cfg, "testnet", s.testnet))
    s.listen_host = str(_get(cfg, "listen.host", s.listen_host))
    s.listen_port = int(_get(cfg, "listen.port", s.listen_port))
    s.signal_path = str(_get(cfg, "signal_path", s.signal_path))
    s.max_body_bytes = int(_get(cfg, "max_body_bytes", s.max_body_bytes))
    s.max_clock_skew_ms = int(_get(cfg, "max_clock_skew_ms", s.max_clock_skew_ms))
    s.live_enabled = bool(_get(cfg, "live.enabled", s.live_enabled))
    s.test_simulate_fills = bool(_get(cfg, "test.simulate_fills", s.test_simulate_fills))
    s.max_order_qty_btc = float(_get(cfg, "guards.max_order_qty_btc", s.max_order_qty_btc))
    s.max_leg_qty_btc = float(_get(cfg, "guards.max_leg_qty_btc", s.max_leg_qty_btc))
    s.max_entry_slippage_pct = float(_get(cfg, "guards.max_entry_slippage_pct", s.max_entry_slippage_pct))
    s.protection_trigger_by = str(_get(cfg, "guards.protection_trigger_by", s.protection_trigger_by))
    s.snapshot_interval_ms = int(_get(cfg, "report.snapshot_interval_ms", s.snapshot_interval_ms))
    s.report_http_timeout_s = float(_get(cfg, "report.http_timeout_s", s.report_http_timeout_s))
    s.report_max_attempts = int(_get(cfg, "report.max_attempts", s.report_max_attempts))
    s.report_attempt_window_ms = int(_get(cfg, "report.attempt_window_ms", s.report_attempt_window_ms))
    s.fill_poll_timeout_s = float(_get(cfg, "exchange.fill_poll_timeout_s", s.fill_poll_timeout_s))
    s.fill_poll_interval_s = float(_get(cfg, "exchange.fill_poll_interval_s", s.fill_poll_interval_s))
    s.reconcile_interval_ms = int(_get(cfg, "exchange.reconcile_interval_ms", s.reconcile_interval_ms))
    s.state_dir = str(_get(cfg, "state_dir", s.state_dir))
    s.log_file = str(_get(cfg, "log_file", s.log_file) or "")
    s.routing = str(_get(cfg, "routing", s.routing))
    s.db_schema = str(_get(cfg, "database.schema", s.db_schema) or "lake_executor")
    s.history_enabled = bool(_get(cfg, "history.enabled", s.history_enabled))
    s.history_sync_interval_s = int(_get(cfg, "history.sync_interval_s", s.history_sync_interval_s))
    s.history_equity_interval_s = int(_get(cfg, "history.equity_interval_s", s.history_equity_interval_s))
    s.history_backfill_days = int(_get(cfg, "history.backfill_days", s.history_backfill_days))
    if s.history_sync_interval_s < 10 or s.history_equity_interval_s < 10 or s.history_backfill_days < 1:
        raise ConfigError("history.sync_interval_s/equity_interval_s must be >= 10 and backfill_days >= 1")
    eae = _get(cfg, "guards.expired_actions_execute", None)
    if eae is not None:
        if not isinstance(eae, list) or not all(isinstance(x, str) for x in eae):
            raise ConfigError("guards.expired_actions_execute must be a list of action names")
        s.expired_actions_execute = [str(x) for x in eae]

    sec = Secrets()
    sec.bybit_api_key = env.get("BYBIT_API_KEY", "")
    sec.bybit_api_secret = env.get("BYBIT_API_SECRET", "")
    for m in MODES:
        up = m.upper()
        v = env.get(f"LAKE_SIGNAL_SECRET_{up}", "")
        if v:
            sec.signal_secret[m] = v
        v = env.get(f"LAKE_REPORT_SECRET_{up}", "")
        if v:
            sec.report_secret[m] = v
        v = env.get(f"LAKE_REPORT_URL_{up}", "")
        if v:
            sec.report_url[m] = v
    sec.admin_token = env.get("ADMIN_TOKEN", "")
    sec.telegram_bot_token = env.get("TELEGRAM_BOT_TOKEN", "")
    sec.telegram_chat_id = env.get("TELEGRAM_CHAT_ID", "")
    sec.database_url = env.get("DATABASE_URL", "").strip()
    s.secrets = sec

    raw_accounts = _get(cfg, "accounts", None)
    if raw_accounts is None:
        # 1단계 호환: 최상위 값 + BYBIT_* 로 bybit 계정 하나
        a = AccountSettings(name="bybit", exchange="bybit", enabled=True, symbol=s.symbol,
                            position_mode=s.position_mode, leverage=s.leverage, margin_mode=s.margin_mode,
                            testnet=s.testnet, env_prefix="BYBIT", qty_multiplier=1.0, report=True, category=s.category)
        _fill_account_env(a, env, sec)
        s.accounts = [a]
    else:
        if not isinstance(raw_accounts, list) or not raw_accounts:
            raise ConfigError("accounts must be a non-empty list")
        s.accounts = [_account_from_cfg(s, raw, env, sec) for raw in raw_accounts]
        # 1단계 필드도 첫 계정에 맞춰 두어 최상위 값을 읽는 옛 코드/출력이 어긋나지 않게 한다
        first = s.accounts[0]
        s.symbol, s.position_mode, s.leverage = first.symbol, first.position_mode, first.leverage
        s.margin_mode, s.testnet = first.margin_mode, first.testnet
    validate_database(s)
    validate(s)
    return s


_SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_KNOWN_ACTIONS = ("entry", "add", "partial_exit", "full_exit", "protection_update")


def validate_database(s: Settings) -> None:
    """DATABASE_URL / database.schema / guards.expired_actions_execute 검증 (값은 메시지에 넣지 않는다)."""
    if not _SCHEMA_RE.match(s.db_schema or ""):
        raise ConfigError("database.schema must match [a-z_][a-z0-9_]{0,62}")
    bad = [a for a in s.expired_actions_execute if a not in _KNOWN_ACTIONS]
    if bad:
        raise ConfigError(f"guards.expired_actions_execute has unknown actions: {bad}")
    url = s.db_url
    if not url:
        return
    if not url.lower().startswith(("postgres://", "postgresql://")):
        raise ConfigError("DATABASE_URL must start with postgres:// or postgresql:// (leave empty for SQLite)")
    try:
        u = urllib.parse.urlsplit(url)
        host, port = (u.hostname or ""), u.port
    except ValueError:
        raise ConfigError("DATABASE_URL is not a valid URL") from None
    if not host:
        raise ConfigError("DATABASE_URL has no host")
    if u.query:
        raise ConfigError("DATABASE_URL must not carry query parameters (e.g. ?pgbouncer=true); use the plain session-pooler URL")
    if host.endswith("pooler.supabase.com") and port == 6543:
        raise ConfigError("DATABASE_URL uses the Supabase transaction pooler (6543); use the session pooler port 5432")


def validate_account(a: AccountSettings) -> None:
    if not ACCOUNT_NAME_RE.match(a.name or ""):
        raise ConfigError(f"account name {a.name!r} must match [a-z0-9_-]{{1,32}}")
    if a.exchange not in EXCHANGES:
        raise ConfigError(f"account {a.name}: exchange must be one of {EXCHANGES}")
    if a.position_mode not in ("hedge", "one_way"):
        raise ConfigError(f"account {a.name}: position_mode must be hedge | one_way")
    if a.exchange == "toobit" and a.position_mode != "hedge":
        raise ConfigError(f"account {a.name}: toobit supports position_mode hedge only (LONG/SHORT 양방향)")
    if a.exchange == "toobit" and a.testnet:
        # Toobit 은 테스트넷/데모 환경이 없다. testnet:true 를 받아들이면 운영자가 샌드박스라고 믿은 채 실주문이 나간다.
        raise ConfigError(f"account {a.name}: toobit has no testnet — set testnet:false (orders always go to the mainnet)")
    if a.margin_mode not in ("", "isolated", "cross"):
        raise ConfigError(f"account {a.name}: margin_mode must be isolated | cross | ''")
    if a.leverage < 1 or a.leverage > 100:
        raise ConfigError(f"account {a.name}: leverage out of range")
    if not (a.qty_multiplier > 0) or a.qty_multiplier > 100:
        raise ConfigError(f"account {a.name}: qty_multiplier must be in (0, 100]")
    if not a.symbol:
        raise ConfigError(f"account {a.name}: symbol required")
    if not ENV_PREFIX_RE.match(a.env_prefix or ""):
        raise ConfigError(f"account {a.name}: env_prefix must match [A-Z][A-Z0-9_]*")
    if a.exchange == "bybit" and a.category != "linear":
        raise ConfigError(f"account {a.name}: category must be linear (계약: Bybit/linear/BTCUSDT)")
    if a.exchange == "okx" and _real_keys(a.api_key, a.api_secret) and not a.api_passphrase:
        raise ConfigError(f"account {a.name}: OKX requires {a.env_prefix}_API_PASSPHRASE when API keys are set")
    for m, v in list(a.report_secret.items()):
        if len(v.encode("utf-8")) < 32:
            raise ConfigError(f"account {a.name}: report secret for {m} must be at least 32 bytes")


def validate(s: Settings) -> None:
    if s.position_mode not in ("hedge", "one_way"):
        raise ConfigError("position_mode must be hedge | one_way")
    if s.category != "linear":
        raise ConfigError("category must be linear (계약: Bybit/linear/BTCUSDT)")
    if s.margin_mode not in ("", "isolated", "cross"):
        raise ConfigError("margin_mode must be isolated | cross | ''")
    if s.leverage < 1 or s.leverage > 100:
        raise ConfigError("leverage out of range")
    if s.max_order_qty_btc <= 0 or s.max_leg_qty_btc <= 0:
        raise ConfigError("guards.max_*_qty_btc must be > 0")
    if s.max_clock_skew_ms <= 0 or s.max_body_bytes <= 0:
        raise ConfigError("max_clock_skew_ms / max_body_bytes must be > 0")
    for m, v in list(s.secrets.signal_secret.items()) + list(s.secrets.report_secret.items()):
        if len(v.encode("utf-8")) < 32:
            raise ConfigError(f"secret for {m} must be at least 32 bytes")
    # 관리 엔드포인트 토큰도 HMAC 시크릿과 같은 최소 길이 (무차별 대입 방지). 생성: python -c "import secrets;print(secrets.token_urlsafe(32))"
    if s.secrets.admin_token and len(s.secrets.admin_token.encode("utf-8")) < 32:
        raise ConfigError("ADMIN_TOKEN must be at least 32 bytes (generate with secrets.token_urlsafe(32))")
    if s.protection_trigger_by not in ("MarkPrice", "LastPrice", "IndexPrice"):
        raise ConfigError("guards.protection_trigger_by must be MarkPrice | LastPrice | IndexPrice")
    if s.routing not in ROUTINGS:
        raise ConfigError(f"routing must be one of {ROUTINGS}")
    if not s.accounts:
        raise ConfigError("at least one account is required")
    names: set[str] = set()
    prefixes: dict[str, str] = {}     # env_prefix -> account name
    suffixes: dict[str, str] = {}     # env_suffix(name) -> account name
    for a in s.accounts:
        if not isinstance(a, AccountSettings):
            raise ConfigError("accounts must contain AccountSettings")
        validate_account(a)
        if a.name in names:
            raise ConfigError(f"duplicate account name: {a.name}")
        names.add(a.name)
        # 같은 env_prefix = 같은 API 키 = 거래소의 같은 계정. fanout 이 같은 order_link_id 를 두 번 보내 한쪽이 다른 쪽의
        # 주문/체결을 가져가게 되므로(Toobit -1141 멱등 경로) 계정마다 다른 키(접두사)를 요구한다.
        if a.env_prefix in prefixes:
            raise ConfigError(f"accounts {prefixes[a.env_prefix]!r} and {a.name!r} share env_prefix {a.env_prefix} "
                              f"(same API keys); give each account its own env_prefix and keys")
        prefixes[a.env_prefix] = a.name
        sfx = env_suffix(a.name)
        if sfx in suffixes:
            raise ConfigError(f"accounts {suffixes[sfx]!r} and {a.name!r} map to the same env suffix {sfx} "
                              f"(LAKE_REPORT_URL_*_{sfx}); rename one of them")
        suffixes[sfx] = a.name
