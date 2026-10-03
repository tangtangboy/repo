"""설정 로더: config.json(비밀 아님) + .env(비밀). 검증 후 Settings 로 고정."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from .util import parse_env_file, read_json

PLACEHOLDER = "PUT_"
MODES = ("test", "live")


class ConfigError(ValueError):
    pass


@dataclass
class Secrets:
    bybit_api_key: str = ""
    bybit_api_secret: str = ""
    signal_secret: dict = field(default_factory=dict)   # mode -> secret (수신 검증)
    report_secret: dict = field(default_factory=dict)   # mode -> secret (회신 서명)
    report_url: dict = field(default_factory=dict)      # mode -> URL (비어 있으면 전송 안 함)
    admin_token: str = ""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    def has_real_bybit_keys(self) -> bool:
        k, s = self.bybit_api_key, self.bybit_api_secret
        return bool(k and s) and PLACEHOLDER not in k and PLACEHOLDER not in s


@dataclass
class Settings:
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

    secrets: Secrets = field(default_factory=Secrets)

    # ---- 파생 ----
    @property
    def db_path(self) -> str:
        return os.path.join(self.state_dir, "lake.db")

    @property
    def halt_file(self) -> str:
        return os.path.join(self.state_dir, "HALT")

    def allowed_position_idx(self) -> set[int]:
        return {1, 2} if self.position_mode == "hedge" else {0}

    def live_execution_possible(self) -> tuple[bool, str]:
        """실주문 가능 여부와 아니면 그 이유(reason_code)."""
        if not self.live_enabled:
            return False, "LIVE_DISABLED"
        if not self.secrets.has_real_bybit_keys():
            return False, "LIVE_DISABLED"
        if not self.secrets.signal_secret.get("live"):
            return False, "LIVE_DISABLED"
        return True, ""


def _get(d: dict, path: str, default):
    cur = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


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
    s.secrets = sec
    validate(s)
    return s


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
