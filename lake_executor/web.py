"""운영 대시보드 (/ui) — 수신기와 같은 프로세스, stdlib + fastapi/starlette 만 (템플릿·JS·CDN 없음).

무엇을 하나
  - 로그인(ADMIN_TOKEN) → 서명 쿠키 세션 → 모든 POST 는 CSRF 토큰 필수.
  - 읽기: 개요(/ui), 신호 목록·상세(/ui/signals), 주문(/ui/orders), 회신(/ui/reports), 접수 로그(/ui/ingress).
  - 쓰기: 거래소 키·lake 시크릿을 `.env` 에 저장(/ui/accounts, /ui/secrets), live.enabled(config.json), HALT/재개/대사, 재시작.
  - 신호는 여전히 웹훅(POST {signal_path}) 으로만 들어온다. 대시보드는 신호를 만들거나 재생하지 않고 수신 검증도 바꾸지 않는다.

안전 규칙
  - ADMIN_TOKEN 미설정 → /ui 전체가 404 (관리 엔드포인트와 같은 정책). 로그인 실패는 /admin/* 와 같은 IP 별 예산을 쓴다.
  - 세션 쿠키 = sid.exp_ms.sig (키는 ADMIN_TOKEN 에서 파생) — 서버 상태 없이 재시작을 넘겨 살아남고, 토큰을 바꾸면 전부 무효.
    로그아웃은 메모리 revoke 목록(재시작 시 사라짐, 12h 절대 만료로 상한).
  - 시크릿 값은 어떤 페이지·플래시·알림·로그에도 싣지 않는다(mask(): 앞 4자 + 길이 뿐). 입력란은 절대 미리 채우지 않는다.
  - 플래시 배너(?flash=) 는 같은 세션이 60초 안에 서명한 것만 띄운다 — 링크를 꾸며 가짜 '완료' 배너를 보일 수 없다.
  - /ui 아래의 등록되지 않은 경로·메서드도 같은 404 정책과 보안 헤더를 받는다(Starlette 기본 404/405 로 떨어지지 않음).
  - 새 값의 적용 경로는 **재시작뿐**(핫스왑 없음). 디스크의 .env/config.json 이 단일 진실이며, 저장 뒤 "재시작 필요" 배너가 뜬다.
    유일한 프로세스 내 변경은 settings.live_enabled=False (더 보수적인 방향만 즉시 적용).
  - import 시 부작용 없음: receiver.create_app 이 mount(app, settings, store, services, admin_throttle) 로 붙인다.
"""
from __future__ import annotations

import collections
import hashlib
import hmac
import html
import json
import logging
import os
import re
import secrets
import signal
import threading
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from starlette.concurrency import run_in_threadpool
from starlette.routing import Match

from . import config as config_mod
from . import receiver as rcv
from . import util
from .util import now_ms, parse_env_file, read_json

log = logging.getLogger("lake_executor.web")

UI_PREFIX = "/ui"
COOKIE_NAME = "lake_ui"
SESSION_TTL_MS = 12 * 3600 * 1000
FORM_MAX_BYTES = 16384
CSRF_FIELD = "_csrf"
MAX_REVOKED = 1024
SESSION_KEY_INFO = b"lake-executor/ui/session/v1"

MODES = rcv.MODES
SIGNAL_STATUSES = ("accepted", "processing", "done", "rejected", "error")
REPORT_STATES = ("pending", "sent", "duplicate", "failed", "conflict", "unsent")
PAGE_LIMIT_DEFAULT = 50
PAGE_LIMIT_MAX = 200
CHECK_MIN_INTERVAL_MS = 10_000       # 계정 연결 확인: 계정당 10초에 1회
RECONCILE_MIN_INTERVAL_MS = 5_000    # 대시보드 대사: 5초 간격, 동시 1건
AUDIT_LEN = 20
FLASH_MAX = 300
FLASH_TTL_MS = 60_000                # 서명된 플래시(?flash=) 유효 시간
OFFSET_MAX = 10**9                   # 목록 페이지 offset 상한 (sqlite 64비트 바인딩 안쪽)
_FILTER_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")

SEC_HEADERS: dict[str, str] = {
    "Cache-Control": "no-store",
    "Content-Security-Policy": ("default-src 'none'; style-src 'unsafe-inline'; img-src 'self' data:; "
                                "form-action 'self'; base-uri 'none'; frame-ancestors 'none'"),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
}

CSS = """
body{font:14px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;margin:0;background:#f4f6f8;color:#1b1f24}
header{display:flex;align-items:center;gap:14px;padding:10px 20px;background:#1b1f24;color:#e6e8eb;flex-wrap:wrap}
header .brand{font-weight:700;letter-spacing:.02em}
header nav a{color:#c9d1d9;text-decoration:none;margin-right:10px;padding:4px 7px;border-radius:4px}
header nav a.active,header nav a:hover{background:#30363d;color:#fff}
header form{margin-left:auto}
main{padding:16px 20px;max-width:1500px}
h1{font-size:20px;margin:0 0 12px}
h2{font-size:16px;margin:20px 0 8px}
table{border-collapse:collapse;width:100%;background:#fff;font-size:13px;margin-bottom:12px}
th,td{border:1px solid #d0d7de;padding:4px 8px;text-align:left;vertical-align:top;font-family:ui-monospace,Consolas,monospace}
th{background:#eaeef2;font-family:inherit}
.banner{padding:10px 20px;font-weight:600}
.banner.halt{background:#b42318;color:#fff}
.banner.halt a{color:#fff}
.banner.pending{background:#fff3cd;color:#664d03;border-bottom:1px solid #ffe69c}
.banner.flash{background:#d1e7dd;color:#0f5132}
.banner.error{background:#f8d7da;color:#842029}
.card{background:#fff;border:1px solid #d0d7de;border-radius:6px;padding:12px 16px;margin-bottom:16px}
.kv{display:grid;grid-template-columns:max-content 1fr;gap:2px 16px;font-family:ui-monospace,Consolas,monospace;font-size:13px;margin:0 0 8px}
.kv dt{color:#57606a}.kv dd{margin:0;word-break:break-all}
form.act{display:inline-block;margin:6px 12px 6px 0;padding:8px 10px;border:1px solid #d0d7de;border-radius:6px;background:#fff;vertical-align:top}
form.act label{display:block;margin:4px 0}
form.act input[type=text],form.act input[type=password],form.act select{font-family:ui-monospace,Consolas,monospace;min-width:280px}
form.login{max-width:420px;margin:60px auto;padding:20px;background:#fff;border:1px solid #d0d7de;border-radius:6px}
form.login input{width:100%;box-sizing:border-box;margin:8px 0;font-family:ui-monospace,Consolas,monospace}
button{padding:4px 12px;border:1px solid #57606a;border-radius:4px;background:#f6f8fa;cursor:pointer}
button.danger{background:#b42318;color:#fff;border-color:#8a1a12}
.st{padding:1px 6px;border-radius:3px;font-size:12px;white-space:nowrap}
.st-done,.st-sent,.st-filled,.st-ok,.st-true{background:#d1e7dd;color:#0f5132}
.st-rejected,.st-failed,.st-conflict,.st-error,.st-fail,.st-false{background:#f8d7da;color:#842029}
.st-accepted,.st-processing,.st-pending,.st-new,.st-submitted{background:#cfe2ff;color:#084298}
.st-duplicate,.st-unsent,.st-skipped{background:#e2e3e5;color:#41464b}
pre{background:#f6f8fa;border:1px solid #d0d7de;padding:8px;overflow:auto;font-size:12px;max-height:480px}
.muted{color:#57606a}.warn{color:#9a3412}
.pager a{margin-right:12px}
footer{padding:12px 20px;color:#57606a;font-size:12px}
"""


# --------------------------------------------------------------------------- #
# 세션 / CSRF
# --------------------------------------------------------------------------- #
@dataclass
class Session:
    sid: str
    exp_ms: int
    csrf: str
    cookie: str


class SessionCodec:
    """서명 쿠키 `sid.exp_ms.sig`. 키 = HMAC(ADMIN_TOKEN, SESSION_KEY_INFO) 를 요청마다 다시 계산한다
    (토큰 교체 → 재시작 뒤 모든 세션 무효, 토큰이 같으면 재시작을 넘겨 유지). CSRF = HMAC(key, 'csrf|'+sid)."""

    def __init__(self, token_getter: Callable[[], str]):
        self._token_getter = token_getter
        self.revoked: set[str] = set()
        self._lock = threading.Lock()

    def _key(self) -> bytes | None:
        token = str(self._token_getter() or "")
        if not token:
            return None
        return hmac.new(token.encode("utf-8"), SESSION_KEY_INFO, hashlib.sha256).digest()

    @staticmethod
    def _sig(key: bytes, sid: str, exp_ms: int) -> str:
        return hmac.new(key, f"{sid}.{exp_ms}".encode("utf-8"), hashlib.sha256).hexdigest()

    @staticmethod
    def _csrf(key: bytes, sid: str) -> str:
        return hmac.new(key, b"csrf|" + sid.encode("utf-8"), hashlib.sha256).hexdigest()

    def issue(self, now: int) -> Session:
        key = self._key()
        if key is None:
            raise RuntimeError("ADMIN_TOKEN not configured")
        sid = secrets.token_urlsafe(24)
        exp_ms = int(now) + SESSION_TTL_MS
        sig = self._sig(key, sid, exp_ms)
        return Session(sid, exp_ms, self._csrf(key, sid), f"{sid}.{exp_ms}.{sig}")

    def parse(self, cookie_value: str | None, now: int) -> Session | None:
        if not cookie_value or len(cookie_value) > 256:
            return None
        parts = cookie_value.split(".")
        if len(parts) != 3:
            return None
        sid, exp_s, sig = parts
        # str.isdigit() 은 '²' 같은 비ASCII 숫자도 True 라 int() 가 ValueError 를 낸다 → ASCII 숫자만 받는다
        if not sid or not (exp_s.isascii() and exp_s.isdigit()) or len(exp_s) > 20:
            return None
        exp_ms = int(exp_s)
        key = self._key()
        if key is None:
            return None
        if not hmac.compare_digest(sig.encode("utf-8"), self._sig(key, sid, exp_ms).encode("utf-8")):
            return None
        if exp_ms <= int(now):
            return None
        with self._lock:
            if sid in self.revoked:
                return None
        return Session(sid, exp_ms, self._csrf(key, sid), cookie_value)

    def revoke(self, sid: str) -> None:
        with self._lock:
            if len(self.revoked) > MAX_REVOKED:
                self.revoked.clear()
            self.revoked.add(sid)

    def flash_sig(self, sid: str, at_ms: int, text: str) -> str | None:
        """플래시 배너 서명 = HMAC(key, 'flash|sid|at_ms|text')[:32]. 세션(sid) 에 묶여 다른 세션·꾸민 링크에서는 맞지 않는다."""
        key = self._key()
        if key is None:
            return None
        msg = b"flash|" + sid.encode("utf-8") + b"|" + str(int(at_ms)).encode("ascii") + b"|" + text.encode("utf-8")
        return hmac.new(key, msg, hashlib.sha256).hexdigest()[:32]


# --------------------------------------------------------------------------- #
# 재시작 가드
# --------------------------------------------------------------------------- #
class RestartGuard:
    """대시보드 재시작 요청: 기동 60초 뒤부터, 60초 간격, 5분에 3회까지 (systemd StartLimitBurst=5/300s 아래).
    이력은 store meta `ui_restarts`(JSON 리스트, ms) 에 영속되어 재시작을 넘겨 유지된다.
    request() 는 DELAY_S 뒤 kill(getpid, SIGTERM) → main._on_signal 정상 종료 → systemd Restart=always."""

    MIN_INTERVAL_MS = 60_000
    MAX_PER_WINDOW = 3
    WINDOW_MS = 300_000
    DELAY_S = 0.5
    META_KEY = "ui_restarts"

    def __init__(self, store, started_ms: int, kill=os.kill, timer=threading.Timer, alerts: Any = None):
        self.store = store
        self.started_ms = int(started_ms)
        self.kill = kill
        self.timer = timer
        self.alerts = alerts

    def history(self, now: int) -> list[int]:
        try:
            raw = self.store.get_meta(self.META_KEY)
            hist = json.loads(raw) if raw else []
        except (TypeError, ValueError):
            hist = []
        if not isinstance(hist, list):
            hist = []
        out = []
        for v in hist:
            try:
                t = int(v)
            except (TypeError, ValueError):
                continue
            if now - t < self.WINDOW_MS:
                out.append(t)
        return sorted(out)

    def allowed(self, now: int) -> tuple[bool, str]:
        if now - self.started_ms < self.MIN_INTERVAL_MS:
            return False, "PROCESS_TOO_YOUNG"
        hist = self.history(now)
        if hist and now - hist[-1] < self.MIN_INTERVAL_MS:
            return False, "TOO_SOON"
        if len(hist) >= self.MAX_PER_WINDOW:
            return False, "RATE_LIMITED"
        return True, ""

    def request(self, reason: str) -> tuple[bool, str]:
        now = now_ms()
        ok, why = self.allowed(now)
        if not ok:
            return False, why
        hist = self.history(now) + [now]
        self.store.set_meta(self.META_KEY, json.dumps(hist))
        log.warning("ui: restart requested (%s): SIGTERM in %.1fs", reason, self.DELAY_S)
        if self.alerts is not None:
            try:
                self.alerts.send(f"[admin] restart requested via {reason}")
            except Exception:  # noqa: BLE001
                pass
        pid = os.getpid()
        t = self.timer(self.DELAY_S, lambda: self.kill(pid, signal.SIGTERM))
        try:
            t.daemon = True
        except Exception:  # noqa: BLE001 - 테스트용 가짜 타이머
            pass
        t.start()
        return True, ""


# --------------------------------------------------------------------------- #
# 시크릿 화이트리스트 / 마스킹
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class KeySpec:
    group: str            # signal | report | telegram | admin | account
    account: str | None
    secret: bool          # True → password 입력란 (URL/chat id 는 text). 표시는 어느 쪽이든 mask()
    min_bytes: int


def allowed_env_keys(settings: Any) -> dict[str, KeySpec]:
    """대시보드가 쓸 수 있는 .env 키 전체 (config.load 가 아는 키만; 그 외는 BAD_KEY)."""
    out: dict[str, KeySpec] = {}
    accounts = list(getattr(settings, "accounts", None) or [])
    for m in MODES:
        up = m.upper()
        out[f"LAKE_SIGNAL_SECRET_{up}"] = KeySpec("signal", None, True, 32)
        out[f"LAKE_REPORT_SECRET_{up}"] = KeySpec("report", None, True, 32)
        out[f"LAKE_REPORT_URL_{up}"] = KeySpec("report", None, False, 0)
    for acct in accounts:
        sfx = util.env_suffix(acct.name)
        for m in MODES:
            up = m.upper()
            out[f"LAKE_REPORT_URL_{up}_{sfx}"] = KeySpec("report", acct.name, False, 0)
            out[f"LAKE_REPORT_SECRET_{up}_{sfx}"] = KeySpec("report", acct.name, True, 32)
    out["TELEGRAM_BOT_TOKEN"] = KeySpec("telegram", None, True, 0)
    out["TELEGRAM_CHAT_ID"] = KeySpec("telegram", None, False, 0)
    out["ADMIN_TOKEN"] = KeySpec("admin", None, True, 32)
    for acct in accounts:
        p = acct.env_prefix
        out[f"{p}_API_KEY"] = KeySpec("account", acct.name, True, 0)
        out[f"{p}_API_SECRET"] = KeySpec("account", acct.name, True, 0)
        if acct.exchange == "okx":
            out[f"{p}_API_PASSPHRASE"] = KeySpec("account", acct.name, True, 0)
    out["DATABASE_URL"] = KeySpec("database", None, True, 0)   # postgres://… (비밀번호 포함) — 비우면 SQLite
    return out


def account_key_names(acct: Any) -> dict[str, str]:
    """폼 필드 이름 → .env 키 (okx 만 passphrase)."""
    p = acct.env_prefix
    names = {"api_key": f"{p}_API_KEY", "api_secret": f"{p}_API_SECRET"}
    if acct.exchange == "okx":
        names["api_passphrase"] = f"{p}_API_PASSPHRASE"
    return names


def in_process_env(settings: Any) -> dict[str, str]:
    """화이트리스트 키 → 지금 프로세스가 들고 있는 값 (표시는 mask 를 거친다)."""
    sec = settings.secrets
    out: dict[str, str] = {}
    for m in MODES:
        up = m.upper()
        out[f"LAKE_SIGNAL_SECRET_{up}"] = (sec.signal_secret or {}).get(m, "")
        out[f"LAKE_REPORT_SECRET_{up}"] = (sec.report_secret or {}).get(m, "")
        out[f"LAKE_REPORT_URL_{up}"] = (sec.report_url or {}).get(m, "")
    for acct in getattr(settings, "accounts", None) or []:
        sfx = util.env_suffix(acct.name)
        for m in MODES:
            up = m.upper()
            out[f"LAKE_REPORT_URL_{up}_{sfx}"] = (acct.report_url or {}).get(m, "")
            out[f"LAKE_REPORT_SECRET_{up}_{sfx}"] = (acct.report_secret or {}).get(m, "")
        p = acct.env_prefix
        out[f"{p}_API_KEY"] = acct.api_key or ""
        out[f"{p}_API_SECRET"] = acct.api_secret or ""
        out[f"{p}_API_PASSPHRASE"] = acct.api_passphrase or ""
    out["TELEGRAM_BOT_TOKEN"] = sec.telegram_bot_token or ""
    out["TELEGRAM_CHAT_ID"] = sec.telegram_chat_id or ""
    out["ADMIN_TOKEN"] = sec.admin_token or ""
    out["DATABASE_URL"] = getattr(sec, "database_url", "") or ""
    return out


def mask(value: Any) -> str:
    """값을 절대 드러내지 않는 상태 문자열: missing | placeholder | set (len=N) | set (ABCD… len=N)."""
    v = "" if value is None else str(value)
    if not v:
        return "missing"
    if config_mod.PLACEHOLDER in v:
        return "placeholder"
    if len(v) < 12:
        return f"set (len={len(v)})"
    return f"set ({v[:4]}… len={len(v)})"


def mask_url(value: Any) -> str:
    v = "" if value is None else str(value)
    if not v:
        return "missing"
    if config_mod.PLACEHOLDER in v:
        return "placeholder"
    try:
        host = urllib.parse.urlsplit(v).netloc
    except ValueError:
        host = "?"
    return f"set (host={host or '?'} len={len(v)})"


def mask_key(key: str, value: Any) -> str:
    return mask_url(value) if key.startswith("LAKE_REPORT_URL_") else mask(value)


# --------------------------------------------------------------------------- #
# 렌더링 도우미
# --------------------------------------------------------------------------- #
class _Raw(str):
    """이미 이스케이프된 HTML 조각 (표 셀에 링크/배지를 넣을 때)."""


def _esc(v: Any) -> str:
    return html.escape("" if v is None else str(v), quote=True)


def _cell(v: Any) -> str:
    return v if isinstance(v, _Raw) else _esc(v)


def _table(headers: list[str], rows: list[list[Any]], empty: str = "(none)") -> str:
    if not rows:
        return f'<p class="muted">{_esc(empty)}</p>'
    h = "".join(f"<th>{_esc(x)}</th>" for x in headers)
    b = "".join("<tr>" + "".join(f"<td>{_cell(c)}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table>"


def _kv(pairs: list[tuple[str, Any]]) -> str:
    return '<dl class="kv">' + "".join(f"<dt>{_esc(k)}</dt><dd>{_cell(v)}</dd>" for k, v in pairs) + "</dl>"


def _fmt_ms(ms: Any) -> str:
    if ms in (None, ""):
        return ""
    try:
        t = int(ms)
    except (TypeError, ValueError):
        return str(ms)
    return datetime.fromtimestamp(t / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{t % 1000:03d}Z"


def _fmt_uptime(ms: int) -> str:
    s = max(0, int(ms) // 1000)
    return f"{s // 3600}h{(s % 3600) // 60:02d}m{s % 60:02d}s"


def _status(s: Any) -> _Raw:
    text = "" if s is None else str(s)
    cls = re.sub(r"[^a-z0-9]", "-", text.lower())[:24] or "none"
    return _Raw(f'<span class="st st-{cls}">{_esc(text)}</span>')


def _link(href: str, text: Any) -> _Raw:
    return _Raw(f'<a href="{_esc(href)}">{_esc(text)}</a>')


def _event_link(event_id: Any, mode: Any) -> Any:
    if not event_id:
        return ""
    q = urllib.parse.urlencode({"mode": mode}) if mode else ""
    return _link(f"{UI_PREFIX}/signals/{urllib.parse.quote(str(event_id), safe='')}" + (f"?{q}" if q else ""), event_id)


def _json_pre(obj: Any) -> _Raw:
    try:
        text = json.dumps(obj, indent=2, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        text = str(obj)
    return _Raw(f"<pre>{_esc(text)}</pre>")


def _pager(path: str, params: dict[str, Any], limit: int, offset: int, has_more: bool) -> str:
    def _href(off: int) -> str:
        q = {k: v for k, v in params.items() if v not in (None, "")}
        q["limit"] = limit
        q["offset"] = max(0, off)
        return f"{path}?{urllib.parse.urlencode(q)}"
    parts = []
    if offset > 0:
        parts.append(f'<a href="{_esc(_href(offset - limit))}">&laquo; prev</a>')
    parts.append(f'<span class="muted">offset {offset} · limit {limit}</span>')
    if has_more:
        parts.append(f'<a href="{_esc(_href(offset + limit))}">next &raquo;</a>')
    return '<p class="pager">' + " ".join(parts) + "</p>"


def _form_post(action: str, csrf: str, fields_html: str, button: str, confirm_word: str | None = None,
               *, danger: bool = False) -> str:
    confirm = ""
    if confirm_word:
        confirm = (f'<label>type <code>{_esc(confirm_word)}</code> to confirm '
                   f'<input type="text" name="confirm" autocomplete="off"></label>')
    cls = ' class="danger"' if danger else ""
    return (f'<form method="post" action="{_esc(action)}" class="act">'
            f'<input type="hidden" name="{CSRF_FIELD}" value="{_esc(csrf)}">'
            f"{fields_html}{confirm}<button type=\"submit\"{cls}>{_esc(button)}</button></form>")


def _select(name: str, options: list[tuple[str, str]], *, blank: str | None = None) -> str:
    opts = ""
    if blank is not None:
        opts += f'<option value="">{_esc(blank)}</option>'
    opts += "".join(f'<option value="{_esc(v)}">{_esc(label)}</option>' for v, label in options)
    return f'<select name="{_esc(name)}">{opts}</select>'


# --------------------------------------------------------------------------- #
# 오류 (핸들러 안에서 던지면 응답으로 바뀐다)
# --------------------------------------------------------------------------- #
class UiError(Exception):
    def __init__(self, status: int, code: str, message: str = "", *, as_json: bool = False):
        super().__init__(f"{status} {code}")
        self.status = status
        self.code = code
        self.message = message
        self.as_json = as_json


def validate_env_candidate(settings: Any, paths: Any, updates: dict[str, str], exchange_factory: Callable) -> Any:
    """쓰기 전 검증: 화이트리스트 → config.load(병합 후보) → 키가 바뀐 계정의 거래소 생성자 드라이런(네트워크 없음).
    실패는 UiError(400) — 메시지에는 키 이름/길이만 들어간다 (config.validate 의 문구), 값은 절대 없다."""
    specs = allowed_env_keys(settings)
    for k, v in updates.items():
        if k not in specs:
            raise UiError(400, "BAD_KEY", f"{k} is not an editable key", as_json=True)
        spec = specs[k]
        if spec.min_bytes and len(v.encode("utf-8")) < spec.min_bytes:
            raise UiError(400, "INVALID_CANDIDATE", f"{k} must be at least {spec.min_bytes} bytes")
        if k.startswith("LAKE_REPORT_URL_"):
            # config.load 는 URL 을 검사하지 않는다 — 오타 난 URL 이 재시작 뒤 회신기 안에서야 터지지 않도록 여기서 막는다
            try:
                u = urllib.parse.urlsplit(v)
            except ValueError:
                u = None
            if u is None or u.scheme not in ("http", "https") or not u.netloc:
                raise UiError(400, "INVALID_CANDIDATE", f"{k} must be an absolute http(s) URL")
        if k == "DATABASE_URL" and v and not v.lower().startswith(("postgres://", "postgresql://")):
            # 자세한 검사는 config.validate_database (아래 config.load) — 값은 메시지에 넣지 않는다
            raise UiError(400, "INVALID_CANDIDATE", "DATABASE_URL must start with postgres:// (empty = SQLite)")
    try:
        cand = config_mod.load(paths.config, paths.env, env_override=dict(updates))
    except (config_mod.ConfigError, ValueError, TypeError) as e:
        raise UiError(400, "INVALID_CANDIDATE", str(e)[:300]) from None
    touched = {specs[k].account for k in updates if specs[k].group == "account"}
    for name in sorted(n for n in touched if n):
        acct = cand.account_or_none(name)
        if acct is None or not acct.has_real_keys():
            continue
        try:
            ex = exchange_factory(acct)
            close = getattr(ex, "close", None)
            if callable(close):
                close()
        except Exception as e:  # noqa: BLE001 - 원문(키 포함 가능) 은 절대 내보내지 않는다
            raise UiError(400, "EXCHANGE_CONSTRUCT_FAILED",
                          f"exchange {name} cannot be constructed: {type(e).__name__}") from None
    return cand


def _default_exchange_factory(acct: Any) -> Any:
    from .exchange import build_exchange  # 지연 import
    return build_exchange(acct)


# --------------------------------------------------------------------------- #
# mount
# --------------------------------------------------------------------------- #
def mount(app: FastAPI, settings: Any, store: Any, services: Any, admin_throttle: Any) -> None:
    """receiver.create_app 끝에서 호출. 라우트·세션·상태를 app 에 붙인다 (import 부작용 없음)."""
    account_names = [a.name for a in (getattr(settings, "accounts", None) or [])]
    codec = SessionCodec(lambda: getattr(settings.secrets, "admin_token", "") or "")
    started_ms = int(rcv._svc(services, "started_ms") or now_ms())

    app.state.ui_session_codec = codec
    app.state.ui_pending = None                                  # {"keys": [...], "at_ms": int} — 재시작으로만 지워진다
    app.state.ui_audit = collections.deque(maxlen=AUDIT_LEN)    # (at_ms, ip, action, detail) — 이름만, 값 없음
    app.state.ui_check_results = {}                              # account -> {"at_ms","ok","lines"}
    app.state.ui_check_at = {}                                   # account -> 마지막 check ms (속도 제한)
    app.state.ui_last_reconcile = None
    app.state.ui_exchange_factory = _default_exchange_factory
    app.state.ui_restart_guard = RestartGuard(store, started_ms, alerts=rcv._svc(services, "alerts"))
    reconcile_lock = threading.Lock()
    reconcile_last = [0]
    write_lock = threading.Lock()      # .env / config.json 쓰기(검증→쓰기→재검증→복원) 를 한 번에 하나만

    # ------------------------------------------------------------------ 작은 도우미
    def _token() -> str:
        return str(getattr(settings.secrets, "admin_token", "") or "")

    def _paths() -> Any:
        p = rcv._svc(services, "paths")
        if p is None or not getattr(p, "env", None) or not getattr(p, "config", None):
            return None
        return p

    def _executor() -> Any:
        return rcv._svc(services, "executor")

    def _alert(text: str) -> None:
        alerts = rcv._svc(services, "alerts")
        if alerts is not None:
            try:
                alerts.send(text)
            except Exception:  # noqa: BLE001
                pass

    def _audit(ip: str, action: str, detail: str = "", *, alert: bool = True) -> None:
        app.state.ui_audit.append((now_ms(), ip, action, detail))
        log.warning("ui: %s by %s%s", action, ip, f" ({detail})" if detail else "")
        if alert:
            _alert(f"[admin] {action} via dashboard" + (f": {detail}" if detail else ""))

    def _mark_pending(keys: list[str]) -> None:
        cur = app.state.ui_pending or {"keys": [], "at_ms": 0}
        merged = list(cur["keys"]) + [k for k in keys if k not in cur["keys"]]
        app.state.ui_pending = {"keys": merged, "at_ms": now_ms()}

    def _with_headers(resp: Response) -> Response:
        for k, v in SEC_HEADERS.items():
            resp.headers[k] = v
        return resp

    def _html(status: int, body: str, extra_headers: dict[str, str] | None = None) -> HTMLResponse:
        resp = HTMLResponse(content=body, status_code=status)
        _with_headers(resp)
        for k, v in (extra_headers or {}).items():
            resp.headers.append(k, v)
        return resp

    def _json(status: int, body: dict) -> JSONResponse:
        return _with_headers(JSONResponse(status_code=status, content=body))  # type: ignore[return-value]

    def _text(status: int, text: str) -> PlainTextResponse:
        return _with_headers(PlainTextResponse(content=text, status_code=status))  # type: ignore[return-value]

    def _redirect(location: str, *, flash: str | None = None, cookie: str | None = None,
                  session: Session | None = None) -> Response:
        """flash 는 세션이 있을 때만 붙고, fat(시각)+fsig(HMAC) 로 서명된다 (_flash_of 가 검증; 꾸민 링크는 배너가 안 뜬다)."""
        if flash and session is not None:
            text = flash[:FLASH_MAX]
            at = now_ms()
            sig = codec.flash_sig(session.sid, at, text)
            if sig:
                location += ("&" if "?" in location else "?") + urllib.parse.urlencode(
                    {"flash": text, "fat": at, "fsig": sig})
        resp = Response(status_code=303)
        resp.headers["Location"] = location
        _with_headers(resp)
        if cookie:
            resp.headers.append("set-cookie", cookie)
        return resp

    def _cookie_header(value: str, secure: bool) -> str:
        return (f"{COOKIE_NAME}={value}; Path={UI_PREFIX}; HttpOnly; SameSite=Strict; Max-Age={SESSION_TTL_MS // 1000}"
                + ("; Secure" if secure else ""))

    def _clear_cookie_header() -> str:
        return f"{COOKIE_NAME}=; Path={UI_PREFIX}; HttpOnly; SameSite=Strict; Max-Age=0"

    def _is_https(request: Request) -> bool:
        if request.url.scheme == "https":
            return True
        if not rcv._peer_is_loopback(request):   # X-Forwarded-Proto 는 같은 호스트의 Caddy 가 붙인 것만 믿는다
            return False
        xfp = request.headers.get("x-forwarded-proto") or ""
        return xfp.split(",")[0].strip().lower() == "https"

    def _flash_of(request: Request, session: Session) -> str | None:
        """?flash= 는 이 세션이 FLASH_TTL_MS 안에 서명한(fat/fsig) 것만 돌려준다. 그 외(서명 없음·다른 세션·만료·변조) 는 None."""
        q = request.query_params
        text = q.get("flash") or ""
        at_s = q.get("fat") or ""
        sig = q.get("fsig") or ""
        if not text or len(text) > FLASH_MAX or not sig or len(at_s) > 20 or not (at_s.isascii() and at_s.isdigit()):
            return None
        at = int(at_s)
        if abs(now_ms() - at) > FLASH_TTL_MS:
            return None
        want = codec.flash_sig(session.sid, at, text)
        if not want or not hmac.compare_digest(sig.encode("utf-8"), want.encode("utf-8")):
            return None
        return text

    def _q(request: Request, name: str) -> str | None:
        v = request.query_params.get(name)
        return v if v not in (None, "") else None

    def _paging(request: Request) -> tuple[int, int]:
        """limit 1..200 (기본 50), offset ≥ 0. 정수가 아니면 UiError(400)."""
        def _int(name: str, default: int) -> int:
            v = request.query_params.get(name)
            if v in (None, ""):
                return default
            try:
                return int(v)
            except ValueError:
                raise UiError(400, "BAD_REQUEST", f"{name} must be an integer") from None
        limit = min(PAGE_LIMIT_MAX, max(1, _int("limit", PAGE_LIMIT_DEFAULT)))
        offset = min(OFFSET_MAX, max(0, _int("offset", 0)))   # 64비트를 넘는 정수는 sqlite 바인딩에서 OverflowError
        return limit, offset

    def _filter(request: Request, name: str, allowed: tuple[str, ...] | None) -> str | None:
        v = _q(request, name)
        if v is None:
            return None
        if allowed is not None:
            if v not in allowed:
                raise UiError(400, "BAD_REQUEST", f"{name} must be one of {', '.join(allowed)}")
        elif not _FILTER_RE.match(v):
            raise UiError(400, "BAD_REQUEST", f"{name} has invalid characters")
        return v

    # ------------------------------------------------------------------ 페이지 틀
    NAV = [("/ui", "Overview"), ("/ui/signals", "Signals"), ("/ui/orders", "Orders"), ("/ui/reports", "Reports"),
           ("/ui/ingress", "Ingress"), ("/ui/signal-log", "Signal log"), ("/ui/performance", "Performance"), ("/ui/accounts", "Accounts"),
           ("/ui/secrets", "Secrets"), ("/ui/controls", "Controls")]

    def _head(title: str, refresh: int | None = None) -> str:
        meta = f'<meta http-equiv="refresh" content="{int(refresh)}">' if refresh else ""
        return (f'<!doctype html><html lang="en"><head><meta charset="utf-8">'
                f'<meta name="viewport" content="width=device-width, initial-scale=1">'
                f"<title>lake-executor — {_esc(title)}</title>{meta}<style>{CSS}</style></head>")

    def _page(title: str, body: str, *, active: str, session: Session, flash: str | None = None,
              error: str | None = None, refresh: int | None = None) -> str:
        nav = "".join(f'<a href="{_esc(href)}"{" class=\"active\"" if href == active else ""}>{_esc(label)}</a>'
                      for href, label in NAV)
        logout = (f'<form method="post" action="/ui/logout"><input type="hidden" name="{CSRF_FIELD}" '
                  f'value="{_esc(session.csrf)}"><button type="submit">Logout</button></form>')
        banners = ""
        if rcv._halted(settings):
            banners += ('<div class="banner halt">HALT is set — new signals are rejected (OPERATOR_HALT). '
                        '<a href="/ui/controls">Resume on Controls</a></div>')
        pend = app.state.ui_pending
        if pend:
            banners += (f'<div class="banner pending">restart required — changed on disk: '
                        f'{_esc(",".join(pend["keys"]))} ({_esc(_fmt_ms(pend["at_ms"]))}). '
                        f'The running process still uses the old values. <a href="/ui/controls">Controls → Apply &amp; restart</a></div>')
        if flash:
            banners += f'<div class="banner flash">{_esc(flash)}</div>'
        if error:
            banners += f'<div class="banner error">{_esc(error)}</div>'
        now = now_ms()
        foot = (f"now {_esc(_fmt_ms(now))} · process started {_esc(_fmt_ms(started_ms))} "
                f"(uptime {_esc(_fmt_uptime(now - started_ms))}) · session expires {_esc(_fmt_ms(session.exp_ms))}")
        return (f"{_head(title, refresh)}<body><header><span class=\"brand\">lake-executor</span><nav>{nav}</nav>{logout}</header>"
                f"{banners}<main><h1>{_esc(title)}</h1>{body}</main><footer>{foot}</footer></body></html>")

    def _login_page(message: str | None = None, status: int = 200) -> HTMLResponse:
        msg = f'<p class="warn">{_esc(message)}</p>' if message else ""
        body = (f"{_head('login')}<body><form method=\"post\" action=\"/ui/login\" class=\"login\">"
                f"<h1>lake-executor</h1><p>Enter ADMIN_TOKEN</p>{msg}"
                f'<input type="password" name="token" autocomplete="off" autofocus>'
                f'<button type="submit">Log in</button></form></body></html>')
        return _html(status, body)

    def _error(status: int, code: str, message: str = "", session: Session | None = None) -> Response:
        if session is None:
            body = f"{_head(code)}<body><main><h1>{_esc(code)}</h1><p>{_esc(message)}</p></main></body></html>"
            return _html(status, body)
        body = f'<p class="warn">{_esc(message)}</p><p><a href="/ui">back to overview</a></p>'
        return _html(status, _page(code, body, active="", session=session, error=f"{status} {code}"))

    def _ui_error(e: UiError, session: Session | None) -> Response:
        if e.as_json:
            body: dict[str, Any] = {"error": e.code}
            if e.message:
                body["message"] = e.message
            return _json(e.status, body)
        return _error(e.status, e.code, e.message, session)

    # ------------------------------------------------------------------ 게이트 / 폼
    def _gate(request: Request) -> Session | Response:
        """404(토큰 미설정) → 세션(GET: 로그인으로 303, POST: 401) → Session."""
        if not _token():
            return _json(404, {"error": "NOT_FOUND"})
        sess = codec.parse(request.cookies.get(COOKIE_NAME), now_ms())
        if sess is None:
            if request.method == "GET":
                return _redirect(f"{UI_PREFIX}/login")
            return _text(401, "UNAUTHORIZED")
        return sess

    async def _form(request: Request) -> dict[str, str] | Response:
        ct = (request.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
        if ct != "application/x-www-form-urlencoded":
            return _text(415, "UNSUPPORTED_MEDIA_TYPE")
        cl = rcv._content_length(request)
        if cl is not None and cl > FORM_MAX_BYTES:
            return _text(413, "PAYLOAD_TOO_LARGE")
        raw = await rcv._read_body_capped(request, FORM_MAX_BYTES)
        if raw is None:
            return _text(413, "PAYLOAD_TOO_LARGE")
        try:
            text = raw.decode("utf-8", "strict")
        except UnicodeDecodeError:
            return _text(400, "BAD_ENCODING")
        try:
            pairs = urllib.parse.parse_qsl(text, keep_blank_values=True, max_num_fields=64)
        except ValueError:
            return _text(400, "BAD_FORM")
        return {str(k): str(v) for k, v in pairs}

    async def _post_prelude(request: Request) -> tuple[dict[str, str], Session, str] | Response:
        """POST 공통: 게이트 → 폼 → CSRF (실패는 토큰 실패와 같은 IP 예산을 소모)."""
        g = _gate(request)
        if isinstance(g, Response):
            return g
        form = await _form(request)
        if isinstance(form, Response):
            return form
        ip = rcv._client_ip(request)
        given = form.get(CSRF_FIELD, "")
        if not given or not hmac.compare_digest(given.encode("utf-8"), g.csrf.encode("utf-8")):
            admin_throttle.fail(ip)
            log.warning("ui: csrf failure from %s", ip)
            return _text(403, "CSRF")
        return form, g, ip

    def _run(fn: Callable[[], Response], session: Session | None) -> Response:
        try:
            return fn()
        except UiError as e:
            return _ui_error(e, session)

    # ------------------------------------------------------------------ 로그인 / 로그아웃
    @app.get(f"{UI_PREFIX}/login")
    def ui_login_get(request: Request):
        if not _token():
            return _json(404, {"error": "NOT_FOUND"})
        if codec.parse(request.cookies.get(COOKIE_NAME), now_ms()) is not None:
            return _redirect(UI_PREFIX)
        return _login_page()

    def _login_impl(given: str, ip: str, secure: bool) -> Response:
        if admin_throttle.blocked(ip):
            log.warning("ui: login blocked (too many attempts) from %s", ip)
            return _login_page("too many attempts — wait a minute and retry", 401)
        token = _token()
        if not given or not hmac.compare_digest(given.encode("utf-8"), token.encode("utf-8")):
            admin_throttle.fail(ip)
            log.warning("ui: bad login from %s", ip)
            return _login_page("invalid token", 401)
        sess = codec.issue(now_ms())
        log.warning("ui: login from %s", ip)
        return _redirect(UI_PREFIX, cookie=_cookie_header(sess.cookie, secure))

    @app.post(f"{UI_PREFIX}/login")
    async def ui_login_post(request: Request):
        if not _token():
            return _json(404, {"error": "NOT_FOUND"})
        form = await _form(request)
        if isinstance(form, Response):
            return form
        # 토큰은 폼 본문에서만 받는다 (쿼리/헤더 무시)
        return await run_in_threadpool(_login_impl, form.get("token", ""), rcv._client_ip(request), _is_https(request))

    @app.post(f"{UI_PREFIX}/logout")
    async def ui_logout(request: Request):
        pre = await _post_prelude(request)
        if isinstance(pre, Response):
            return pre
        _form_, session, ip = pre
        codec.revoke(session.sid)
        log.info("ui: logout from %s", ip)
        return _redirect(f"{UI_PREFIX}/login", cookie=_clear_cookie_header())

    # ------------------------------------------------------------------ 개요
    def _counts_table(rows: list[dict], key: str) -> str:
        return _table(["mode", key, "n"], [[r["mode"], _status(r[key]), r["n"]] for r in rows], empty="(no rows)")

    def _check_summary(name: str) -> Any:
        res = (app.state.ui_check_results or {}).get(name)
        if not res:
            return _Raw('<span class="muted">never</span>')
        state = "ok" if res.get("ok") else ("skipped" if res.get("ok") is None else "fail")
        return _Raw(f"{_status(state)} {_esc(_fmt_ms(res.get('at_ms')))}")

    def _ledger_summary() -> Any:
        """원장 백엔드(sqlite 경로 / postgres host — 비밀번호 없음) + 왕복 시간 + 재접속 횟수."""
        desc = getattr(store, "describe", None)
        text = desc() if callable(desc) else "sqlite"
        try:
            ping = getattr(store, "ping_ms", None)
            rtt = f" rtt {ping():.0f} ms" if callable(ping) else ""
            state = "ok"
        except Exception as e:  # noqa: BLE001
            rtt, state = f" UNAVAILABLE ({type(e).__name__})", "fail"
        rec = int(getattr(store, "reconnects", 0) or 0)
        return _Raw(f"{_status(state)} {_esc(text)}{_esc(rtt)}" + (f" reconnects={rec}" if rec else ""))

    def _history_summary() -> Any:
        """history.py 동기화 상태: 계정별 마지막 동기화 시각 / 누적 적재 행 / 오류 (Performance 페이지 링크)."""
        hist = rcv._svc(services, "history")
        link = f' · <a href="{UI_PREFIX}/performance">Performance</a>'
        if hist is None:
            return _Raw('<span class="muted">disabled (history.enabled=false)</span>' + link)
        stats = hist.snapshot() if hasattr(hist, "snapshot") else {}
        if not stats:
            return _Raw(f"{_status('ok')} running, nothing synced yet{link}")
        state = "fail" if any(v.get("errors") for v in stats.values()) else "ok"
        parts = [f"{k} {_fmt_ms(v.get('last_sync_ms')) or 'never'} +{sum(v['inserted'].values())}"
                 + (f" errors={v['errors']} ({v.get('last_error', '')})" if v.get("errors") else "")
                 for k, v in sorted(stats.items())]
        return _Raw(f"{_status(state)} {_esc(' · '.join(parts))}{link}")

    @app.get(UI_PREFIX)
    def ui_overview(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g
        executor = _executor()
        ok, reason = settings.live_execution_possible()
        paths = _paths()
        inconsistent = rcv._inconsistent_map(store, settings)
        notes = rcv._inconsistent_notes(store, settings)
        missing: dict[str, int] = {}
        for m in MODES:
            try:
                missing[m] = len(executor.protection_missing(m, None)) if executor is not None else 0
            except Exception as e:  # noqa: BLE001
                log.warning("ui: protection_missing(%s) failed: %s", m, type(e).__name__)
                missing[m] = -1
        try:
            rejections = app.state.ingress_stats.snapshot()
        except Exception:  # noqa: BLE001
            rejections = {}
        pend = app.state.ui_pending
        flags = [
            ("routing", getattr(settings, "routing", "fanout")),
            ("live.enabled (in-process)", _status(str(bool(settings.live_enabled)).lower())),
            ("LAKE_SIGNAL_SECRET_LIVE", mask((settings.secrets.signal_secret or {}).get("live", ""))),
            ("live_execution_possible", _Raw(f"{_status(str(ok).lower())} {_esc(reason or '')}")),
            ("test.simulate_fills", str(bool(getattr(settings, "test_simulate_fills", False))).lower()),
            ("halted", _status(str(rcv._halted(settings)).lower())),
            ("inconsistent", _Raw(" ".join(f"{_esc(m)}={_status(str(v).lower())}" for m, v in inconsistent.items()))),
            ("inconsistent_note", "; ".join(f"{m}: {n}" for m, n in notes.items() if n) or "-"),
            ("protection_missing", " ".join(f"{m}={n}" for m, n in missing.items())),
            ("ingress_rejections (pre-auth counters)", " ".join(f"{k}={v}" for k, v in sorted(rejections.items())) or "0"),
            ("process started", f"{_fmt_ms(started_ms)} (uptime {_fmt_uptime(now_ms() - started_ms)})"),
            ("pending restart", (",".join(pend["keys"]) + " — restart required") if pend else "no"),
            ("ledger", _ledger_summary()),
            ("trade history sync", _history_summary()),
            ("expired actions still executed", ", ".join(getattr(settings, "expired_actions_execute", None) or []) or "none"),
            ("editing", (f"env={paths.env} config={paths.config}" if paths
                         else "editing disabled (serve was not started with known --env/--config)")),
        ]
        rows = []
        for acct in getattr(settings, "accounts", None) or []:
            a_ok, a_reason = settings.live_execution_possible(acct)
            lots = []
            for m in MODES:
                try:
                    lots.append(f"{m}={len(store.open_lots(m, acct.name))}")
                except Exception:  # noqa: BLE001
                    lots.append(f"{m}=?")
            report = " ".join(
                f"{m}:url={'y' if (acct.report_url or {}).get(m) else 'n'}/secret={'y' if (acct.report_secret or {}).get(m) else 'n'}"
                for m in MODES)
            rows.append([
                _link(f"{UI_PREFIX}/accounts", acct.name), acct.exchange,
                _status(str(bool(acct.enabled)).lower()), str(bool(acct.report)).lower(),
                f"{acct.position_mode} x{acct.leverage} {acct.margin_mode or '-'}", str(bool(acct.testnet)).lower(),
                f"key: {mask(acct.api_key)}" + (f" / pass: {mask(acct.api_passphrase)}" if acct.exchange == "okx" else ""),
                _Raw(f"{_status('ok' if a_ok else 'fail')} {_esc(a_reason or '')}"),
                report, " ".join(lots), _check_summary(acct.name),
            ])
        body = "<h2>Flags</h2>" + _kv(flags)
        body += "<h2>Accounts</h2>" + _table(
            ["name", "exchange", "enabled", "report", "mode/lev/margin", "testnet", "keys (in-process)",
             "live", "report test/live", "open lots", "last check"], rows)
        body += "<h2>Signals (mode × status)</h2>" + _counts_table(store.signal_counts(), "status")
        body += "<h2>Reports (mode × state)</h2>" + _counts_table(store.report_counts(), "state")
        pending_n = sum(int(r["n"]) for r in store.report_counts() if r["state"] == "pending")
        body += f'<p class="muted">pending reports: {pending_n}. This page refreshes every 30 s (<a href="/ui?refresh=0">stop</a>).</p>'
        refresh = None if request.query_params.get("refresh") == "0" else 30
        return _html(200, _page("Overview", body, active=UI_PREFIX, session=session, flash=_flash_of(request, session),
                                refresh=refresh))

    # ------------------------------------------------------------------ 신호
    @app.get(f"{UI_PREFIX}/signals")
    def ui_signals(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g

        def _impl() -> Response:
            mode = _filter(request, "mode", MODES)
            status = _filter(request, "status", SIGNAL_STATUSES)
            limit, offset = _paging(request)
            rows = store.list_signals(mode, status, limit + 1, offset)
            has_more = len(rows) > limit
            rows = rows[:limit]
            table = _table(
                ["received_at", "mode", "event_id", "position_id", "seq", "action", "strategy", "leg", "idx", "qty_btc",
                 "status", "reason_code", "processed_at", "note"],
                [[_fmt_ms(r["received_at_ms"]), r["mode"], _event_link(r["event_id"], r["mode"]), r["position_id"],
                  r["event_sequence"], r["action"], r["strategy"], r["leg"], r["position_idx"], r["qty_btc"],
                  _status(r["status"]), r["reason_code"] or "", _fmt_ms(r["processed_at_ms"]), r["note"] or ""]
                 for r in rows])
            filters = ('<p class="muted">filters: mode=' + "|".join(MODES) + " status=" + "|".join(SIGNAL_STATUSES)
                       + " (query string)</p>")
            body = filters + table + _pager(f"{UI_PREFIX}/signals", {"mode": mode, "status": status}, limit, offset, has_more)
            return _html(200, _page("Signals", body, active=f"{UI_PREFIX}/signals", session=session))
        return _run(_impl, session)

    @app.get(f"{UI_PREFIX}/signals/{{event_id}}")
    def ui_signal_detail(request: Request, event_id: str):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g

        def _impl() -> Response:
            if not rcv.ID_RE.fullmatch(event_id or ""):
                raise UiError(404, "NOT_FOUND", "event_id has an invalid form")
            mode_q = _filter(request, "mode", MODES)
            row = store.get_signal(event_id, mode_q)
            if row is None:
                raise UiError(404, "NOT_FOUND", "no such signal")
            mode = row["mode"]
            row = dict(row)
            raw = row.pop("raw_body", None)
            if isinstance(raw, memoryview):
                raw = raw.tobytes()
            pairs = []
            for k, v in row.items():
                if k.endswith("_ms"):
                    v = _fmt_ms(v)
                elif k == "status":
                    v = _status(v)
                pairs.append((k, v))
            body = "<h2>Signal</h2>" + _kv(pairs)
            try:
                payload = json.loads(bytes(raw or b"").decode("utf-8"))
                body += "<h2>Payload</h2>" + _json_pre(payload)
            except (ValueError, UnicodeDecodeError):
                body += f'<h2>Payload</h2><p class="muted">unparseable (sha256 {_esc(row.get("body_sha256"))})</p>'
            runs = store.get_runs(mode, event_id)
            body += "<h2>Runs (per account)</h2>" + _table(
                ["account", "status", "reason_code", "processed_at", "note"],
                [[r["account"], _status(r["status"]), r["reason_code"] or "", _fmt_ms(r["processed_at_ms"]), r["note"] or ""]
                 for r in runs])
            lot_rows = []
            for r in runs:
                lot = store.get_lot(mode, r["account"], row["position_id"])
                if lot:
                    lot_rows.append([r["account"], lot["status"], lot["qty"], lot.get("avg_entry"), lot.get("stop_loss"),
                                     json.dumps(lot.get("take_profit")), lot.get("protection_revision"),
                                     _fmt_ms(lot.get("updated_at_ms"))])
            body += "<h2>Lot (mode, account, position_id)</h2>" + _table(
                ["account", "status", "qty", "avg_entry", "stop_loss", "take_profit", "protection_revision", "updated_at"],
                lot_rows, empty="(no lot)")
            orders = store.orders_for_event(mode, event_id)
            order_rows = []
            seen_fill_ids: set[tuple[str, str]] = set()
            for o in orders:
                order_rows.append([_fmt_ms(o["created_at_ms"]), o["account"], o["order_link_id"], o["order_id"] or "",
                                   o["purpose"], o["side"], o["qty"], o["reduce_only"], o["trigger_price"],
                                   _status(o["status"]), _fmt_ms(o["updated_at_ms"])])
                for f in store.fills_for_order(o["order_link_id"], o["account"]):
                    seen_fill_ids.add((f["account"], f["exec_id"]))
                    order_rows.append(["", f["account"], _Raw(f'<span class="muted">fill</span> {_esc(f["exec_id"])}'),
                                       f["order_id"] or "", "", "", f["qty"], "", f["price"], _status("filled"),
                                       _fmt_ms(f["exec_time_ms"])])
            body += "<h2>Orders and fills</h2>" + _table(
                ["created_at", "account", "order_link_id / fill exec_id", "order_id", "purpose", "side", "qty",
                 "reduce_only", "trigger/price", "status", "updated/exec_time"], order_rows, empty="(no orders)")
            extra = [f for f in store.fills_for_event(mode, event_id) if (f["account"], f["exec_id"]) not in seen_fill_ids]
            if extra:
                body += "<h3>Fills not tied to a listed order</h3>" + _table(
                    ["account", "exec_id", "order_link_id", "qty", "price", "exec_time", "applied", "reported"],
                    [[f["account"], f["exec_id"], f["order_link_id"] or "", f["qty"], f["price"], _fmt_ms(f["exec_time_ms"]),
                      f["applied"], f["reported"]] for f in extra])
            reports = store.reports_for_event(mode, event_id, 50)
            body += "<h2>Reports carrying this event_id</h2>" + _table(
                ["report_id", "account", "sequence", "kind", "execution.status", "state", "http_status", "attempts",
                 "sent_at", "note"],
                [[r["report_id"], r["account"], r["sequence"], r["kind"], _status(r.get("execution_status") or ""),
                  _status(r["state"]), r["http_status"], r["attempts"], _fmt_ms(r["sent_at_ms"]), r["note"] or ""]
                 for r in reports], empty="(no reports)")
            ingress = store.ingress_for_event(event_id, 20)
            body += "<h2>Ingress log for this event_id</h2>" + _table(
                ["id", "received_at", "code", "body_sha256", "note"],
                [[r["id"], _fmt_ms(r["received_at_ms"]), _status(r["code"]), (r["body_sha256"] or "")[:16], r["note"] or ""]
                 for r in ingress], empty="(no duplicate/conflict/expired entries)")
            return _html(200, _page(f"Signal {event_id}", body, active=f"{UI_PREFIX}/signals", session=session))
        return _run(_impl, session)

    # ------------------------------------------------------------------ 주문 / 회신 / 접수 로그
    @app.get(f"{UI_PREFIX}/orders")
    def ui_orders(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g

        def _impl() -> Response:
            mode = _filter(request, "mode", MODES)
            account = _filter(request, "account", tuple(account_names) or None)
            status = _filter(request, "status", None)
            limit, offset = _paging(request)
            rows = store.list_orders(mode, account, status, limit + 1, offset)
            has_more = len(rows) > limit
            rows = rows[:limit]
            table = _table(
                ["created_at", "mode", "account", "order_link_id", "order_id", "event_id", "position_id", "purpose", "side",
                 "qty", "reduce_only", "trigger_price", "status", "updated_at"],
                [[_fmt_ms(o["created_at_ms"]), o["mode"], o["account"], o["order_link_id"], o["order_id"] or "",
                  _event_link(o["event_id"], o["mode"]), o["position_id"], o["purpose"], o["side"], o["qty"], o["reduce_only"],
                  o["trigger_price"], _status(o["status"]), _fmt_ms(o["updated_at_ms"])] for o in rows])
            body = ('<p class="muted">filters: mode, account, status (query string)</p>' + table
                    + _pager(f"{UI_PREFIX}/orders", {"mode": mode, "account": account, "status": status}, limit, offset, has_more))
            return _html(200, _page("Orders", body, active=f"{UI_PREFIX}/orders", session=session))
        return _run(_impl, session)

    @app.get(f"{UI_PREFIX}/reports")
    def ui_reports(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g

        def _impl() -> Response:
            mode = _filter(request, "mode", MODES)
            account = _filter(request, "account", tuple(account_names) or None)
            state = _filter(request, "state", REPORT_STATES)
            limit, offset = _paging(request)
            rows = store.list_reports(mode, account, state, limit + 1, offset)
            has_more = len(rows) > limit
            rows = rows[:limit]
            table = _table(
                ["created_at", "mode", "account", "sequence", "report_id", "kind", "state", "http_status", "attempts",
                 "sent_at", "body bytes", "note"],
                [[_fmt_ms(r["created_at_ms"]), r["mode"], r["account"], r["sequence"], r["report_id"], r["kind"],
                  _status(r["state"]), r["http_status"], r["attempts"], _fmt_ms(r["sent_at_ms"]), r["body_len"],
                  r["note"] or ""] for r in rows])
            body = ('<p class="muted">filters: mode, account, state=' + "|".join(REPORT_STATES)
                    + ". Report bodies are never shown here (size only).</p>" + table
                    + _pager(f"{UI_PREFIX}/reports", {"mode": mode, "account": account, "state": state}, limit, offset, has_more))
            return _html(200, _page("Reports", body, active=f"{UI_PREFIX}/reports", session=session))
        return _run(_impl, session)

    @app.get(f"{UI_PREFIX}/ingress")
    def ui_ingress(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g

        def _impl() -> Response:
            limit, offset = _paging(request)
            rows = store.recent_ingress(limit + 1, offset)
            has_more = len(rows) > limit
            rows = rows[:limit]
            table = _table(
                ["id", "received_at", "code", "event_id", "body_sha256", "note"],
                [[r["id"], _fmt_ms(r["received_at_ms"]), _status(r["code"]), _event_link(r["event_id"], None),
                  (r["body_sha256"] or "")[:16], r["note"] or ""] for r in rows])
            try:
                counters = app.state.ingress_stats.snapshot()
            except Exception:  # noqa: BLE001
                counters = {}
            ctable = _table(["code", "count"], [[k, v] for k, v in sorted(counters.items())], empty="(no pre-auth rejections)")
            body = ("<h2>Post-signature ingress log (DUPLICATE / conflicts / expired / schema)</h2>" + table
                    + _pager(f"{UI_PREFIX}/ingress", {}, limit, offset, has_more)
                    + "<h2>Pre-auth rejections</h2>"
                    + '<p class="muted">pre-auth rejections are counters only (reset on restart); nothing is written to the DB.</p>'
                    + ctable)
            return _html(200, _page("Ingress", body, active=f"{UI_PREFIX}/ingress", session=session))
        return _run(_impl, session)

    # ------------------------------------------------------------------ 라이브 신호 로그 (백테스트용 데이터셋)
    def _signal_log_filters(request: Request) -> tuple[str | None, int | None, int | None]:
        mode = _filter(request, "mode", tuple(MODES))
        since = until = None
        for name in ("since", "until"):
            v = _q(request, name)
            if v:
                try:
                    from .main import parse_when
                    ms = parse_when(v)
                except ValueError:
                    raise UiError(400, "BAD_REQUEST", f"{name}: use YYYY-MM-DD, YYYY-MM-DDTHH:MM or Unix ms") from None
                if name == "since":
                    since = ms
                else:
                    until = ms
        return mode, since, until

    @app.get(f"{UI_PREFIX}/signal-log")
    def ui_signal_log(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g

        def _impl() -> Response:
            mode, since, until = _signal_log_filters(request)
            limit, offset = _paging(request)
            rows = store.signal_log_rows(mode, since, until, limit=limit + 1, offset=offset)
            has_more = len(rows) > limit
            rows = rows[:limit]
            slog = rcv._svc(services, "signal_log")
            stats = slog.snapshot() if slog is not None and hasattr(slog, "snapshot") else {}
            table = _table(
                ["received_at", "mode", "event_id", "action", "leg", "qty_btc", "ref price", "mark @receipt", "last @receipt",
                 "strategy", "ingest", "sl", "tp"],
                [[_fmt_ms(r["received_at_ms"]), r["mode"], _event_link(r["event_id"], r["mode"]), r["action"], r["leg"],
                  r.get("qty_btc"), r.get("reference_price"), r.get("mark_price"), r.get("last_price"), r.get("strategy"),
                  _status(r.get("ingest_result")), r.get("stop_loss"), json.dumps(r.get("take_profit")) if r.get("take_profit") else ""]
                 for r in rows])
            q = {"mode": mode, "since": _q(request, "since"), "until": _q(request, "until")}
            dl = urllib.parse.urlencode({k: v for k, v in q.items() if v})
            filters = ("<form method=\"get\" class=\"filters\">"
                       "<label>mode " + _select("mode", [(m, m) for m in MODES], blank="(all)") + "</label>"
                       '<label>since <input type="text" name="since" placeholder="YYYY-MM-DD" autocomplete="off"></label>'
                       '<label>until <input type="text" name="until" placeholder="YYYY-MM-DD" autocomplete="off"></label>'
                       '<button type="submit">Filter</button></form>')
            body = ("<h2>Live signal log (append-only, written off the trade path)</h2>"
                    f"<p>total rows: {store.signal_log_count(mode)} · writer: "
                    f"{_esc(' '.join(f'{k}={v}' for k, v in stats.items() if k not in ('jsonl',)))}</p>"
                    f'<p><a href="{UI_PREFIX}/signal-log.csv?{_esc(dl)}">Download CSV</a> (joined with our runs/fills; '
                    "columns = signal_log.EXPORT_COLUMNS) · CLI: <code>python -m lake_executor export --format csv</code></p>"
                    + filters + table + _pager(f"{UI_PREFIX}/signal-log", q, limit, offset, has_more))
            return _html(200, _page("Signal log", body, active=f"{UI_PREFIX}/signal-log", session=session))
        return _run(_impl, session)

    @app.get(f"{UI_PREFIX}/signal-log.csv")
    def ui_signal_log_csv(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g

        def _impl() -> Response:
            from .signal_log import rows_to_csv
            mode, since, until = _signal_log_filters(request)
            rows = store.export_signal_rows(mode, since, until, limit=50000)
            text = rows_to_csv(rows)
            name = f"signal_log_{mode or 'all'}_{now_ms()}.csv"
            resp = Response(content=text, media_type="text/csv; charset=utf-8",
                            headers={"Content-Disposition": f'attachment; filename="{name}"'})
            return _with_headers(resp)
        return _run(_impl, session)

    # ------------------------------------------------------------------ 성과: 계정 트레이드 히스토리 + 시드 대비 PnL/ROI (metrics.py)
    def _perf_filters(request: Request) -> tuple[str, str | None, int | None, int | None]:
        names = tuple(a.name for a in settings.accounts)
        mode = _filter(request, "mode", tuple(MODES)) or ("live" if getattr(settings, "live_enabled", False) else "test")
        account = _filter(request, "account", names) if names else None
        _, since, until = _signal_log_filters(request)
        return mode, account, since, until

    def _perf_rows(mode: str, account: str | None, since: int | None, until: int | None) -> list[dict]:
        from .metrics import performance_all
        return performance_all(store, settings, mode, since, until, accounts=[account] if account else None)

    def _pct(v: Any) -> str:
        return "-" if v is None else f"{float(v) * 100:+.2f}%"

    def _num(v: Any, nd: int = 4) -> str:
        return "-" if v is None else f"{float(v):,.{nd}f}"

    def _perf_card(r: dict, session: Session) -> str:
        c, eq, led = r["closed"], r["equity"], r["ledger"]
        sync = r.get("sync") or {}
        head = _kv([
            ("seed (USDT)", f"{_num(r['seed'])} ({r.get('seed_source') or 'no equity snapshot yet'})"),
            ("equity now", f"{_num(r['equity_now'])} @ {_fmt_ms(r.get('equity_at_ms'))}" if r.get("equity_now") is not None else "-"),
            ("net deposits", f"{_num(r['net_deposits'])} (in {_num(r['deposits'])} / out {_num(r['withdrawals'])})"),
            ("PnL total (equity - net deposits)", _Raw(f"<b>{_esc(_num(r['pnl_total']))}</b>")),
            ("ROI vs seed", _Raw(f"<b>{_esc(_pct(r['roi_vs_seed']))}</b>")),
            ("ROI vs net deposits", _pct(r["roi_vs_net_deposits"])),
            ("unrealised pnl", _num(r.get("unrealised_pnl"))),
            ("closed pnl (exchange)", f"{_num(c['realized_closed_pnl'])} over {c['trades']} trades"),
            ("win / loss / win rate", f"{c['wins']} / {c['losses']} / {_pct(c['win_rate'])}"),
            ("profit factor", _num(c["profit_factor"], 2)),
            ("avg win / avg loss", f"{_num(c['avg_win'])} / {_num(c['avg_loss'])}"),
            ("largest win / loss", f"{_num(c['largest_win'])} / {_num(c['largest_loss'])}"),
            ("fees / funding", f"{_num(r['fees'])} / {_num(r['funding'])}"),
            ("executions", r["executions"]),
            ("equity snapshots", f"{eq['points']} (peak {_num(eq.get('peak'))}, max drawdown {_num(eq.get('max_drawdown'))} = {_pct(eq.get('max_drawdown_pct'))})"),
            ("ledger realized (our fills, avg-cost)", f"{_num(led['realized'])} · closed positions {led['positions_closed']} · open {len(led['open_positions'])} · fills {led['fills']}"),
        ])
        strat = _table(["strategy", "realized pnl"], [[k, _num(v)] for k, v in sorted(led["by_strategy"].items())],
                       empty="(no closed positions in our ledger)")
        openp = _table(["position_id", "strategy", "leg", "qty", "avg entry"],
                       [[o["position_id"], o["strategy"], o["leg"], o["qty"], _num(o["avg_entry"], 2)] for o in led["open_positions"]],
                       empty="(no open positions in our ledger)")
        syncs = _table(["kind", "last record", "synced at"],
                       [[k, _fmt_ms(v["last_ts_ms"]) if v["last_ts_ms"] else "-", _fmt_ms(v["updated_at_ms"])] for k, v in sorted(sync.items())],
                       empty="(never synced — serve syncs every history.sync_interval_s; or Backfill below)")
        daily = r["daily"][-31:]
        dtable = _table(["date (UTC)", "closed pnl", "trades", "fees", "funding", "deposits", "withdrawals", "executions", "equity close"],
                        [[d["date"], _num(d["closed_pnl"]), d["trades"], _num(d["fees"]), _num(d["funding"]), _num(d["deposits"]),
                          _num(d["withdrawals"]), d["executions"], _num(d["equity_close"], 2)] for d in reversed(daily)],
                        empty="(no daily data)")
        default_since = datetime.fromtimestamp((now_ms() - 30 * 86400 * 1000) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        bf = _form_post(f"{UI_PREFIX}/performance/backfill", session.csrf,
                        f'<input type="hidden" name="mode" value="{_esc(r["mode"])}">'
                        f'<input type="hidden" name="account" value="{_esc(r["account"])}">'
                        f'<label>since (UTC) <input type="text" name="since" value="{default_since}" autocomplete="off"></label>'
                        '<p class="muted">re-reads executions / closed pnl / cashflow from the exchange since this date and inserts '
                        'what is missing (idempotent). Runs in the background; refresh this page.</p>',
                        "Backfill now")
        return (f'<div class="card"><h2>{_esc(r["mode"])}/{_esc(r["account"])} '
                f'<span class="muted">{_esc(r.get("exchange") or "")} {_esc(r.get("symbol") or "")} x{_esc(r.get("leverage"))}</span></h2>'
                f"{head}<h3>Realized by strategy (our ledger)</h3>{strat}<h3>Open positions (our ledger)</h3>{openp}"
                f"<h3>Sync state</h3>{syncs}<h3>Daily (last 31 days)</h3>{dtable}{bf}</div>")

    @app.get(f"{UI_PREFIX}/performance")
    def ui_performance(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g

        def _impl() -> Response:
            mode, account, since, until = _perf_filters(request)
            rows = _perf_rows(mode, account, since, until)
            hist = rcv._svc(services, "history")
            hstats = hist.snapshot() if hist is not None and hasattr(hist, "snapshot") else {}
            q = {"mode": mode, "account": account, "since": _q(request, "since"), "until": _q(request, "until")}
            qs = urllib.parse.urlencode({k: v for k, v in q.items() if v})
            filters = ("<form method=\"get\" class=\"filters\">"
                       "<label>mode " + _select("mode", [(m, m) for m in MODES]) + "</label>"
                       "<label>account " + _select("account", [(a.name, a.name) for a in settings.accounts], blank="(all)") + "</label>"
                       '<label>since <input type="text" name="since" placeholder="YYYY-MM-DD" autocomplete="off"></label>'
                       '<label>until <input type="text" name="until" placeholder="YYYY-MM-DD" autocomplete="off"></label>'
                       '<button type="submit">Filter</button></form>')
            status = ("history sync: " + ("disabled (history.enabled=false)" if hist is None else
                      (" · ".join(f"{k}: last {_fmt_ms(v.get('last_sync_ms')) or 'never'} +{sum(v['inserted'].values())} rows"
                                  f"{' errors=' + str(v['errors']) + ' (' + v['last_error'] + ')' if v.get('errors') else ''}"
                                  for k, v in sorted(hstats.items())) or "running, nothing synced yet")))
            body = ("<h2>Account performance (seed vs equity, exchange trade history)</h2>"
                    f'<p class="muted">{_esc(status)}</p>'
                    f'<p><a href="{UI_PREFIX}/api/performance.json?{_esc(qs)}">JSON</a> (same numbers, for a dashboard) · '
                    "CLI: <code>python -m lake_executor performance --mode live</code> · "
                    "<code>python -m lake_executor backfill --mode live --since YYYY-MM-DD</code></p>"
                    + filters + ("".join(_perf_card(r, session) for r in rows) or '<p class="muted">no accounts configured</p>'))
            return _html(200, _page("Performance", body, active=f"{UI_PREFIX}/performance", session=session,
                                    flash=_flash_of(request, session)))
        return _run(_impl, session)

    @app.get(f"{UI_PREFIX}/api/performance.json")
    def ui_performance_json(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g

        def _impl() -> Response:
            mode, account, since, until = _perf_filters(request)
            hist = rcv._svc(services, "history")
            return _json(200, {"mode": mode, "since_ms": since, "until_ms": until, "generated_at_ms": now_ms(),
                               "accounts": _perf_rows(mode, account, since, until),
                               "sync": hist.snapshot() if hist is not None and hasattr(hist, "snapshot") else None})
        return _run(_impl, session)

    def _do_backfill(form: dict[str, str], session: Session, ip: str) -> Response:
        from .main import parse_when
        mode = form.get("mode", "")
        account = form.get("account", "")
        if mode not in MODES:
            raise UiError(400, "BAD_REQUEST", "mode must be test or live")
        if account not in {a.name for a in settings.accounts}:
            raise UiError(404, "NOT_FOUND", f"unknown account {account!r}")
        try:
            since = parse_when(form.get("since", ""))
        except ValueError:
            raise UiError(400, "BAD_REQUEST", "since: use YYYY-MM-DD, YYYY-MM-DDTHH:MM or Unix ms") from None
        if since is None:
            raise UiError(400, "BAD_REQUEST", "since is required")
        hist = rcv._svc(services, "history")
        if hist is None:
            raise UiError(409, "HISTORY_DISABLED", "history sync is disabled (history.enabled=false)")
        if account not in (hist.sources.get(mode) or {}):
            raise UiError(409, "NO_SOURCE", f"no history source for {mode}/{account} (live needs real keys; test needs simulate_fills)")
        hist.backfill_async(mode, account, since)
        _audit(ip, f"backfill {mode}/{account}", f"since={since}", alert=False)
        return _redirect(f"{UI_PREFIX}/performance?mode={urllib.parse.quote(mode)}&account={urllib.parse.quote(account)}",
                         session=session, flash=f"backfill {mode}/{account} started (since {_fmt_ms(since)})")

    @app.post(f"{UI_PREFIX}/performance/backfill")
    async def ui_performance_backfill(request: Request):
        pre = await _post_prelude(request)
        if isinstance(pre, Response):
            return pre
        form, session, ip = pre
        return await run_in_threadpool(_run, lambda: _do_backfill(form, session, ip), session)

    # ------------------------------------------------------------------ .env 저장 공통
    def _file_env() -> dict[str, str]:
        paths = _paths()
        if paths is None:
            return {}
        try:
            return parse_env_file(paths.env)
        except (OSError, UnicodeDecodeError) as e:
            log.warning("ui: cannot read env file: %s", type(e).__name__)
            return {}

    def _apply_env_updates(updates: dict[str, str], ip: str, session: Session, redirect_to: str) -> Response:
        paths = _paths()
        if paths is None:
            raise UiError(409, "ENV_PATH_UNKNOWN", "serve was not started with known --env/--config", as_json=True)
        if not updates:
            return _redirect(redirect_to, flash="nothing changed (all fields empty = keep current)", session=session)
        with write_lock:   # 검증→쓰기→재검증→복원 전체를 직렬화 (겹친 제출이 서로의 임시 파일·백업을 건드리지 않게)
            validate_env_candidate(settings, paths, updates, app.state.ui_exchange_factory)
            try:
                changed = util.write_env_file(paths.env, updates)
            except (OSError, ValueError) as e:
                log.error("ui: env write failed: %s", type(e).__name__)
                raise UiError(500, "ENV_WRITE_FAILED", type(e).__name__) from None
            if not changed:
                return _redirect(redirect_to, flash="nothing changed (same values as on disk)", session=session)
            try:
                config_mod.load(paths.config, paths.env)
            except Exception as e:  # noqa: BLE001 - 쓰고 난 뒤의 재검증 실패: 백업 복원
                log.error("ui: post-write config.load failed (%s); restoring .env.bak", type(e).__name__)
                try:
                    with open(paths.env + ".bak", "r", encoding="utf-8", newline="") as f:
                        util.atomic_write_text(paths.env, f.read(), backup=False)
                except OSError as e2:
                    log.error("ui: restoring .env.bak failed: %s", type(e2).__name__)
                raise UiError(500, "ENV_WRITE_FAILED", f"post-write validation failed: {type(e).__name__}") from None
            # 재검증 통과 → 교체 전 값(이전 ADMIN_TOKEN·API 시크릿) 을 담은 .env.bak 은 디스크에 남기지 않는다
            try:
                os.remove(paths.env + ".bak")
            except OSError:
                pass
            _mark_pending(changed)
        keys = ",".join(changed)
        app.state.ui_audit.append((now_ms(), ip, "env updated", keys))
        log.warning("ui: env updated keys=%s by %s", keys, ip)
        _alert(f"[admin] secrets updated via dashboard: {keys}")
        flash = f"saved {keys} — restart required (Controls → Apply & restart)"
        if "ADMIN_TOKEN" in changed:
            flash += "; after the restart every session is invalid — log in again with the new ADMIN_TOKEN"
        return _redirect(redirect_to, flash=flash, session=session)

    # ------------------------------------------------------------------ 계정
    def _acct_or_404(name: str) -> Any:
        acct = settings.account_or_none(name) if hasattr(settings, "account_or_none") else None
        if acct is None:
            raise UiError(404, "NOT_FOUND", "unknown account")
        return acct

    def _check_block(name: str) -> str:
        res = (app.state.ui_check_results or {}).get(name)
        if not res:
            return '<p class="muted">last check: never</p>'
        state = "ok" if res.get("ok") else ("skipped" if res.get("ok") is None else "fail")
        lines = "".join(f"<li>{_esc(l)}</li>" for l in res.get("lines") or [])
        return f"<p>last check {_status(state)} at {_esc(_fmt_ms(res.get('at_ms')))}</p><ul>{lines}</ul>"

    @app.get(f"{UI_PREFIX}/accounts")
    def ui_accounts(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g
        paths = _paths()
        file_env = _file_env()
        cards = []
        for acct in getattr(settings, "accounts", None) or []:
            a_ok, a_reason = settings.live_execution_possible(acct)
            lots = []
            for m in MODES:
                try:
                    lots.append(f"{m}={len(store.open_lots(m, acct.name))}")
                except Exception:  # noqa: BLE001
                    lots.append(f"{m}=?")
            summary = _kv([
                ("exchange", f"{acct.exchange} ({acct.display_name})"), ("enabled", str(bool(acct.enabled)).lower()),
                ("report", str(bool(acct.report)).lower()), ("symbol", acct.symbol),
                ("position_mode / leverage / margin", f"{acct.position_mode} / {acct.leverage} / {acct.margin_mode or '-'}"),
                ("testnet", str(bool(acct.testnet)).lower()), ("qty_multiplier", acct.qty_multiplier),
                ("env_prefix", acct.env_prefix), ("live", _Raw(f"{_status('ok' if a_ok else 'fail')} {_esc(a_reason or '')}")),
                ("has_real_keys (in-process)", str(bool(acct.has_real_keys())).lower()),
                ("report url/secret (effective)", " ".join(
                    f"{m}:url={'y' if (acct.report_url or {}).get(m) else 'n'}/secret={'y' if (acct.report_secret or {}).get(m) else 'n'}"
                    for m in MODES)),
                ("open lots", " ".join(lots)),
            ])
            key_rows = []
            inproc = {"api_key": acct.api_key, "api_secret": acct.api_secret, "api_passphrase": acct.api_passphrase}
            for field, env_key in account_key_names(acct).items():
                file_state = mask(file_env.get(env_key, "")) if paths else "(env path unknown)"
                proc_state = mask(inproc[field])
                if paths and file_env.get(env_key, "") != (inproc[field] or ""):
                    file_state += " — restart pending" if file_env.get(env_key, "") else " — removed on disk, restart pending"
                key_rows.append([env_key, file_state, proc_state])
            keys_table = _table(["env key", "on disk (.env)", "in-process"], key_rows)
            fields = ('<label>API key <input type="password" name="api_key" autocomplete="off"></label>'
                      '<label>API secret <input type="password" name="api_secret" autocomplete="off"></label>')
            if acct.exchange == "okx":
                fields += '<label>passphrase <input type="password" name="api_passphrase" autocomplete="off"></label>'
            fields += '<p class="muted">empty field = keep current value. Saved to .env only; takes effect after restart.</p>'
            forms = ""
            if paths:
                forms += _form_post(f"{UI_PREFIX}/accounts/{urllib.parse.quote(acct.name, safe='')}/keys", session.csrf,
                                    fields, "Save keys")
            else:
                forms += '<p class="warn">editing disabled (serve was not started with known --env/--config)</p>'
            forms += _form_post(f"{UI_PREFIX}/accounts/{urllib.parse.quote(acct.name, safe='')}/check", session.csrf,
                                '<p class="muted">read-only connectivity check with the keys on disk (instrument / price / positions). No orders.</p>',
                                "Check")
            cards.append(f'<div class="card"><h2>{_esc(acct.name)}</h2>{summary}{keys_table}{_check_block(acct.name)}{forms}</div>')
        body = "".join(cards) or '<p class="muted">no accounts configured</p>'
        return _html(200, _page("Accounts", body, active=f"{UI_PREFIX}/accounts", session=session, flash=_flash_of(request, session)))

    def _do_account_keys(form: dict[str, str], session: Session, ip: str, name: str) -> Response:
        acct = _acct_or_404(name)
        mapping = account_key_names(acct)
        updates: dict[str, str] = {}
        for field, v in form.items():
            if field == CSRF_FIELD:
                continue
            if field not in mapping:
                raise UiError(400, "BAD_KEY", f"{field} is not a key field for {name}", as_json=True)
            if v == "":
                continue
            updates[mapping[field]] = v
        return _apply_env_updates(updates, ip, session, f"{UI_PREFIX}/accounts")

    @app.post(f"{UI_PREFIX}/accounts/{{name}}/keys")
    async def ui_account_keys(request: Request, name: str):
        pre = await _post_prelude(request)
        if isinstance(pre, Response):
            return pre
        form, session, ip = pre
        return await run_in_threadpool(_run, lambda: _do_account_keys(form, session, ip, name), session)

    def _do_account_check(form: dict[str, str], session: Session, ip: str, name: str) -> Response:
        from .exchange import ExchangeError  # 지연 import (web 은 거래소 모듈 없이도 import 된다)
        _acct_or_404(name)
        now = now_ms()
        last = app.state.ui_check_at.get(name, 0)
        if now - last < CHECK_MIN_INTERVAL_MS:
            raise UiError(429, "TOO_SOON", f"check for {name} ran {(now - last) // 1000}s ago; wait 10 s")
        app.state.ui_check_at[name] = now
        lines: list[str] = []
        ok: bool | None = True
        paths = _paths()
        acct = None
        try:
            cand = config_mod.load(paths.config, paths.env) if paths else settings
            acct = cand.account_or_none(name)
            if paths is None:
                lines.append("env path unknown — using in-process settings")
        except (config_mod.ConfigError, ValueError, TypeError) as e:
            lines.append(f"config error: {str(e)[:300]}")
            ok = False
        if acct is None and ok:
            lines.append("account missing from config on disk")
            ok = False
        elif ok and not acct.has_real_keys():
            lines.append("skipped (no real keys)")
            ok = None
        elif ok:
            ex = None
            try:
                ex = app.state.ui_exchange_factory(acct)
                lines.append("connect: ok")
            except ExchangeError as e:
                lines.append(f"connect: FAILED ({e.code})")
                ok = False
            except Exception as e:  # noqa: BLE001 - 원문은 절대 싣지 않는다
                lines.append(f"connect: FAILED ({type(e).__name__})")
                ok = False
            if ex is not None:
                steps: list[tuple[str, Callable[[], str]]] = [
                    ("instrument", lambda: " ".join(f"{k}={v}" for k, v in sorted((ex.instrument() or {}).items()))),
                    ("last/mark", lambda: f"{ex.last_price()} / {ex.mark_price()}"),
                    ("positions", lambda: (" ".join(f"idx{idx}:{p.get('side')}:{p.get('size')}" for idx, p in sorted((ex.positions() or {}).items()))
                                           or "none")),
                ]
                for label, fn in steps:
                    try:
                        lines.append(f"{label}: ok {fn()}")
                    except ExchangeError as e:
                        lines.append(f"{label}: FAILED ({e.code})")
                        ok = False
                    except Exception as e:  # noqa: BLE001
                        lines.append(f"{label}: FAILED ({type(e).__name__})")
                        ok = False
                close = getattr(ex, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:  # noqa: BLE001
                        pass
        app.state.ui_check_results[name] = {"at_ms": now, "ok": ok, "lines": lines}
        _audit(ip, f"account check {name}", "ok" if ok else ("skipped" if ok is None else "failed"), alert=False)
        return _redirect(f"{UI_PREFIX}/accounts", session=session,
                         flash=f"check {name}: {'ok' if ok else ('skipped' if ok is None else 'FAILED')}")

    @app.post(f"{UI_PREFIX}/accounts/{{name}}/check")
    async def ui_account_check(request: Request, name: str):
        pre = await _post_prelude(request)
        if isinstance(pre, Response):
            return pre
        form, session, ip = pre
        return await run_in_threadpool(_run, lambda: _do_account_check(form, session, ip, name), session)

    # ------------------------------------------------------------------ 시크릿
    @app.get(f"{UI_PREFIX}/secrets")
    def ui_secrets(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g
        paths = _paths()
        file_env = _file_env()
        inproc = in_process_env(settings)
        specs = allowed_env_keys(settings)
        rows = []
        inputs = ""
        for key, spec in specs.items():
            if spec.group in ("account", "admin"):
                continue
            file_v = file_env.get(key, "")
            file_state = mask_key(key, file_v) if paths else "(env path unknown)"
            # 계정별 LAKE_REPORT_* 는 디스크에 없으면 모드 기본값(LAKE_REPORT_*_{MODE}) 을 물려받고 in_process_env 도 그 유효값을
            # 돌려주므로, 디스크 쪽도 유효값으로 비교해야 '— restart pending' 이 거짓으로 붙지 않는다 (표시는 원래 키의 상태).
            disk_eff = file_v
            if spec.account and not disk_eff:
                disk_eff = file_env.get(key[: -len("_" + util.env_suffix(spec.account))], "")
            if paths and disk_eff != inproc.get(key, ""):
                file_state += " — restart pending"
            rows.append([key, spec.group, spec.account or "-", file_state, mask_key(key, inproc.get(key, ""))])
            typ = "password" if spec.secret else "text"
            inputs += f'<label>{_esc(key)} <input type="{typ}" name="{_esc(key)}" autocomplete="off"></label>'
        table = _table(["key", "group", "account", "on disk (.env)", "in-process"], rows)
        body = ('<p class="muted">Values are never shown (prefix + length only). Empty field = keep. '
                "Saved values take effect only after restart. Exchange API keys are edited on the Accounts page.</p>" + table)
        if paths:
            body += "<h2>Update lake / report / telegram values</h2>" + _form_post(
                f"{UI_PREFIX}/secrets", session.csrf, inputs + '<p class="muted">secrets must be ≥ 32 bytes; report URLs must be absolute.</p>',
                "Save")
            admin_state = mask(file_env.get("ADMIN_TOKEN", "")) if paths else "(env path unknown)"
            body += "<h2>Rotate ADMIN_TOKEN</h2>" + _kv([("on disk", admin_state), ("in-process", mask(inproc.get("ADMIN_TOKEN", "")))])
            body += _form_post(f"{UI_PREFIX}/secrets", session.csrf,
                               '<label>new ADMIN_TOKEN (≥ 32 bytes) <input type="password" name="ADMIN_TOKEN" autocomplete="off"></label>'
                               '<p class="warn">after restart every dashboard session (including this one) becomes invalid.</p>',
                               "Rotate", confirm_word="ROTATE", danger=True)
        else:
            body += '<p class="warn">editing disabled (serve was not started with known --env/--config)</p>'
        return _html(200, _page("Secrets", body, active=f"{UI_PREFIX}/secrets", session=session, flash=_flash_of(request, session)))

    def _do_secrets(form: dict[str, str], session: Session, ip: str) -> Response:
        specs = allowed_env_keys(settings)
        updates: dict[str, str] = {}
        for k, v in form.items():
            if k in (CSRF_FIELD, "confirm"):
                continue
            if k not in specs:
                raise UiError(400, "BAD_KEY", f"{k} is not an editable key", as_json=True)
            if v == "":
                continue
            updates[k] = v
        if "ADMIN_TOKEN" in updates and form.get("confirm", "") != "ROTATE":
            raise UiError(400, "CONFIRM_REQUIRED", "type ROTATE to rotate ADMIN_TOKEN")
        return _apply_env_updates(updates, ip, session, f"{UI_PREFIX}/secrets")

    @app.post(f"{UI_PREFIX}/secrets")
    async def ui_secrets_post(request: Request):
        pre = await _post_prelude(request)
        if isinstance(pre, Response):
            return pre
        form, session, ip = pre
        return await run_in_threadpool(_run, lambda: _do_secrets(form, session, ip), session)

    # ------------------------------------------------------------------ 제어
    @app.get(f"{UI_PREFIX}/controls")
    def ui_controls(request: Request):
        g = _gate(request)
        if isinstance(g, Response):
            return g
        session = g
        executor = _executor()
        paths = _paths()
        file_live: Any = "(env path unknown)"
        if paths:
            try:
                file_live = str(bool((read_json(paths.config).get("live") or {}).get("enabled", False))).lower()
            except Exception as e:  # noqa: BLE001
                file_live = f"unreadable ({type(e).__name__})"
        ready = []
        for m in MODES:
            for n in account_names:
                ex = (getattr(executor, "exchanges", None) or {}).get(m, {}).get(n) if executor is not None else None
                ready.append(f"{m}/{n}={'ready' if ex is not None else '-'}")
        guard: RestartGuard = app.state.ui_restart_guard
        now = now_ms()
        g_ok, g_why = guard.allowed(now)
        pend = app.state.ui_pending
        status = _kv([
            ("halted", _status(str(rcv._halted(settings)).lower())),
            ("live.enabled in-process / on disk", f"{str(bool(settings.live_enabled)).lower()} / {file_live}"),
            ("LAKE_SIGNAL_SECRET_LIVE (in-process)", mask((settings.secrets.signal_secret or {}).get("live", ""))),
            ("live_execution_possible", " ".join(str(x) for x in settings.live_execution_possible())),
            ("exchange_ready", " ".join(ready)),
            ("inconsistent_note", "; ".join(f"{m}: {n}" for m, n in rcv._inconsistent_notes(store, settings).items() if n) or "-"),
            ("pending restart", (",".join(pend["keys"]) + " — restart required") if pend else "no"),
            ("restart guard", f"{'allowed' if g_ok else g_why} (restarts in last 5 min: {len(guard.history(now))}, "
                              f"uptime {_fmt_uptime(now - guard.started_ms)})"),
        ])
        acct_opts = [(n, n) for n in account_names]
        forms = _form_post(f"{UI_PREFIX}/controls/halt", session.csrf,
                           '<p>Reject every new signal (OPERATOR_HALT). Existing SL/TP stay.</p>', "HALT", danger=True)
        forms += _form_post(f"{UI_PREFIX}/controls/resume", session.csrf, "<p>Clear HALT.</p>", "Resume")
        forms += _form_post(f"{UI_PREFIX}/controls/reconcile", session.csrf,
                            "<label>mode " + _select("mode", [(m, m) for m in MODES]) + "</label>"
                            "<label>account " + _select("account", acct_opts, blank="(all with an exchange)") + "</label>"
                            '<p class="muted">holds the executor lock; may take seconds. One at a time, 5 s apart.</p>',
                            "Reconcile now")
        forms += _form_post(f"{UI_PREFIX}/controls/live", session.csrf,
                            '<input type="hidden" name="enabled" value="1"><p>Write live.enabled=true to config.json (restart required).</p>',
                            "Enable LIVE", confirm_word="LIVE", danger=True)
        forms += _form_post(f"{UI_PREFIX}/controls/live", session.csrf,
                            '<input type="hidden" name="enabled" value="0"><p>Write live.enabled=false and apply immediately.</p>',
                            "Disable LIVE")
        forms += _form_post(f"{UI_PREFIX}/controls/restart", session.csrf,
                            "<p>SIGTERM → clean exit → systemd restarts in ~5 s. Webhooks get 502 for ~5–10 s (lake retries). "
                            "Run <b>Check</b> on Accounts first; a .env that fails at boot keeps the service down (exit 2).</p>",
                            "Apply & restart", danger=True)
        last = app.state.ui_last_reconcile
        if last:
            body_r = last.get("body") or {}
            accts = body_r.get("accounts") or {}
            rtable = _table(["account", "consistent", "inconsistent", "note"],
                            [[n, _status(str(bool(r.get("consistent"))).lower()), str(bool(r.get("inconsistent"))).lower(),
                              r.get("inconsistent_note") or ""] for n, r in accts.items()],
                            empty=f"(http {last.get('status')}: {body_r.get('error', '')} {body_r.get('code', '')})")
            recon = (f"<p>at {_esc(_fmt_ms(last.get('at_ms')))} mode={_esc(last.get('mode'))} account={_esc(last.get('account') or '*')} "
                     f"http={_esc(last.get('status'))}</p>" + rtable
                     + "<h3>positions (ledger) vs exchange_positions</h3>"
                     + _json_pre({"positions": body_r.get("positions"), "exchange_positions": body_r.get("exchange_positions")}))
        else:
            recon = '<p class="muted">no reconcile run from the dashboard yet</p>'
        audit = _table(["at", "ip", "action", "detail"],
                       [[_fmt_ms(a), ip, act, det] for a, ip, act, det in reversed(list(app.state.ui_audit))],
                       empty="(no dashboard actions since start)")
        body = ("<h2>Status</h2>" + status + "<h2>Actions</h2>" + forms
                + '<p class="muted">Values saved from the dashboard live in the server .env/config.json. '
                "deploy/finalize.py overwrites them unless run with --keep-remote-env / --keep-remote-config.</p>"
                + "<h2>Last reconcile</h2>" + recon + "<h2>Recent dashboard actions</h2>" + audit)
        return _html(200, _page("Controls", body, active=f"{UI_PREFIX}/controls", session=session, flash=_flash_of(request, session)))

    def _do_halt_ui(form: dict[str, str], session: Session, ip: str) -> Response:
        app.state.admin_ops.halt("dashboard")
        _audit(ip, "HALT set", alert=False)   # admin_ops 가 "[admin] HALT set via dashboard" 알림을 보낸다
        return _redirect(f"{UI_PREFIX}/controls", flash="HALT set", session=session)

    def _do_resume_ui(form: dict[str, str], session: Session, ip: str) -> Response:
        app.state.admin_ops.resume("dashboard")
        _audit(ip, "HALT cleared", alert=False)
        return _redirect(f"{UI_PREFIX}/controls", flash="HALT cleared", session=session)

    def _do_reconcile_ui(form: dict[str, str], session: Session, ip: str) -> Response:
        mode = form.get("mode", "")
        account = form.get("account") or None
        if mode not in MODES:
            raise UiError(400, "BAD_MODE", "mode must be test or live")
        if account is not None and account not in account_names:
            raise UiError(400, "BAD_ACCOUNT", "unknown account")
        if not reconcile_lock.acquire(blocking=False):
            raise UiError(409, "RECONCILE_BUSY", "another reconcile is still running")
        try:
            now = now_ms()
            if now - reconcile_last[0] < RECONCILE_MIN_INTERVAL_MS:
                raise UiError(429, "TOO_SOON", "wait 5 s between reconcile runs")
            reconcile_last[0] = now
            status, body = app.state.admin_ops.reconcile(mode, account)
        finally:
            reconcile_lock.release()
        app.state.ui_last_reconcile = {"at_ms": now, "mode": mode, "account": account, "status": status, "body": body}
        _audit(ip, f"reconcile {mode}/{account or '*'}", f"http={status} consistent={body.get('consistent')}")
        if status != 200:
            raise UiError(status, str(body.get("code") or body.get("error") or "RECONCILE_FAILED"), "see Controls → Last reconcile")
        return _redirect(f"{UI_PREFIX}/controls", session=session,
                         flash=f"reconcile {mode}/{account or '*'}: consistent={body.get('consistent')}")

    def _do_live_ui(form: dict[str, str], session: Session, ip: str) -> Response:
        paths = _paths()
        if paths is None:
            raise UiError(409, "ENV_PATH_UNKNOWN", "serve was not started with known --env/--config", as_json=True)
        enabled = form.get("enabled", "")
        if enabled not in ("0", "1"):
            raise UiError(400, "BAD_REQUEST", "enabled must be 0 or 1")
        want = enabled == "1"
        if want and form.get("confirm", "") != "LIVE":
            raise UiError(400, "CONFIRM_REQUIRED", "type LIVE to enable live execution")
        with write_lock:
            try:
                util.set_json_value(paths.config, "live.enabled", want, validate=lambda tmp: config_mod.load(tmp, paths.env))
            except config_mod.ConfigError as e:
                raise UiError(500, "CONFIG_WRITE_FAILED", str(e)[:300]) from None
            except Exception as e:  # noqa: BLE001
                log.error("ui: config write failed: %s", type(e).__name__)
                raise UiError(500, "CONFIG_WRITE_FAILED", type(e).__name__) from None
            if want:
                _mark_pending(["live.enabled"])
            else:
                settings.live_enabled = False   # 유일한 프로세스 내 변경: 더 보수적인 방향만 즉시 적용
                # 앞선 Enable 이 남긴 'live.enabled' 보류 표시는 디스크(false) 와 프로세스(False) 가 다시 같으므로 지운다
                pend = app.state.ui_pending
                if pend and "live.enabled" in pend["keys"]:
                    keys = [k for k in pend["keys"] if k != "live.enabled"]
                    app.state.ui_pending = {"keys": keys, "at_ms": pend["at_ms"]} if keys else None
        if want:
            _audit(ip, "live.enabled=true written", "restart required")
            flash = "live.enabled=true written to config.json — restart required"
        else:
            _audit(ip, "live.enabled=false", "applied immediately")
            flash = "live.enabled=false written and applied immediately"
        return _redirect(f"{UI_PREFIX}/controls", flash=flash, session=session)

    def _do_restart_ui(form: dict[str, str], session: Session, ip: str) -> Response:
        guard: RestartGuard = app.state.ui_restart_guard
        ok, why = guard.request("dashboard")
        if not ok:
            raise UiError(429, why, {"PROCESS_TOO_YOUNG": "process started less than 60 s ago",
                                     "TOO_SOON": "last restart less than 60 s ago",
                                     "RATE_LIMITED": "3 restarts in the last 5 min"}.get(why, why))
        _audit(ip, "restart", alert=False)   # guard 가 알림을 보낸다
        return _redirect(f"{UI_PREFIX}/controls", flash="restarting in ~5 s — this page will be unavailable briefly", session=session)

    for _path, _impl in (("halt", _do_halt_ui), ("resume", _do_resume_ui), ("reconcile", _do_reconcile_ui),
                         ("live", _do_live_ui), ("restart", _do_restart_ui)):
        def _make(impl: Callable[[dict[str, str], Session, str], Response]):
            async def handler(request: Request):
                pre = await _post_prelude(request)
                if isinstance(pre, Response):
                    return pre
                form, session, ip = pre
                return await run_in_threadpool(_run, lambda: impl(form, session, ip), session)
            return handler
        app.add_api_route(f"{UI_PREFIX}/controls/{_path}", _make(_impl), methods=["POST"], name=f"ui_controls_{_path}")

    # ------------------------------------------------------------------ 그 밖의 /ui 경로·메서드 (반드시 마지막에 등록)
    ALL_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]

    def ui_fallback(request: Request, rest: str = "") -> Response:
        """등록되지 않은 /ui 경로나 허용되지 않은 메서드. Starlette 기본 404/405(라우트 표 노출, 보안 헤더 없음) 대신
        같은 게이트(토큰 미설정 404 JSON → 로그인 303/401) 를 거친 뒤 405(경로는 있음) 또는 404 를 보안 헤더와 함께 낸다."""
        g = _gate(request)
        if isinstance(g, Response):
            return g
        for route in app.router.routes:
            if getattr(route, "endpoint", None) is ui_fallback:
                continue
            try:
                m, _child = route.matches(request.scope)
            except Exception:  # noqa: BLE001
                continue
            if m == Match.PARTIAL:
                return _error(405, "METHOD_NOT_ALLOWED", f"{request.method} is not allowed on this path", g)
        return _error(404, "NOT_FOUND", "no such page", g)

    app.add_api_route(f"{UI_PREFIX}/{{rest:path}}", ui_fallback, methods=ALL_METHODS, name="ui_fallback",
                      include_in_schema=False)
    app.add_api_route(UI_PREFIX, ui_fallback, methods=[m for m in ALL_METHODS if m != "GET"], name="ui_fallback_root",
                      include_in_schema=False)

    log.info("dashboard mounted at %s (%s)", UI_PREFIX, "enabled" if _token() else "disabled: ADMIN_TOKEN unset")
