"""운영 유틸 (ARCHITECTURE.md §6): 로깅, 알림(텔레그램), HALT 파일.

규칙
  - 로그에 시크릿·원본 본문·거래소 오류 원문 전체를 남기지 않는다. 설정된 시크릿 값이
    메시지에 섞여 들어와도 포맷터가 '***' 로 가린다(안전망). 원문 절단(200자)은 각 모듈 책임.
  - Alerts.send 는 어떤 경우에도 예외를 밖으로 내지 않는다. 같은 문구는 60초에 1회만.
  - import 시 부작용 없음 (HTTP 클라이언트는 첫 전송 때 만든다).
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time
from typing import Any

from .util import now_ms

log = logging.getLogger("lake_executor.ops")

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
ALERT_DEDUPE_MS = 60_000
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_TIMEOUT_S = 5.0
TELEGRAM_MAX_TEXT = 4000

# setup_logging 이 단 핸들러를 표시하는 속성 (재호출 시 중복 방지)
_HANDLER_TAG = "_lake_executor_handler"


# --------------------------------------------------------------------------- #
# 로깅
# --------------------------------------------------------------------------- #
class RedactingFormatter(logging.Formatter):
    """포맷된 로그 문자열에서 알려진 시크릿 값을 '***' 로 치환한다."""

    def __init__(self, fmt: str, secrets: list[str] | None = None):
        super().__init__(fmt)
        # 길이 긴 것부터 치환 (부분 문자열 중복 방지), 8자 미만은 오탐 위험이 커서 제외
        self._secrets = sorted({s for s in (secrets or []) if s and len(s) >= 8}, key=len, reverse=True)

    def format(self, record: logging.LogRecord) -> str:
        try:
            text = super().format(record)
        except Exception:  # 포맷 인자 불일치 등 — 로깅이 서비스를 죽이면 안 된다
            text = f"{record.levelname} [{record.name}] <unformattable log record>"
        for s in self._secrets:
            if s in text:
                text = text.replace(s, "***")
        return text


def _collect_secrets(settings: Any) -> list[str]:
    sec = getattr(settings, "secrets", None)
    if sec is None:
        return []
    out: list[str] = []
    for name in ("bybit_api_key", "bybit_api_secret", "admin_token", "telegram_bot_token"):
        v = getattr(sec, name, "")
        if v:
            out.append(str(v))
    for name in ("signal_secret", "report_secret"):
        d = getattr(sec, name, None) or {}
        out.extend(str(v) for v in d.values() if v)
    # 2단계: 계정별 키/패스프레이즈/회신 시크릿 (OKX_*, TOOBIT_*, LAKE_REPORT_SECRET_{MODE}_{NAME})
    for acct in getattr(settings, "accounts", None) or []:
        for name in ("api_key", "api_secret", "api_passphrase"):
            v = getattr(acct, name, "")
            if v:
                out.append(str(v))
        d = getattr(acct, "report_secret", None) or {}
        out.extend(str(v) for v in d.values() if v)
    return out


def setup_logging(settings: Any, level: int = logging.INFO) -> logging.Logger:
    """stdout + (설정 시) 파일 핸들러를 루트 로거에 단다. 여러 번 불러도 핸들러가 중복되지 않는다."""
    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        if getattr(h, _HANDLER_TAG, False):
            root.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass

    formatter = RedactingFormatter(LOG_FORMAT, _collect_secrets(settings))

    try:  # Windows 콘솔 등 비-UTF-8 stdout 대비
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    setattr(sh, _HANDLER_TAG, True)
    root.addHandler(sh)

    log_file = getattr(settings, "log_file", "") or ""
    if log_file:
        try:
            d = os.path.dirname(log_file)
            if d:
                os.makedirs(d, exist_ok=True)
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(formatter)
            setattr(fh, _HANDLER_TAG, True)
            root.addHandler(fh)
        except Exception as e:  # 파일을 못 열어도 stdout 로깅은 계속
            root.warning("log file unavailable (%s): %s", log_file, type(e).__name__)

    # HTTP 라이브러리의 요청 단위 INFO 로그(URL 포함) 는 소음·노출 위험 → WARNING 이상만
    for name in ("httpx", "httpcore", "urllib3", "pybit"):
        logging.getLogger(name).setLevel(logging.WARNING)
    return root


# --------------------------------------------------------------------------- #
# 알림
# --------------------------------------------------------------------------- #
class Alerts:
    """운영자 알림: 로그 ERROR + (설정 시) 텔레그램. 예외는 전부 삼킨다.

    client 를 주입하면(테스트) 그 객체의 .post(url, json=..., timeout=...) 를 쓴다.
    """

    def __init__(self, settings: Any, client: Any = None):
        sec = getattr(settings, "secrets", None)
        self._token: str = str(getattr(sec, "telegram_bot_token", "") or "")
        self._chat_id: str = str(getattr(sec, "telegram_chat_id", "") or "")
        self._client = client
        self._lock = threading.Lock()
        self._recent: dict[str, int] = {}   # text -> 마지막 전송 ms (60초 중복 억제)
        self.sent_count = 0                  # 실제로 처리(로그/전송)된 알림 수 (중복 억제 제외)

    @property
    def telegram_enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    def send(self, text: str) -> bool:
        """알림 1건. 60초 안의 동일 문구는 무시(False). 전송 실패해도 예외 없음."""
        try:
            text = str(text)
            if not text:
                return False
            now = now_ms()
            with self._lock:
                self._prune(now)
                last = self._recent.get(text)
                if last is not None and now - last < ALERT_DEDUPE_MS:
                    return False
                self._recent[text] = now
                self.sent_count += 1
            log.error("ALERT: %s", text)
            if self.telegram_enabled:
                self._telegram(text)
            return True
        except Exception:
            # 알림 경로의 오류가 실행기를 멈추면 안 된다
            return False

    def _prune(self, now: int) -> None:
        if len(self._recent) > 256:
            self._recent = {k: v for k, v in self._recent.items() if now - v < ALERT_DEDUPE_MS}

    def _telegram(self, text: str) -> None:
        url = TELEGRAM_API.format(token=self._token)
        payload = {"chat_id": self._chat_id, "text": text[:TELEGRAM_MAX_TEXT], "disable_web_page_preview": True}
        try:
            client = self._client
            if client is None:
                import httpx  # 지연 import: 알림 미사용 환경에서 의존 최소화
                client = httpx.Client()
                self._client = client
            r = client.post(url, json=payload, timeout=TELEGRAM_TIMEOUT_S)
            status = getattr(r, "status_code", None)
            if status is not None and not (200 <= int(status) < 300):
                log.warning("telegram sendMessage failed: http %s", status)
        except Exception as e:
            # httpx 예외 문자열에는 토큰이 든 URL 이 포함될 수 있으므로 타입명만 남긴다
            log.warning("telegram sendMessage error: %s", type(e).__name__)

    def close(self) -> None:
        c, self._client = self._client, None
        if c is not None and hasattr(c, "close"):
            try:
                c.close()
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# HALT 파일
# --------------------------------------------------------------------------- #
def halted(settings: Any) -> bool:
    return os.path.exists(settings.halt_file)


def halt(settings: Any, note: str = "") -> None:
    """HALT 파일 생성 → 실행기는 새 신호를 rejected/OPERATOR_HALT 로 처리 (기존 보호주문 유지)."""
    path = settings.halt_file
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"halted_at_ms={now_ms()} {time.strftime('%Y-%m-%dT%H:%M:%S%z')}\n")
        if note:
            f.write(note[:500] + "\n")
    log.warning("HALT set: %s", path)


def resume(settings: Any) -> None:
    try:
        os.remove(settings.halt_file)
        log.warning("HALT cleared: %s", settings.halt_file)
    except FileNotFoundError:
        pass
