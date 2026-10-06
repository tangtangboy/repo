"""util.py — .env 쓰기(render_env_update / quote_env_value / write_env_file), 원자적 교체, set_json_value.

대시보드(web.py) 가 쓰는 경로. 불변식: parse_env_file(쓴 결과) == {**parse_env_file(이전), **updates}.
"""
from __future__ import annotations

import json
import os
import stat

import pytest

from lake_executor import util
from lake_executor.util import (atomic_write_text, parse_env_file, quote_env_value, read_json, render_env_update,
                                set_json_value, write_env_file)

POSIX_ONLY = pytest.mark.skipif(os.name == "nt", reason="file mode bits are POSIX only")

OLD = (
    "# lake-executor secrets\n"
    "\n"
    "BYBIT_API_KEY=old-key\n"
    "BYBIT_API_SECRET=old-secret   \n"
    "LAKE_SIGNAL_SECRET_TEST='quoted value'\n"
    "# LAKE_REPORT_URL_TEST_OKX=\n"
    "# LAKE_REPORT_SECRET_TEST_OKX=\n"
    "ADMIN_TOKEN=first\n"
    "TELEGRAM_CHAT_ID=123\n"
    "ADMIN_TOKEN=second\n"
)


# ---------------------------------------------------------------- render_env_update
def test_round_trip_preserves_comments_order_and_merges(tmp_path):
    updates = {
        "BYBIT_API_KEY": "new-key-0123",                 # 제자리 교체
        "ADMIN_TOKEN": "rotated-token-0123456789abcdef-0123456789",   # 중복 정의 → 하나로
        "LAKE_REPORT_URL_TEST_OKX": "https://lake.test/okx",          # 주석 템플릿 활성화
        "OKX_API_KEY": "brand-new",                      # 끝에 추가
    }
    out = render_env_update(OLD, updates)
    assert parse_env_file_text(out, tmp_path) == {**parse_env_file_text(OLD, tmp_path), **updates}
    lines = out.split("\n")
    assert lines[0] == "# lake-executor secrets"
    assert lines[1] == ""
    assert lines[2] == "BYBIT_API_KEY=new-key-0123"
    assert lines[3] == "BYBIT_API_SECRET=old-secret   "          # 건드리지 않은 줄은 그대로
    assert "LAKE_REPORT_URL_TEST_OKX=https://lake.test/okx" in lines
    assert lines.index("LAKE_REPORT_URL_TEST_OKX=https://lake.test/okx") == 5   # 주석 템플릿 자리
    assert "# LAKE_REPORT_SECRET_TEST_OKX=" in lines                            # 다른 템플릿은 그대로
    assert out.count("ADMIN_TOKEN=") == 1
    assert lines.index("ADMIN_TOKEN=rotated-token-0123456789abcdef-0123456789") == 7   # 첫 정의 자리
    assert lines[-2] == "OKX_API_KEY=brand-new"
    assert out.endswith("\n") and "\r" not in out


def parse_env_file_text(text: str, tmp_path) -> dict[str, str]:
    p = tmp_path / f"env-{abs(hash(text))}.txt"
    p.write_text(text, encoding="utf-8")
    return parse_env_file(str(p))


def test_render_handles_empty_file_and_missing_trailing_newline():
    assert render_env_update("", {"A": "1"}) == "A=1\n"
    assert render_env_update("B=2", {"A": "1"}) == "B=2\nA=1\n"
    assert render_env_update("B=2\r\nC=3\r\n", {"C": "x"}) == "B=2\nC=x\n"


def test_render_rejects_bad_keys_and_values():
    with pytest.raises(ValueError):
        render_env_update("", {"lower": "x"})
    with pytest.raises(ValueError):
        render_env_update("", {"A B": "x"})
    with pytest.raises(ValueError):
        render_env_update("", {"A": "line\nbreak"})
    with pytest.raises(ValueError):
        render_env_update("", {"A": "nul\0"})
    assert util.ENV_KEY_RE.match("LAKE_REPORT_URL_LIVE_OKX_SUB")


# ---------------------------------------------------------------- quoting
@pytest.mark.parametrize("value,expected", [
    ("plain-value_1", "plain-value_1"),
    ("a b", '"a b"'),
    (" lead", '" lead"'),
    ("has#hash", '"has#hash"'),
    ('say "hi"', "'say \"hi\"'"),
    ("'x'", "\"'x'\""),
    ("", ""),
    ("it's", "it's"),
])
def test_quote_env_value(value, expected, tmp_path):
    q = quote_env_value(value)
    assert q == expected
    assert parse_env_file_text(f"K={q}\n", tmp_path).get("K", "") == value


def test_quote_env_value_unrepresentable():
    with pytest.raises(ValueError):
        quote_env_value("""both 'quotes' and "quotes" with space""")


# ---------------------------------------------------------------- atomic write / write_env_file
def test_write_env_file_backs_up_and_returns_changed_keys(tmp_path):
    p = tmp_path / ".env"
    p.write_text(OLD, encoding="utf-8")
    changed = write_env_file(str(p), {"BYBIT_API_KEY": "new-key", "TELEGRAM_CHAT_ID": "123"})   # 둘째는 같은 값
    assert changed == ["BYBIT_API_KEY"]
    assert parse_env_file(str(p))["BYBIT_API_KEY"] == "new-key"
    assert (tmp_path / ".env.bak").read_text(encoding="utf-8") == OLD
    assert not [n for n in os.listdir(tmp_path) if ".tmp-" in n]
    # 아무것도 안 바뀌면 파일을 건드리지 않는다
    before = p.read_bytes()
    assert write_env_file(str(p), {"BYBIT_API_KEY": "new-key"}) == []
    assert p.read_bytes() == before


def test_write_env_file_creates_missing_file(tmp_path):
    p = tmp_path / "new.env"
    assert write_env_file(str(p), {"A": "1"}) == ["A"]
    assert p.read_text(encoding="utf-8") == "A=1\n"
    assert not (tmp_path / "new.env.bak").exists()


def test_atomic_write_cleans_tmp_on_failure(tmp_path, monkeypatch):
    p = tmp_path / "f.txt"
    p.write_text("before", encoding="utf-8")
    real_replace = os.replace

    def boom(src, dst):
        if str(dst).endswith(".bak"):
            return real_replace(src, dst)     # 백업은 성공, 본 파일 교체만 실패
        raise OSError("injected")
    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        atomic_write_text(str(p), "after")
    monkeypatch.setattr(os, "replace", real_replace)
    assert p.read_text(encoding="utf-8") == "before"
    assert not [n for n in os.listdir(tmp_path) if ".tmp-" in n]
    assert (tmp_path / "f.txt.bak").read_text(encoding="utf-8") == "before"   # 백업은 교체 전에 만든다


@POSIX_ONLY   # Windows 는 다른 스레드가 읽는 중인 파일로의 os.replace 를 거부한다 (공유 모드) — 배포 환경(Linux) 에서만 의미 있는 검사
def test_atomic_write_concurrent_writers_do_not_collide(tmp_path):
    """같은 pid 의 여러 스레드가 같은 파일을 동시에 써도(임시 파일 이름이 고유) 예외·잔재·손상이 없다."""
    import threading
    p = tmp_path / "shared.env"
    p.write_text("A=0\n", encoding="utf-8")
    errors: list[BaseException] = []
    texts = [f"A={i}\n" for i in range(8)]

    def worker(i: int) -> None:
        try:
            for _ in range(10):
                atomic_write_text(str(p), texts[i])
        except BaseException as e:  # noqa: BLE001
            errors.append(e)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert errors == []
    assert p.read_text(encoding="utf-8") in texts
    assert not [n for n in os.listdir(tmp_path) if ".tmp-" in n]


def test_tmp_file_name_is_unique_per_call(tmp_path, monkeypatch):
    seen: list[str] = []
    real_replace = os.replace

    def spy(src, dst):
        seen.append(os.path.basename(src))
        return real_replace(src, dst)
    monkeypatch.setattr(os, "replace", spy)
    p = tmp_path / "x.env"
    atomic_write_text(str(p), "A=1\n", backup=False)
    atomic_write_text(str(p), "A=2\n", backup=False)
    assert len(seen) == 2 and seen[0] != seen[1] and all(n.startswith("x.env.tmp-") for n in seen)


@POSIX_ONLY
def test_atomic_write_mode_0600(tmp_path):
    p = tmp_path / "s.env"
    atomic_write_text(str(p), "A=1\n")
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600
    atomic_write_text(str(p), "A=2\n")
    assert stat.S_IMODE(os.stat(str(p) + ".bak").st_mode) == 0o600


# ---------------------------------------------------------------- set_json_value
def test_set_json_value_nested_booleans_and_validation(tmp_path):
    p = tmp_path / "config.json"
    p.write_text(json.dumps({"live": {"enabled": False}, "x": 1}), encoding="utf-8")
    set_json_value(str(p), "live.enabled", True)
    assert read_json(str(p)) == {"live": {"enabled": True}, "x": 1}
    assert '"enabled": true' in p.read_text(encoding="utf-8")       # JSON 불리언 (문자열 아님)
    set_json_value(str(p), "new.nested.key", "v")
    assert read_json(str(p))["new"] == {"nested": {"key": "v"}}

    before = p.read_bytes()

    def bad(tmp_path_str):
        assert os.path.exists(tmp_path_str)
        raise ValueError("rejected")
    with pytest.raises(ValueError):
        set_json_value(str(p), "live.enabled", False, validate=bad)
    assert p.read_bytes() == before
    assert not [n for n in os.listdir(tmp_path) if ".tmp-" in n]
