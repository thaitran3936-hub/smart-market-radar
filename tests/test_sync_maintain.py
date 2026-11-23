import gzip
import sqlite3

import pandas as pd
import pytest

from src.data import collector, db, sync


def _make_db(path, rows=3):
    con = sqlite3.connect(path)
    db.init_db(con)
    for i in range(rows):
        con.execute("INSERT INTO prices_daily(symbol, trade_date, close, match_val, data_status, fetch_time) VALUES (?,?,?,?,?,'t')",
                    ("AAA", f"2026-11-0{i+1}", 10.0, 2e9, "ok"))
    con.commit()
    return con


def test_active_symbols_picks_liquid(tmp_path):
    con = _make_db(str(tmp_path / "a.db"))
    con.execute("INSERT INTO prices_daily(symbol, trade_date, close, match_val, data_status, fetch_time) VALUES ('ZZZ','2026-11-03',5,1000,'ok','t')")
    con.commit()
    assert collector.active_symbols(con) == {"AAA"}


class FakeResp:
    def __init__(self, status=200, js=None, content=b""):
        self.status_code, self._js, self._c = status, js, content
    def json(self): return self._js
    def iter_content(self, n): yield self._c
    def __enter__(self): return self
    def __exit__(self, *a): return False


class FakeSession:
    def __init__(self, gz_bytes, stamp="t1", status=200):
        self.gz, self.stamp, self.status = gz_bytes, stamp, status
    def get(self, url, **kw):
        if "releases/tags" in url:
            if self.status != 200:
                return FakeResp(self.status)
            return FakeResp(200, {"assets": [{"name": "market.db.gz", "url": "http://x/asset", "updated_at": self.stamp}]})
        return FakeResp(200, content=self.gz)


def _gz_of(path):
    return gzip.compress(open(path, "rb").read())


def test_download_replaces_and_is_idempotent(tmp_path):
    src = tmp_path / "server.db"
    _make_db(str(src), rows=3).close()
    dest = str(tmp_path / "local" / "market.db")
    s = FakeSession(_gz_of(src))
    r = sync.download_latest("o/r", "tok", dest, session=s)
    assert r["status"] == "updated" and r["prices_rows"] == 3
    assert sync.download_latest("o/r", "tok", dest, session=s)["status"] == "up_to_date"
    assert sync.download_latest("o/r", "tok", dest, session=s, force=True)["status"] == "updated"


def test_download_keeps_local_streaming(tmp_path):
    src = tmp_path / "server.db"
    _make_db(str(src)).close()
    dest = tmp_path / "market.db"
    con = _make_db(str(dest), rows=1)
    con.execute("INSERT INTO stream_raw(recv_time,msg_type,symbol,payload) VALUES ('2026-11-23T10:00:00','TradeMessage','HPG','{}')")
    con.commit(); con.close()
    r = sync.download_latest("o/r", "tok", str(dest), session=FakeSession(_gz_of(src)))
    assert r["stream_rows"] == 1
    con = sqlite3.connect(dest)
    assert con.execute("SELECT COUNT(*) FROM stream_raw").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM prices_daily").fetchone()[0] == 3


def test_corrupt_download_keeps_old_db(tmp_path):
    dest = tmp_path / "market.db"
    _make_db(str(dest), rows=2).close()
    with pytest.raises(sync.SyncError):
        sync.download_latest("o/r", "tok", str(dest), session=FakeSession(gzip.compress(b"not a db")))
    assert sqlite3.connect(dest).execute("SELECT COUNT(*) FROM prices_daily").fetchone()[0] == 2


def test_errors_are_friendly(tmp_path):
    with pytest.raises(sync.SyncError):
        sync.download_latest("", "", str(tmp_path / "x.db"))
    for code in (404, 401):
        with pytest.raises(sync.SyncError) as e:
            sync.download_latest("o/r", "SECRET", str(tmp_path / "x.db"), session=FakeSession(b"", status=code))
        assert "SECRET" not in str(e.value)
