from quixstreams.sources.community.crypto.utils import expand_dataloader_prefixes


def test_expand_dataloader_prefixes_daily_and_monthly():
    # daily
    prefs = expand_dataloader_prefixes(
        "p/{market}/{segment}/{datatype}/{symbol}/{date}/",
        market="spot",
        segments=["daily"],
        datatypes=["trades"],
        symbols=["BTCUSDT"],
        date_from="2025-01-01",
        date_to="2025-01-02",
    )
    assert prefs == [
        "p/spot/daily/trades/BTCUSDT/2025-01-01/",
        "p/spot/daily/trades/BTCUSDT/2025-01-02/",
    ]
    # monthly
    prefs2 = expand_dataloader_prefixes(
        "p/{market}/{segment}/{datatype}/{interval}/{symbol}/",
        market="um_futures",
        segments=["monthly"],
        datatypes=["klines"],
        symbols=["BTCUSDT"],
        interval="1m",
    )
    assert prefs2 == ["p/um_futures/monthly/klines/1m/BTCUSDT/" ]