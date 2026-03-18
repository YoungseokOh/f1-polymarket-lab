from f1_polymarket_lab.connectors import parse_market_taxonomy


def test_head_to_head_practice_parser() -> None:
    parsed = parse_market_taxonomy("Norris vs Piastri in FP2?")

    assert parsed.taxonomy == "head_to_head_practice"
    assert parsed.target_session_code == "FP2"


def test_red_flag_parser() -> None:
    parsed = parse_market_taxonomy("Red flag during FP3 at the British Grand Prix?")

    assert parsed.taxonomy == "red_flag"
    assert parsed.target_session_code == "FP3"
