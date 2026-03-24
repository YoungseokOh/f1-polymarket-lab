from __future__ import annotations

from datetime import date

from f1_polymarket_lab.connectors import infer_market_scheduled_date, parse_market_taxonomy


def test_parse_market_taxonomy_expands_session_and_season_markets() -> None:
    qualifying = parse_market_taxonomy("Will Lando Norris win Qualifying at the 2025 Monaco GP?")
    assert qualifying.taxonomy == "qualifying_winner"
    assert qualifying.target_session_code == "Q"

    race = parse_market_taxonomy("Will Driver A win the 2025 F1 Qatar Grand Prix?")
    assert race.taxonomy == "race_winner"
    assert race.target_session_code == "R"
    assert race.confidence == 0.3
    assert race.metadata["contains_placeholder"] == "true"

    drivers = parse_market_taxonomy("F1 2026 Drivers Champion")
    assert drivers.taxonomy == "drivers_champion"
    assert drivers.target_session_code is None

    constructors = parse_market_taxonomy("Will Ferrari win the 2025 F1 Constructors Championship?")
    assert constructors.taxonomy == "constructors_champion"


def test_parse_market_taxonomy_supports_session_specific_market_families() -> None:
    pole = parse_market_taxonomy(
        "Will Max Verstappen get pole position at the 2026 Japanese Grand Prix?"
    )
    assert pole.taxonomy == "driver_pole_position"
    assert pole.target_session_code == "Q"
    assert pole.driver_a == "Verstappen"

    head_to_head = parse_market_taxonomy(
        "Will Lawson finish ahead of Tsunoda in the 2026 Japanese Grand Prix?"
    )
    assert head_to_head.taxonomy == "head_to_head_session"
    assert head_to_head.target_session_code == "R"
    assert head_to_head.driver_a == "Tsunoda" or head_to_head.driver_b == "Tsunoda"

    race_fastest_lap = parse_market_taxonomy(
        "Will Charles Leclerc get the fastest lap at the 2026 Japanese Grand Prix?"
    )
    assert race_fastest_lap.taxonomy == "driver_fastest_lap_session"
    assert race_fastest_lap.target_session_code == "R"

    podium = parse_market_taxonomy(
        "Will Oscar Piastri finish on the podium at the 2026 Japanese Grand Prix?"
    )
    assert podium.taxonomy == "driver_podium"
    assert podium.target_session_code == "R"

    constructor = parse_market_taxonomy(
        "Will Ferrari be the constructor that scores 1st at the 2026 Japanese Grand Prix?"
    )
    assert constructor.taxonomy == "constructor_scores_first"
    assert constructor.target_session_code == "R"

    sprint_qualifying = parse_market_taxonomy(
        "Will Oscar Piastri win Sprint Qualifying Pole Winner at the 2026 Chinese Grand Prix?"
    )
    assert sprint_qualifying.taxonomy == "qualifying_winner"
    assert sprint_qualifying.target_session_code == "SQ"

    most_points = parse_market_taxonomy(
        "F1 Japan Grand Prix: Which Constructor scores the most points?"
    )
    assert most_points.taxonomy == "constructor_scores_first"
    assert most_points.target_session_code == "R"

    matchup = parse_market_taxonomy(
        "Hamilton vs. Leclerc",
        (
            "This market is based on whether Lewis Hamilton or Charles Leclerc "
            "finishes ahead of the other at the F1 Monaco Grand Prix, "
            "scheduled for May 25, 2025."
        ),
    )
    assert matchup.taxonomy == "head_to_head_session"
    assert matchup.target_session_code == "R"

    fastest_lap_with_red_flag_clause = parse_market_taxonomy(
        "Will Pierre Gasly achieve the fastest lap at the 2026 F1 Japanese Grand Prix?",
        (
            "This market will resolve in favor of the driver who is officially credited "
            "with the fastest lap in the Final Classification. "
            "If no driver completes a lap during the race "
            "(e.g., due to a red flag ending the race prematurely), "
            "this market will resolve to Other."
        ),
    )
    assert fastest_lap_with_red_flag_clause.taxonomy == "driver_fastest_lap_session"
    assert fastest_lap_with_red_flag_clause.target_session_code == "R"

    title_guided_fastest_lap = parse_market_taxonomy(
        "Will Pierre Gasly achieve the fastest lap at the 2026 F1 Japanese Grand Prix?",
        (
            "This market resolves based on the official race classification. "
            "Times from practice sessions, qualifying, or any other sessions are not considered."
        ),
        title="Japanese Grand Prix: Driver Fastest Lap",
    )
    assert title_guided_fastest_lap.taxonomy == "driver_fastest_lap_session"
    assert title_guided_fastest_lap.target_session_code == "R"


def test_parse_market_taxonomy_prefers_race_winner_over_podium_ceremony_text() -> None:
    winner = parse_market_taxonomy(
        "Will Pierre Gasly win the 2026 F1 Chinese Grand Prix?",
        (
            "This market is on the winner of the 2026 F1 Chinese Grand Prix. "
            "The timing of the podium ceremony does not determine the result for this market."
        ),
        title="Chinese Grand Prix: Driver Podium Finish",
    )
    assert winner.taxonomy == "race_winner"
    assert winner.target_session_code == "R"


def test_infer_market_scheduled_date_parses_slug_and_description_dates() -> None:
    assert (
        infer_market_scheduled_date("f1-japanese-grand-prix-practice-2-fastest-lap-2026-03-27")
        == date(2026, 3, 27)
    )
    assert (
        infer_market_scheduled_date(
            "This market is on the driver with the fastest lap in Practice 2, "
            "scheduled for Mar 27, 2026."
        )
        == date(2026, 3, 27)
    )
