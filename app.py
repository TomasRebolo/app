#!/usr/bin/python3
# Copyright (c) BDist Development Team
# Distributed under the terms of the Modified BSD License.
import os
from decimal import Decimal, InvalidOperation
from logging.config import dictConfig

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool 
from datetime import datetime

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

RATELIMIT_STORAGE_URI = os.environ.get("RATELIMIT_STORAGE_URI", "memory://")

app = Flask(__name__)
app.config.from_prefixed_env()
log = app.logger
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri=RATELIMIT_STORAGE_URI,
)

# Use the DATABASE_URL environment variable if it exists, otherwise use the default.
# Use the format postgres://username:password@hostname/database_name to connect to the database.
DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://db:db@postgres/db")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={
        "autocommit": True,  # If True don’t start transactions automatically.
        "row_factory": namedtuple_row,
    },
    min_size=4,
    max_size=10,
    open=True,
    #check=ConnectionPool.check_connection,
    name="postgres_pool",
    timeout=5,
)


def is_decimal(s):
    """Returns True if string is a parseable Decimal number."""
    try:
        Decimal(s)
        return True
    except InvalidOperation:
        return False


@app.route("/", methods=("GET",))
@app.route("/players", methods=("GET",))
@limiter.limit("1 per second")
def player_index():
    """Show all the accounts, most recent first."""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            players = cur.execute(
                """
                SELECT
                  *
                FROM
                  players_index_view
                LIMIT
                  20;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    return render_template("players/index.html", players=players)
                                                                                                                           

@app.route("/players/update", methods=("POST",))
@limiter.limit("1 per second")
def player_update_view():
    """Show the page to update the account balance."""
    player_api_id = request.form["player_api_id"]

    with pool.connection() as conn:
        with conn.cursor() as cur:
            player = cur.execute(
                """
                SELECT pa.*, p.player_name
                FROM player_attributes pa
                JOIN Player p ON pa.player_api_id = p.player_api_id
                WHERE pa.player_api_id = %(player_api_id)s 
                ORDER BY pa.date DESC
                LIMIT 1;
                """,
                {"player_api_id": player_api_id},
            ).fetchone()
            log.debug(f"Found {cur.rowcount} rows.")

    return render_template("players/update.html", player=player)


@app.route("/players/<player_api_id>/update", methods=("POST",))
def player_update_save(player_api_id):
    """Insert new player attributes."""

    # Extract current date-time
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Helper functions to validate input
    def validate_integer(value, field_name):
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"{field_name} must be an integer.")

    def validate_text(value, field_name, valid_options=None):
        if not isinstance(value, str) or value.strip() == "":
            raise ValueError(f"{field_name} must be a non-empty text.")
        if valid_options and value not in valid_options:
            raise ValueError(f"{field_name} must be one of {valid_options}.")
        return value

    def validate_not_null(value, field_name):
        if value is None or (isinstance(value, str) and value.strip() == ""):
            raise ValueError(f"{field_name} is required.")
    
    # Validate and parse form data
    overall_rating = validate_integer(request.form["overall_rating"], "overall_rating")
    potential = validate_integer(request.form["potential"], "potential")
    preferred_foot = request.form["preferred_foot"]
    attacking_work_rate = request.form["attacking_work_rate"]
    defensive_work_rate = request.form["defensive_work_rate"]
    crossing = validate_integer(request.form["crossing"], "crossing")
    finishing = validate_integer(request.form["finishing"], "finishing")
    heading_accuracy = validate_integer(request.form["heading_accuracy"], "heading_accuracy")
    short_passing = validate_integer(request.form["short_passing"], "short_passing")
    volleys = validate_integer(request.form["volleys"], "volleys")
    dribbling = validate_integer(request.form["dribbling"], "dribbling")
    curve = validate_integer(request.form["curve"], "curve")
    free_kick_accuracy = validate_integer(request.form["free_kick_accuracy"], "free_kick_accuracy")
    long_passing = validate_integer(request.form["long_passing"], "long_passing")
    ball_control = validate_integer(request.form["ball_control"], "ball_control")
    acceleration = validate_integer(request.form["acceleration"], "acceleration")
    sprint_speed = validate_integer(request.form["sprint_speed"], "sprint_speed")
    agility = validate_integer(request.form["agility"], "agility")
    reactions = validate_integer(request.form["reactions"], "reactions")
    balance = validate_integer(request.form["balance"], "balance")
    shot_power = validate_integer(request.form["shot_power"], "shot_power")
    jumping = validate_integer(request.form["jumping"], "jumping")
    stamina = validate_integer(request.form["stamina"], "stamina")
    strength = validate_integer(request.form["strength"], "strength")
    long_shots = validate_integer(request.form["long_shots"], "long_shots")
    aggression = validate_integer(request.form["aggression"], "aggression")
    interceptions = validate_integer(request.form["interceptions"], "interceptions")
    positioning = validate_integer(request.form["positioning"], "positioning")
    vision = validate_integer(request.form["vision"], "vision")
    penalties = validate_integer(request.form["penalties"], "penalties")
    marking = validate_integer(request.form["marking"], "marking")
    standing_tackle = validate_integer(request.form["standing_tackle"], "standing_tackle")
    sliding_tackle = validate_integer(request.form["sliding_tackle"], "sliding_tackle")
    gk_diving = validate_integer(request.form["gk_diving"], "gk_diving")
    gk_handling = validate_integer(request.form["gk_handling"], "gk_handling")
    gk_kicking = validate_integer(request.form["gk_kicking"], "gk_kicking")
    gk_positioning = validate_integer(request.form["gk_positioning"], "gk_positioning")
    gk_reflexes = validate_integer(request.form["gk_reflexes"], "gk_reflexes")

    validate_not_null(current_datetime, "date")
    validate_not_null(overall_rating, "overall_rating")
    validate_not_null(potential, "potential")
    validate_not_null(preferred_foot, "preferred_foot")
    validate_not_null(attacking_work_rate, "attacking_work_rate")
    validate_not_null(defensive_work_rate, "defensive_work_rate")
    validate_not_null(crossing, "crossing")
    validate_not_null(finishing, "finishing")
    validate_not_null(heading_accuracy, "heading_accuracy")
    validate_not_null(short_passing, "short_passing")
    validate_not_null(volleys, "volleys")
    validate_not_null(dribbling, "dribbling")
    validate_not_null(curve, "curve")
    validate_not_null(free_kick_accuracy, "free_kick_accuracy")
    validate_not_null(long_passing, "long_passing")
    validate_not_null(ball_control, "ball_control")
    validate_not_null(acceleration, "acceleration")
    validate_not_null(sprint_speed, "sprint_speed")
    validate_not_null(agility, "agility")
    validate_not_null(reactions, "reactions")
    validate_not_null(balance, "balance")
    validate_not_null(shot_power, "shot_power")
    validate_not_null(jumping, "jumping")
    validate_not_null(stamina, "stamina")
    validate_not_null(strength, "strength")
    validate_not_null(long_shots, "long_shots")
    validate_not_null(aggression, "aggression")
    validate_not_null(interceptions, "interceptions")
    validate_not_null(positioning, "positioning")
    validate_not_null(vision, "vision")
    validate_not_null(penalties, "penalties")
    validate_not_null(marking, "marking")
    validate_not_null(standing_tackle, "standing_tackle")
    validate_not_null(sliding_tackle, "sliding_tackle")
    validate_not_null(gk_diving, "gk_diving")
    validate_not_null(gk_handling, "gk_handling")
    validate_not_null(gk_kicking, "gk_kicking")
    validate_not_null(gk_positioning, "gk_positioning")
    validate_not_null(gk_reflexes, "gk_reflexes")
    # Validation for required fields
    if not current_datetime:
        raise ValueError("date is required.")
    
    if not isinstance(attacking_work_rate, str):
        raise ValueError("attacking_work_rate must be a string.")

    if not isinstance(defensive_work_rate, str):
        raise ValueError("defensive_work_rate must be a string.")

    if preferred_foot not in ["left", "right", "none"]:
        raise ValueError("Preferred foot must be 'left', 'right', or 'none'.")
    
    # Insert all attributes into the database
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO Player_Attributes (
                    player_api_id,
                    date,
                    overall_rating,
                    potential,
                    preferred_foot,
                    attacking_work_rate,
                    defensive_work_rate,
                    crossing,
                    finishing,
                    heading_accuracy,
                    short_passing,
                    volleys,
                    dribbling,
                    curve,
                    free_kick_accuracy,
                    long_passing,
                    ball_control,
                    acceleration,
                    sprint_speed,
                    agility,
                    reactions,
                    balance,
                    shot_power,
                    jumping,
                    stamina,
                    strength,
                    long_shots,
                    aggression,
                    interceptions,
                    positioning,
                    vision,
                    penalties,
                    marking,
                    standing_tackle,
                    sliding_tackle,
                    gk_diving,
                    gk_handling,
                    gk_kicking,
                    gk_positioning,
                    gk_reflexes
                )
                VALUES (
                    %(player_api_id)s,
                    %(date)s,
                    %(overall_rating)s,
                    %(potential)s,
                    %(preferred_foot)s,
                    %(attacking_work_rate)s,
                    %(defensive_work_rate)s,
                    %(crossing)s,
                    %(finishing)s,
                    %(heading_accuracy)s,
                    %(short_passing)s,
                    %(volleys)s,
                    %(dribbling)s,
                    %(curve)s,
                    %(free_kick_accuracy)s,
                    %(long_passing)s,
                    %(ball_control)s,
                    %(acceleration)s,
                    %(sprint_speed)s,
                    %(agility)s,
                    %(reactions)s,
                    %(balance)s,
                    %(shot_power)s,
                    %(jumping)s,
                    %(stamina)s,
                    %(strength)s,
                    %(long_shots)s,
                    %(aggression)s,
                    %(interceptions)s,
                    %(positioning)s,
                    %(vision)s,
                    %(penalties)s,
                    %(marking)s,
                    %(standing_tackle)s,
                    %(sliding_tackle)s,
                    %(gk_diving)s,
                    %(gk_handling)s,
                    %(gk_kicking)s,
                    %(gk_positioning)s,
                    %(gk_reflexes)s
                );
                """,
                {
                    "player_api_id": player_api_id,
                    "date": current_datetime,
                    "overall_rating": overall_rating,
                    "potential": potential,
                    "preferred_foot": preferred_foot,
                    "attacking_work_rate": attacking_work_rate,
                    "defensive_work_rate": defensive_work_rate,
                    "crossing": crossing,
                    "finishing": finishing,
                    "heading_accuracy": heading_accuracy,
                    "short_passing": short_passing,
                    "volleys": volleys,
                    "dribbling": dribbling,
                    "curve": curve,
                    "free_kick_accuracy": free_kick_accuracy,
                    "long_passing": long_passing,
                    "ball_control": ball_control,
                    "acceleration": acceleration,
                    "sprint_speed": sprint_speed,
                    "agility": agility,
                    "reactions": reactions,
                    "balance": balance,
                    "shot_power": shot_power,
                    "jumping": jumping,
                    "stamina": stamina,
                    "strength": strength,
                    "long_shots": long_shots,
                    "aggression": aggression,
                    "interceptions": interceptions,
                    "positioning": positioning,
                    "vision": vision,
                    "penalties": penalties,
                    "marking": marking,
                    "standing_tackle": standing_tackle,
                    "sliding_tackle": sliding_tackle,
                    "gk_diving": gk_diving,
                    "gk_handling": gk_handling,
                    "gk_kicking": gk_kicking,
                    "gk_positioning": gk_positioning,
                    "gk_reflexes": gk_reflexes,
                },
            )

    return redirect(url_for("player_index"))



@app.route("/accounts/<account_number>/delete", methods=("POST",))
def account_delete(account_number):
    """Delete the account."""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                # BEGIN is executed, a transaction started
                cur.execute(
                    """
                    DELETE FROM depositor
                    WHERE account_number = %(account_number)s;
                    """,
                    {"account_number": account_number},
                )
                cur.execute(
                    """
                    DELETE FROM account
                    WHERE account_number = %(account_number)s;
                    """,
                    {"account_number": account_number},
                )
                # These two operations run atomically in the same transaction

        # COMMIT is executed at the end of the block.
        # The connection is in idle state again.

    # The connection is returned to the pool at the end of the `connection()` context

    return redirect(url_for("account_index"))


@app.route("/ping", methods=("GET",))
@limiter.exempt
def ping():
    log.debug("ping!")
    return jsonify({"message": "mensagem alterada 12357890!", "status": "success"})


if __name__ == "__main__":
    app.run()
