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
                                                                                                                           

@app.route("/players/<player_api_id>/update", methods=("GET",))
@limiter.limit("1 per second")
def player_update_view(player_api_id):
    """Show the page to update the account balance."""

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

    # At the end of the `connection()` context, the transaction is committed
    # or rolled back, and the connection returned to the pool.

    return render_template("players/update.html", player=player)


@app.route("/players/<player_api_id>/update", methods=("POST",))
def player_update_save(player_api_id):
    """Insert new player attributes."""

    # Extract all attributes from the form
    date = request.form["date"]
    overall_rating = request.form["overall_rating"]
    potential = request.form["potential"]
    preferred_foot = request.form["preferred_foot"]
    attacking_work_rate = request.form["attacking_work_rate"]
    defensive_work_rate = request.form["defensive_work_rate"]
    crossing = request.form["crossing"]
    finishing = request.form["finishing"]
    heading_accuracy = request.form["heading_accuracy"]
    short_passing = request.form["short_passing"]
    volleys = request.form["volleys"]
    dribbling = request.form["dribbling"]
    curve = request.form["curve"]
    free_kick_accuracy = request.form["free_kick_accuracy"]
    long_passing = request.form["long_passing"]
    ball_control = request.form["ball_control"]
    acceleration = request.form["acceleration"]
    sprint_speed = request.form["sprint_speed"]
    agility = request.form["agility"]
    reactions = request.form["reactions"]
    balance = request.form["balance"]
    shot_power = request.form["shot_power"]
    jumping = request.form["jumping"]
    stamina = request.form["stamina"]
    strength = request.form["strength"]
    long_shots = request.form["long_shots"]
    aggression = request.form["aggression"]
    interceptions = request.form["interceptions"]
    positioning = request.form["positioning"]
    vision = request.form["vision"]
    penalties = request.form["penalties"]
    marking = request.form["marking"]
    standing_tackle = request.form["standing_tackle"]
    sliding_tackle = request.form["sliding_tackle"]
    gk_diving = request.form["gk_diving"]
    gk_handling = request.form["gk_handling"]
    gk_kicking = request.form["gk_kicking"]
    gk_positioning = request.form["gk_positioning"]
    gk_reflexes = request.form["gk_reflexes"]

    # Validation for required fields
    if not date:
        raise ValueError("date is required.")
    

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
                    "date": date,
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
