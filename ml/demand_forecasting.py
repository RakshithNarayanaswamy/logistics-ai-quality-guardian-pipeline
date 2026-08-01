import logging
import os

import joblib
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARTIFACT_PATH = os.path.join(BASE_DIR, "ml", "artifacts", "demand_model.joblib")

load_dotenv(os.path.join(BASE_DIR, ".env"))

_DATABASE = "LOGISTICS_PIPELINE"
_SCHEMA = "ANALYTICS"
_FORECAST_HORIZON_DAYS = 7
_FEATURES = ["day_of_week", "day_of_month", "lag_1", "lag_7", "rolling_mean_7"]


def _get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        database=_DATABASE,
        schema=_SCHEMA,
    )


def _load_daily_demand(conn):
    query = """
        select
            f.order_date_key as order_date,
            p.part_name,
            sum(f.quantity) as total_quantity
        from fact_shipments f
        join dim_part p on f.part_key = p.part_key
        group by 1, 2
        order by 1, 2
    """
    df = pd.read_sql(query, conn)
    df.columns = [c.lower() for c in df.columns]
    return df


def _add_features(df):
    df = df.sort_values(["part_name", "order_date"]).copy()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["day_of_week"] = df["order_date"].dt.dayofweek
    df["day_of_month"] = df["order_date"].dt.day

    grp = df.groupby("part_name")["total_quantity"]
    df["lag_1"] = grp.shift(1)
    df["lag_7"] = grp.shift(7)
    df["rolling_mean_7"] = grp.transform(lambda s: s.shift(1).rolling(7).mean())
    return df


def _part_dummy_cols(df):
    return [c for c in df.columns if c.startswith("part_")]


def train_and_evaluate():
    conn = _get_connection()
    try:
        raw = _load_daily_demand(conn)
    finally:
        conn.close()

    featured = _add_features(raw)
    encoded = pd.get_dummies(featured, columns=["part_name"], prefix="part")
    part_cols = _part_dummy_cols(encoded)
    encoded = encoded.dropna(subset=_FEATURES)

    cutoff = encoded["order_date"].max() - pd.Timedelta(days=_FORECAST_HORIZON_DAYS)
    train = encoded[encoded["order_date"] <= cutoff]
    test = encoded[encoded["order_date"] > cutoff]

    feature_cols = _FEATURES + part_cols
    model = GradientBoostingRegressor(random_state=42)
    model.fit(train[feature_cols], train["total_quantity"])

    if len(test) > 0:
        preds = model.predict(test[feature_cols])
        mae = mean_absolute_error(test["total_quantity"], preds)
        rmse = mean_squared_error(test["total_quantity"], preds) ** 0.5
        logging.info(
            "demand_forecast — held-out MAE=%.1f RMSE=%.1f (test n=%d)",
            mae, rmse, len(test),
        )

    os.makedirs(os.path.dirname(ARTIFACT_PATH), exist_ok=True)
    joblib.dump(
        {"model": model, "feature_cols": feature_cols, "part_cols": part_cols},
        ARTIFACT_PATH,
    )

    return model, feature_cols, part_cols, featured


def _forecast_forward(model, feature_cols, part_cols, history):
    """Iteratively forecast _FORECAST_HORIZON_DAYS ahead per part.

    Each step's lag/rolling features depend on the previous step's prediction,
    so this must run one day at a time rather than as a single batch predict.
    """
    forecasts = []
    for part_name in history["part_name"].unique():
        part_history = history[history["part_name"] == part_name].sort_values("order_date").copy()
        last_date = part_history["order_date"].max()

        for step in range(1, _FORECAST_HORIZON_DAYS + 1):
            forecast_date = last_date + pd.Timedelta(days=step)
            recent = part_history["total_quantity"].tolist()

            row = {
                "day_of_week": forecast_date.dayofweek,
                "day_of_month": forecast_date.day,
                "lag_1": recent[-1],
                "lag_7": recent[-7] if len(recent) >= 7 else recent[0],
                "rolling_mean_7": sum(recent[-7:]) / len(recent[-7:]),
            }
            for col in part_cols:
                row[col] = 1 if col == f"part_{part_name}" else 0

            x = pd.DataFrame([row])[feature_cols]
            predicted_quantity = float(model.predict(x)[0])

            forecasts.append({
                "order_date": forecast_date,
                "part_name": part_name,
                "predicted_quantity": predicted_quantity,
            })
            part_history = pd.concat([
                part_history,
                pd.DataFrame([{"order_date": forecast_date, "part_name": part_name,
                                "total_quantity": predicted_quantity}]),
            ], ignore_index=True)

    return pd.DataFrame(forecasts)


def _write_forecast(conn, forecast_df):
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE OR REPLACE TABLE {_DATABASE}.{_SCHEMA}.demand_forecast (
                order_date DATE,
                part_name STRING,
                predicted_quantity FLOAT
            )
        """)
        rows = [
            (row.order_date.date().isoformat(), row.part_name, row.predicted_quantity)
            for row in forecast_df.itertuples()
        ]
        cur.executemany(
            f"INSERT INTO {_DATABASE}.{_SCHEMA}.demand_forecast "
            f"(order_date, part_name, predicted_quantity) VALUES (%s, %s, %s)",
            rows,
        )
    finally:
        cur.close()


def run():
    model, feature_cols, part_cols, history = train_and_evaluate()
    forecast_df = _forecast_forward(model, feature_cols, part_cols, history)

    conn = _get_connection()
    try:
        _write_forecast(conn, forecast_df)
    finally:
        conn.close()

    logging.info("demand_forecast — wrote %d forecast rows to %s.%s.demand_forecast",
                 len(forecast_df), _DATABASE, _SCHEMA)
    return forecast_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
