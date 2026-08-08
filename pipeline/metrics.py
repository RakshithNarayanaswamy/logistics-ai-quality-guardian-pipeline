from pipeline.snowflake_utils import get_connection

_ANALYTICS_SCHEMA = "ANALYTICS"


def _num(value):
    """Snowflake returns NUMBER columns as Decimal. Left as-is, pandas gives
    them `object` dtype and plotting libraries treat them as categories rather
    than magnitudes — so coerce to float at the boundary.
    """
    return None if value is None else float(value)


def compute_metrics():
    """Deterministic-only: queries dbt's marts for the 7 core supply-chain metrics.
    No AI here — see ai/metric_assistant.py for the narration layer.
    """
    conn = get_connection(_ANALYTICS_SCHEMA)
    try:
        cur = conn.cursor()

        # 1. OTIF
        cur.execute("""
            SELECT ROUND(
                100.0 * SUM(IFF(is_on_time, 1, 0))
                / NULLIF(SUM(IFF(status = 'delivered', 1, 0)), 0),
            1)
            FROM fact_shipments
        """)
        otif_pct = _num(cur.fetchone()[0])

        # 4. Lead time / cycle time
        cur.execute("""
            SELECT ROUND(AVG(lead_time_days), 1)
            FROM fact_shipments
            WHERE lead_time_days IS NOT NULL
        """)
        avg_lead_time_days = _num(cur.fetchone()[0])

        # 2. Shipment status transitions — avg hours spent between each pair of stages
        cur.execute("""
            WITH ordered AS (
                SELECT
                    shipment_id,
                    event_status,
                    event_ts,
                    LEAD(event_ts)
                        OVER (PARTITION BY shipment_id ORDER BY event_sequence) AS next_ts,
                    LEAD(event_status)
                        OVER (PARTITION BY shipment_id ORDER BY event_sequence) AS next_status
                FROM fact_shipment_status_history
            )
            SELECT
                event_status || ' -> ' || next_status AS transition,
                ROUND(AVG(DATEDIFF('hour', event_ts, next_ts)), 1) AS avg_hours
            FROM ordered
            WHERE next_ts IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """)
        status_transition_avg_hours = [
            {"transition": r[0], "avg_hours": _num(r[1])} for r in cur.fetchall()
        ]

        # 3. Inventory levels / stockouts — most recent snapshot date
        cur.execute("""
            WITH latest AS (
                SELECT MAX(snapshot_date) AS max_date FROM fact_inventory_snapshot
            )
            SELECT
                ROUND(100.0 * SUM(IFF(warehouse_inventory = 0, 1, 0)) / COUNT(*), 1)
            FROM fact_inventory_snapshot, latest
            WHERE snapshot_date = latest.max_date
        """)
        stockout_rate_pct = _num(cur.fetchone()[0])

        # 5. Carrier performance & cost
        cur.execute("""
            SELECT
                c.carrier_name,
                ROUND(
                    100.0 * SUM(IFF(f.is_on_time, 1, 0))
                    / NULLIF(SUM(IFF(f.status = 'delivered', 1, 0)), 0),
                1) AS otif_pct,
                ROUND(AVG(f.cost), 2) AS avg_cost
            FROM fact_shipments f
            JOIN dim_carrier c ON f.carrier_key = c.carrier_key
            GROUP BY c.carrier_name
            ORDER BY otif_pct ASC
        """)
        carrier_performance = [
            {"carrier_name": r[0], "otif_pct": _num(r[1]), "avg_cost": _num(r[2])}
            for r in cur.fetchall()
        ]

        # 6. Demand forecasting inputs
        cur.execute("""
            SELECT part_name, SUM(predicted_quantity) AS next_7_day_forecast
            FROM demand_forecast
            GROUP BY part_name
            ORDER BY next_7_day_forecast DESC
        """)
        demand_forecast = [
            {"part_name": r[0], "next_7_day_total": _num(r[1])}
            for r in cur.fetchall()
        ]

        # 7. Exceptions & disruptions — delay rate overall + by carrier
        cur.execute("""
            SELECT ROUND(100.0 * SUM(IFF(status = 'delayed', 1, 0)) / COUNT(*), 1)
            FROM fact_shipments
        """)
        delay_rate_pct = _num(cur.fetchone()[0])

        cur.execute("""
            SELECT
                c.carrier_name,
                ROUND(100.0 * SUM(IFF(f.status = 'delayed', 1, 0)) / COUNT(*), 1) AS delay_rate_pct
            FROM fact_shipments f
            JOIN dim_carrier c ON f.carrier_key = c.carrier_key
            GROUP BY c.carrier_name
            ORDER BY delay_rate_pct DESC
        """)
        delay_rate_by_carrier = [
            {"carrier_name": r[0], "delay_rate_pct": _num(r[1])} for r in cur.fetchall()
        ]

        return {
            "otif_pct": otif_pct,
            "avg_lead_time_days": avg_lead_time_days,
            "status_transition_avg_hours": status_transition_avg_hours,
            "stockout_rate_pct": stockout_rate_pct,
            "carrier_performance": carrier_performance,
            "demand_forecast": demand_forecast,
            "delay_rate_pct": delay_rate_pct,
            "delay_rate_by_carrier": delay_rate_by_carrier,
        }
    finally:
        cur.close()
        conn.close()
