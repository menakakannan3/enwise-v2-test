from alembic import op
from sqlalchemy import text

# Revision identifiers
revision = "s11_site_status_15min_fast_cagg"
down_revision = "s10_add_calibration_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("COMMIT"))

    # -------------------------------------------------------------
    # 1️⃣ Modify column type: station_parameters.param_interval
    #    Convert from INT/NUMBER → NUMERIC(10,2)
    # -------------------------------------------------------------
    conn.execute(text("""
        ALTER TABLE station_parameters
        ALTER COLUMN param_interval TYPE NUMERIC(10,2)
        USING param_interval::NUMERIC(10,2);
    """))

    print("✔ Updated station_parameters.param_interval → NUMERIC(10,2)")

    # -------------------------------------------------------------
    # 2️⃣ Drop old view safely
    # -------------------------------------------------------------
    conn.execute(text("""
        DROP VIEW IF EXISTS public.site_status_15min CASCADE;
    """))

    # -------------------------------------------------------------
    # 3️⃣ Create corrected IST-aligned CURRENT 15-min view 
    #    (reads ONLY from sensor_agg_15min)
    # -------------------------------------------------------------
    conn.execute(text("""
        CREATE VIEW public.site_status_15min AS
        WITH cagg_local AS (
            SELECT
                station_param_id,
                avg_value,
                -- normalize bucket to local IST timestamp (timestamp without time zone)
                timezone('Asia/Kolkata', bucket) AS bucket_local
            FROM public.sensor_agg_15min
        ),
        target AS (
            -- current IST 15-min bucket (10:02, 10:12 → 10:00)
            SELECT time_bucket('15 minutes', now() AT TIME ZONE 'Asia/Kolkata') AS bucket_time
        )
        SELECT
            c.station_param_id,
            c.avg_value,
            t.bucket_time AS bucket_time
        FROM cagg_local c
        CROSS JOIN target t
        WHERE c.bucket_local = t.bucket_time;
    """))

    print("✔ FAST site_status_15min view created (IST-aligned CURRENT bucket)")
    print("✔ Uses sensor_agg_15min ONLY — no heavy sensor_data scan")
    print("✔ Bucket aligned to exact 00/15/30/45 IST timestamps")


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("COMMIT"))

    # -------------------------------------------------------------
    # 1️⃣ Revert column type back to INTEGER
    # -------------------------------------------------------------
    conn.execute(text("""
        ALTER TABLE station_parameters
        ALTER COLUMN param_interval TYPE INTEGER
        USING param_interval::INTEGER;
    """))

    print("✔ Reverted station_parameters.param_interval → INTEGER")

    # -------------------------------------------------------------
    # 2️⃣ Drop the IST-aligned view
    # -------------------------------------------------------------
    conn.execute(text("""
        DROP VIEW IF EXISTS public.site_status_15min CASCADE;
    """))

    print("✔ site_status_15min view dropped (downgrade)")
