#!/bin/sh
# Load tab_actual_export.csv into Tab_Actual. Historical rows get Idx = a1, a2, a3, ...
# so they never conflict with poller data (Idx = 1, 2, 3, ...). Safe to run multiple times.

set -e
CSV="${CSV_PATH:-/mnt/csv/tab_actual_export.csv}"
if [ ! -f "$CSV" ]; then
  echo "No CSV at $CSV, skipping historical load."
  exit 0
fi

echo "Loading historical data from $CSV into Tab_Actual (Idx = a1, a2, a3, ...)..."
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'ALTER TABLE "Tab_Actual" ALTER COLUMN "Idx" TYPE TEXT USING "Idx"::TEXT;' 2>/dev/null || true
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOSQL
CREATE TEMP TABLE _csv_tab_actual (
  "Idx"           INTEGER,
  "TrendDate"     TIMESTAMPTZ,
  "Val_1"         DOUBLE PRECISION,
  "Val_2"         DOUBLE PRECISION,
  "Val_3"         DOUBLE PRECISION,
  "Val_4"         DOUBLE PRECISION,
  "Val_5"         DOUBLE PRECISION,
  "Val_6"         DOUBLE PRECISION,
  "Val_7"         DOUBLE PRECISION,
  "Val_8"         DOUBLE PRECISION,
  "Val_9"         DOUBLE PRECISION,
  "Val_10"        DOUBLE PRECISION,
  "Val_11"        DOUBLE PRECISION,
  "Val_12"        DOUBLE PRECISION,
  "Val_14"        DOUBLE PRECISION,
  "Val_15"        DOUBLE PRECISION,
  "Val_19"        DOUBLE PRECISION,
  "Val_20"        DOUBLE PRECISION,
  "Val_21"        DOUBLE PRECISION,
  "Val_22"        DOUBLE PRECISION,
  "Val_23"        DOUBLE PRECISION,
  "Val_27"        DOUBLE PRECISION,
  "Val_28"        DOUBLE PRECISION,
  "Val_29"        DOUBLE PRECISION,
  "Val_30"        DOUBLE PRECISION,
  "Val_31"        DOUBLE PRECISION,
  "Val_32"        DOUBLE PRECISION,
  "Val_33"        DOUBLE PRECISION,
  "Val_34"        DOUBLE PRECISION,
  "Val_35"        DOUBLE PRECISION,
  "Val_36"        DOUBLE PRECISION,
  "Val_37"        DOUBLE PRECISION,
  "Val_38"        DOUBLE PRECISION,
  "Val_39"        DOUBLE PRECISION,
  "Val_40"        DOUBLE PRECISION,
  "Val_41"        DOUBLE PRECISION,
  "Val_42"        DOUBLE PRECISION,
  "Val_43"        DOUBLE PRECISION,
  "Val_44"        DOUBLE PRECISION,
  "Val_45"        DOUBLE PRECISION,
  "Val_46"        DOUBLE PRECISION,
  "Val_47"        DOUBLE PRECISION,
  "Val_48"        DOUBLE PRECISION
);
COPY _csv_tab_actual FROM '$CSV' WITH (FORMAT csv, HEADER true);
-- Replace old unique index (TrendDate-only) with (TrendDate, Idx) so many rows per timestamp are allowed
DROP INDEX IF EXISTS tab_actual_uniq_time_pk CASCADE;
CREATE UNIQUE INDEX IF NOT EXISTS tab_actual_uniq_time_pk ON "Tab_Actual" ("TrendDate", "Idx");
INSERT INTO "Tab_Actual" (
  "Idx", "TrendDate", "Val_1", "Val_2", "Val_3", "Val_4", "Val_5", "Val_6", "Val_7", "Val_8", "Val_9", "Val_10",
  "Val_11", "Val_12", "Val_14", "Val_15", "Val_19", "Val_20", "Val_21", "Val_22", "Val_23",
  "Val_27", "Val_28", "Val_29", "Val_30", "Val_31", "Val_32", "Val_33", "Val_34", "Val_35", "Val_36", "Val_37",
  "Val_38", "Val_39", "Val_40", "Val_41", "Val_42", "Val_43", "Val_44", "Val_45", "Val_46", "Val_47", "Val_48"
)
SELECT
  'a' || row_number() OVER (ORDER BY "TrendDate", "Idx"),
  "TrendDate", "Val_1", "Val_2", "Val_3", "Val_4", "Val_5", "Val_6", "Val_7", "Val_8", "Val_9", "Val_10",
  "Val_11", "Val_12", "Val_14", "Val_15", "Val_19", "Val_20", "Val_21", "Val_22", "Val_23",
  "Val_27", "Val_28", "Val_29", "Val_30", "Val_31", "Val_32", "Val_33", "Val_34", "Val_35", "Val_36", "Val_37",
  "Val_38", "Val_39", "Val_40", "Val_41", "Val_42", "Val_43", "Val_44", "Val_45", "Val_46", "Val_47", "Val_48"
FROM _csv_tab_actual;
EOSQL
echo "Historical load done."
