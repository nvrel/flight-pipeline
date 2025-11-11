#!/usr/bin/env bash
set -euo pipefail
mkdir -p out/quality/months

echo "[months] flight_clean"
spark-sql -S --output-format csv -e "
SELECT date_format(FL_DATE,'yyyy-MM') AS month, count(*) AS n
FROM delta.'out/flight_clean.parquet'
GROUP BY 1 ORDER BY 1;
" | tee out/quality/months/flight_clean_months.csv

echo "[months] weather_clean"
spark-sql -S --output-format csv -e "
SELECT date_format(timestamp,'yyyy-MM') AS month, count(*) AS n
FROM delta.'out/weather_clean.parquet'
GROUP BY 1 ORDER BY 1;
" | tee out/quality/months/weather_clean_months.csv

echo "[months] join_intermediate"
spark-sql -S --output-format csv -e "
SELECT date_format(FL_DATE,'yyyy-MM') AS month, count(*) AS n
FROM delta.'out/join_intermediate.parquet'
GROUP BY 1 ORDER BY 1;
" | tee out/quality/months/join_intermediate_months.csv

echo "[months] join_flat_lag7"
spark-sql -S --output-format csv -e "
SELECT date_format(FL_DATE,'yyyy-MM') AS month, count(*) AS n
FROM delta.'out/join_flat_lag7.parquet'
GROUP BY 1 ORDER BY 1;
" | tee out/quality/months/join_flat_lag7_months.csv
