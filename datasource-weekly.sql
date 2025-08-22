WITH date_range AS (
  SELECT 
    CONCAT(`sgl_publicpublic.reference_dates`.sagala_month_week,' ',`sgl_publicpublic.reference_dates`.year) AS week_year,
    MIN(`sgl_publicpublic.reference_dates`.first_day_of_month) AS from_date,
    MIN(`sgl_publicpublic.reference_dates`.last_day_of_iso_week) AS until_date
  FROM `sgl_publicpublic.reference_dates`
  WHERE
  `sgl_publicpublic.reference_dates`.year = 2025
  AND `sgl_publicpublic.reference_dates`.date < CURRENT_DATE()
  GROUP BY week_year
)
, cmr AS (
  SELECT
    date_range.week_year AS week_year,
    COALESCE(`sgl_publicpublic.stores`.alternative_name, `sgl_publicpublic.stores`.name) AS store_name,
    ROUND((100 - COALESCE(AVG(`sgl_publicpublic.mar_items`.availability_percentage),0))/100,5) AS cmr_percentage
  FROM `sgl_publicpublic.mar_items`
  LEFT JOIN `sgl_publicpublic.mar_reasons` ON `sgl_publicpublic.mar_reasons`.id = `sgl_publicpublic.mar_items`.mar_reason_id
  JOIN `sgl_publicpublic.merchants` ON `sgl_publicpublic.merchants`.id = `sgl_publicpublic.mar_items`.merchant_id
  JOIN `sgl_publicpublic.stores` ON `sgl_publicpublic.stores`.id = `sgl_publicpublic.merchants`.store_id
  CROSS JOIN date_range
  WHERE 1=1
    AND DATE(`sgl_publicpublic.mar_items`.date) BETWEEN date_range.from_date AND date_range.until_date
    AND (
      `sgl_publicpublic.mar_reasons`.`text` IN (
        'Operational Issue',
        'Kesalahan mematikan opsi alat makan',
        'Menu Lupa Diaktifkan',
        'Store Tidak Memberikan Alasan',
        'Store Belum Siap Beroperasi',
        'Peralatan Rusak',
        'Lainnya'
      )
      OR `sgl_publicpublic.mar_items`.mar_reason_id IS NULL
    )
  GROUP BY
    store_name, 
    date_range.week_year
)
, serving_time AS (
  SELECT
    date_range.week_year AS week_year,
    COALESCE(`sgl_publicpublic.stores`.alternative_name, `sgl_publicpublic.stores`.name) AS store_name,
    ROUND(AVG(`sgl_publicpublic.orders`.kitchen_preparation_duration),2) AS kitchen_preparation_duration
  FROM
    `sgl_publicpublic.orders`
  JOIN `sgl_publicpublic.merchants` ON `sgl_publicpublic.merchants`.id = `sgl_publicpublic.orders`.merchant_id
  JOIN `sgl_publicpublic.stores` ON `sgl_publicpublic.stores`.id = `sgl_publicpublic.merchants`.store_id
  CROSS JOIN date_range
  WHERE 1=1
    AND DATE(`sgl_publicpublic.orders`.ordered_at, 'Asia/Jakarta') BETWEEN date_range.from_date AND date_range.until_date
    AND `sgl_publicpublic.orders`.status = 'completed'
    AND `sgl_publicpublic.merchants`.channel_id IN (1,2,3)
  GROUP BY
    store_name, 
    date_range.week_year
  ORDER BY store_name
)
, current_revenue AS (
  SELECT
    date_range.week_year AS week_year,
    COALESCE(`sgl_publicpublic.stores`.alternative_name, `sgl_publicpublic.stores`.name) AS store_name,
    COUNT(DISTINCT sb.reference_external_id) AS trx_count
  FROM `sgl_publicpublic.salesorder_brands` sb
  JOIN `sgl_publicpublic.stores` ON sb.store_id = `sgl_publicpublic.stores`.id
  JOIN `sgl_publicpublic.brands` ON sb.brand_id = `sgl_publicpublic.brands`.id AND `sgl_publicpublic.brands`.deleted_at IS NULL
  CROSS JOIN date_range
  WHERE 1=1
    AND DATE(sb.ordered_at, 'Asia/Jakarta') BETWEEN date_range.from_date AND date_range.until_date
    AND sb.status = "completed"
    AND sb.channel_id IN (1,2,3)
    AND sb.reference_external_id IS NOT NULL
  GROUP BY
    store_name, 
    date_range.week_year
  ORDER BY store_name
)
, current_revenue_2 AS (
  SELECT
    date_range.week_year AS week_year,
    COALESCE(`sgl_publicpublic.stores`.alternative_name, `sgl_publicpublic.stores`.name) AS store_name,
    COUNT(DISTINCT sb.reference_external_id) AS trx_count
  FROM `sgl_publicpublic.salesorder_brands` sb
  JOIN `sgl_publicpublic.stores` ON sb.store_id = `sgl_publicpublic.stores`.id
  JOIN `sgl_publicpublic.brands` ON sb.brand_id = `sgl_publicpublic.brands`.id AND `sgl_publicpublic.brands`.deleted_at IS NULL
  CROSS JOIN date_range
  WHERE 1=1
    AND DATE(sb.ordered_at, 'Asia/Jakarta') BETWEEN date_range.from_date AND date_range.until_date
    AND sb.status = "completed"
    AND sb.channel_id IN (1,2,3,4)
    AND sb.reference_external_id IS NOT NULL
  GROUP BY
    store_name, 
    date_range.week_year
  ORDER BY store_name
)
, complain_data AS (
  SELECT
    date_range.week_year AS week_year,
    COALESCE(`sgl_publicpublic.stores`.alternative_name, `sgl_publicpublic.stores`.name) AS store_name,
    ROUND(COUNT(DISTINCT `sgl_publicpublic.reviews`.id) / current_revenue_2.trx_count, 5) AS complain_rate,
    COUNT(DISTINCT `sgl_publicpublic.reviews`.id) AS complain_count,
    current_revenue_2.trx_count AS trx
  FROM `sgl_publicpublic.reviews`
  LEFT JOIN `sgl_publicpublic.stores` ON `sgl_publicpublic.stores`.id = `sgl_publicpublic.reviews`.store_id
  CROSS JOIN date_range
  LEFT JOIN current_revenue_2 ON current_revenue_2.store_name = `sgl_publicpublic.stores`.alternative_name AND current_revenue_2.week_year = date_range.week_year
  WHERE 1=1
    AND DATE(`sgl_publicpublic.reviews`.reviewed_at, 'Asia/Jakarta') BETWEEN date_range.from_date AND date_range.until_date
    AND `sgl_publicpublic.reviews`.is_valid_complaint IS TRUE
    AND `sgl_publicpublic.reviews`.review_category = 'warning'
  GROUP BY store_name, current_revenue_2.trx_count, date_range.week_year
)
, cancellation AS (
  SELECT
    date_range.week_year AS week_year,
    COALESCE(`sgl_publicpublic.stores`.alternative_name, `sgl_publicpublic.stores`.name) AS store_name,
    ROUND(COUNT(*) / current_revenue.trx_count, 5) AS cancel_rate
  FROM `sgl_publicpublic.orders`
  JOIN `sgl_publicpublic.merchants` ON `sgl_publicpublic.merchants`.id = `sgl_publicpublic.orders`.merchant_id
  JOIN `sgl_publicpublic.stores` ON `sgl_publicpublic.stores`.id = `sgl_publicpublic.merchants`.store_id
  JOIN `sgl_publicpublic.channels` ON `sgl_publicpublic.channels`.id = `sgl_publicpublic.merchants`.channel_id
  CROSS JOIN date_range
  LEFT JOIN current_revenue ON current_revenue.store_name = `sgl_publicpublic.stores`.alternative_name AND current_revenue.week_year = date_range.week_year
  WHERE 1=1
    AND DATE(`sgl_publicpublic.orders`.ordered_at, 'Asia/Jakarta') BETWEEN date_range.from_date AND date_range.until_date
    AND `sgl_publicpublic.orders`.status = 'cancelled'
    AND `sgl_publicpublic.orders`.cancel_proposer = 'merchant'
    AND `sgl_publicpublic.orders`.cancel_reason IN ('merchant_already_closed', 'driver_cant_found_merchant', 'merchant_busy', 'customer_waited_too_long')
    AND NOT (`sgl_publicpublic.channels`.id = 1 AND `sgl_publicpublic.orders`.cancel_proposer = 'buyer' AND `sgl_publicpublic.orders`.cancel_reason = 'customer_wants_to_cancel_order')
    AND NOT (`sgl_publicpublic.channels`.id = 1 AND `sgl_publicpublic.orders`.cancel_proposer = 'system' AND `sgl_publicpublic.orders`.cancel_reason IN ('payment_failed', 'customer_wants_to_cancel_order'))
  GROUP BY store_name, current_revenue.trx_count, date_range.week_year
),

raw_data_servingtime AS (
  SELECT 
    dt.week_year,
    DATE(`sgl_publicpublic.salesorders`.ordered_at, 'Asia/Jakarta') AS date,
    COALESCE(st.alternative_name, st.name) AS store,
    sb.reference_external_id AS external_id,
    ch.name AS channels,
    br.name AS brands,
    EXTRACT(HOUR FROM DATETIME(`sgl_publicpublic.salesorders`.ordered_at, 'Asia/Jakarta')) AS group_hours,
    EXTRACT(TIME FROM DATETIME(`sgl_publicpublic.salesorders`.ordered_at, 'Asia/Jakarta')) AS jam_order,
    COALESCE(
      DATETIME_DIFF(
        DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.ready_at), 'Asia/Jakarta'),
        DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.accepted_at), 'Asia/Jakarta'),
        SECOND
      ),
      o.kitchen_preparation_duration
    ) AS serving_time,

    -- sebaran servingtime
    CASE 
      WHEN COALESCE(
        DATETIME_DIFF(
          DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.ready_at), 'Asia/Jakarta'),
          DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.accepted_at), 'Asia/Jakarta'),
          SECOND
        ),
        o.kitchen_preparation_duration
      ) < 420 THEN '< 7 menit'
      WHEN COALESCE(
        DATETIME_DIFF(
          DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.ready_at), 'Asia/Jakarta'),
          DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.accepted_at), 'Asia/Jakarta'),
          SECOND
        ),
        o.kitchen_preparation_duration
      ) BETWEEN 420 AND 840 THEN '7-14 menit'
      WHEN COALESCE(
        DATETIME_DIFF(
          DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.ready_at), 'Asia/Jakarta'),
          DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.accepted_at), 'Asia/Jakarta'),
          SECOND
        ),
        o.kitchen_preparation_duration
      ) > 840 THEN '> 14 menit'
      ELSE 'unknown'
    END AS serving_time_range

  FROM `sgl_publicpublic.salesorders`
  JOIN `sgl_publicpublic.salesorder_brands` sb 
    ON sb.reference_external_id = `sgl_publicpublic.salesorders`.reference_external_id
  LEFT JOIN `sgl_publicpublic.channels` ch 
    ON ch.id = `sgl_publicpublic.salesorders`.channel_id
  LEFT JOIN `sgl_publicpublic.brands` br 
    ON br.id = sb.brand_id 
  LEFT JOIN `sgl_publicpublic.stores` st 
    ON st.id = `sgl_publicpublic.salesorders`.store_id
  JOIN `sgl_publicpublic.orders` o 
    ON o.external_id = `sgl_publicpublic.salesorders`.reference_external_id
  CROSS JOIN date_range dt 

  WHERE 1=1
    AND `sgl_publicpublic.salesorders`.status = 'completed'
    AND `sgl_publicpublic.salesorders`.deleted_at IS NULL
    AND ch.id IN (1,2,3)
    AND COALESCE(
          DATETIME_DIFF(
            DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.ready_at), 'Asia/Jakarta'),
            DATETIME(TIMESTAMP(`sgl_publicpublic.salesorders`.accepted_at), 'Asia/Jakarta'),
            SECOND
          ),
          o.kitchen_preparation_duration
        ) > 20 -- buang data absurd <20 detik
),
servingtime_v2 AS(
SELECT 
  week_year,
  store,
  COUNTIF(serving_time_range = '> 14 menit') * 1.0 / COUNT(*) AS pct
FROM raw_data_servingtime
GROUP BY store, week_year
ORDER BY store, week_year
)
SELECT
  date_range.week_year AS week_year,
  COALESCE(s.alternative_name, s.name) AS store,
  COALESCE(cmr.cmr_percentage, 0) AS cmr_percentage,
  COALESCE(serving_time.kitchen_preparation_duration, 0) AS avg_serving_time,
  COALESCE(complain_data.complain_rate, 0) AS complain_rate,
  COALESCE(cancellation.cancel_rate, 0) AS cancel_rate,
  COALESCE(servingtime_v2.pct, 0) AS servingtime_v2,
  date_range.from_date AS from_date,
  date_range.until_date AS until_date
FROM `sgl_publicpublic.stores` s
CROSS JOIN date_range
LEFT JOIN cmr ON cmr.store_name = s.alternative_name AND cmr.week_year = date_range.week_year
LEFT JOIN serving_time ON serving_time.store_name = s.alternative_name AND serving_time.week_year = date_range.week_year
LEFT JOIN complain_data ON complain_data.store_name = s.alternative_name AND complain_data.week_year = date_range.week_year
LEFT JOIN cancellation ON cancellation.store_name = s.alternative_name AND cancellation.week_year = date_range.week_year
LEFT JOIN servingtime_v2 ON servingtime_v2.store = s.alternative_name AND servingtime_v2.week_year = date_range.week_year
WHERE s.is_online_sales IS TRUE
ORDER BY store, until_date;
