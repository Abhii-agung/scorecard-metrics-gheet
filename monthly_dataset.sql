WITH date_range AS (
  SELECT 
    `sgl_publicpublic.reference_dates`.month_name_year AS month_year,
    MIN(`sgl_publicpublic.reference_dates`.first_day_of_month) AS from_date,
    MIN(`sgl_publicpublic.reference_dates`.last_day_of_month) AS until_date
  FROM `sgl_publicpublic.reference_dates`
  WHERE
    `sgl_publicpublic.reference_dates`.year = 2025
    AND `sgl_publicpublic.reference_dates`.date < CURRENT_DATE()
  GROUP BY month_year
)
, cmr AS (
  SELECT
    date_range.month_year AS month_year,
    `sgl_publicpublic.stores`.alternative_name AS store_name,
    ROUND((100 - COALESCE(AVG(`sgl_publicpublic.mar_items`.availability_percentage),0))/100,5) AS cmr_percentage
  FROM `sgl_publicpublic.mar_items`
  LEFT JOIN `sgl_publicpublic.mar_reasons` ON `sgl_publicpublic.mar_reasons`.id = `sgl_publicpublic.mar_items`.mar_reason_id
  JOIN `sgl_publicpublic.merchants` ON `sgl_publicpublic.merchants`.id = `sgl_publicpublic.mar_items`.merchant_id
  JOIN `sgl_publicpublic.stores` ON `sgl_publicpublic.stores`.id = `sgl_publicpublic.merchants`.store_id
  CROSS JOIN date_range
  WHERE 1=1
    AND DATE(`sgl_publicpublic.mar_items`.date) BETWEEN date_range.from_date AND date_range.until_date
    AND (
      (`sgl_publicpublic.mar_reasons`.`text` IN (
        'Operational Issue',
        'Kesalahan mematikan opsi alat makan',
        'Menu Lupa Diaktifkan',
        'Store Tidak Memberikan Alasan',
        'Store Belum Siap Beroperasi',
        'Peralatan Rusak',
        'Lainnya'
      )) 
      OR `sgl_publicpublic.mar_items`.mar_reason_id IS NULL
    )
  GROUP BY store_name, date_range.month_year
)
, serving_time AS (
  SELECT
    date_range.month_year AS month_year,
    `sgl_publicpublic.stores`.alternative_name AS store_name,
    ROUND(AVG(`sgl_publicpublic.orders`.kitchen_preparation_duration),2) AS kitchen_preparation_duration
  FROM `sgl_publicpublic.orders`
  JOIN `sgl_publicpublic.merchants` ON `sgl_publicpublic.merchants`.id = `sgl_publicpublic.orders`.merchant_id
  JOIN `sgl_publicpublic.stores` ON `sgl_publicpublic.stores`.id = `sgl_publicpublic.merchants`.store_id
  CROSS JOIN date_range
  WHERE 1=1
    AND DATE(`sgl_publicpublic.orders`.ordered_at, 'Asia/Jakarta') BETWEEN date_range.from_date AND date_range.until_date
    AND `sgl_publicpublic.orders`.status = 'completed'
    AND `sgl_publicpublic.merchants`.channel_id IN (1,2,3)
  GROUP BY store_name, date_range.month_year
)
, current_revenue AS (
  SELECT
    date_range.month_year AS month_year,
    `sgl_publicpublic.stores`.alternative_name AS store_name,
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
  GROUP BY store_name, date_range.month_year
)
, current_revenue_2 AS (
  SELECT
    date_range.month_year AS month_year,
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
  GROUP BY store_name, date_range.month_year
)
, complain_data AS (
  SELECT
    date_range.month_year AS month_year,
    `sgl_publicpublic.stores`.alternative_name AS store_name,
    ROUND(COUNT(DISTINCT `sgl_publicpublic.reviews`.id) / current_revenue_2.trx_count,5) as complain_rate,
    COUNT(DISTINCT `sgl_publicpublic.reviews`.id) As complain_count,
    current_revenue_2.trx_count as trx
  FROM `sgl_publicpublic.reviews`
  LEFT JOIN `sgl_publicpublic.stores` on `sgl_publicpublic.stores`.id = `sgl_publicpublic.reviews`.store_id
  CROSS JOIN date_range
  LEFT JOIN current_revenue_2 ON current_revenue_2.store_name = `sgl_publicpublic.stores`.alternative_name AND current_revenue_2.month_year = date_range.month_year
  WHERE 1=1
    AND DATE(`sgl_publicpublic.reviews`.reviewed_at, 'Asia/Jakarta') BETWEEN date_range.from_date AND date_range.until_date
    AND `sgl_publicpublic.reviews`.is_valid_complaint IS TRUE 
    AND `sgl_publicpublic.reviews`.review_category = 'warning'
  GROUP BY store_name, current_revenue_2.trx_count, date_range.month_year
)
, cancellation AS (
  SELECT
    date_range.month_year AS month_year,
    `sgl_publicpublic.stores`.alternative_name AS store_name,
    ROUND(COUNT(*) / current_revenue.trx_count,5) as cancel_rate
  FROM `sgl_publicpublic.orders`
  JOIN `sgl_publicpublic.merchants` ON `sgl_publicpublic.merchants`.id = `sgl_publicpublic.orders`.merchant_id
  JOIN `sgl_publicpublic.stores` ON `sgl_publicpublic.stores`.id = `sgl_publicpublic.merchants`.store_id
  JOIN `sgl_publicpublic.channels` ON `sgl_publicpublic.channels`.id = `sgl_publicpublic.merchants`.channel_id
  CROSS JOIN date_range
  LEFT JOIN current_revenue ON current_revenue.store_name = `sgl_publicpublic.stores`.alternative_name AND current_revenue.month_year = date_range.month_year
  WHERE 1=1
    AND DATE(`sgl_publicpublic.orders`.ordered_at, 'Asia/Jakarta') BETWEEN date_range.from_date AND date_range.until_date
    AND `sgl_publicpublic.orders`.status = 'cancelled'
    AND (`sgl_publicpublic.orders`.cancel_proposer = 'merchant')
    AND `sgl_publicpublic.orders`.cancel_reason IN ('merchant_already_closed', 'driver_cant_found_merchant', 'merchant_busy','customer_waited_too_long')
    AND NOT (`sgl_publicpublic.channels`.id = 1 AND `sgl_publicpublic.orders`.cancel_proposer = 'buyer' AND `sgl_publicpublic.orders`.cancel_reason = 'customer_wants_to_cancel_order')
    AND NOT (`sgl_publicpublic.channels`.id = 1 AND `sgl_publicpublic.orders`.cancel_proposer = 'system' AND `sgl_publicpublic.orders`.cancel_reason = 'payment_failed')
    AND NOT (`sgl_publicpublic.channels`.id = 1 AND `sgl_publicpublic.orders`.cancel_proposer = 'system' AND `sgl_publicpublic.orders`.cancel_reason = 'customer_wants_to_cancel_order')
  GROUP BY store_name, current_revenue.trx_count, date_range.month_year
)

SELECT
  date_range.month_year AS month_year,
  s.alternative_name AS store,
  COALESCE(cmr.cmr_percentage,0) AS cmr_percentage,
  COALESCE(serving_time.kitchen_preparation_duration,0) AS avg_serving_time,
  COALESCE(complain_data.complain_rate,0) AS complain_rate,
  COALESCE(cancellation.cancel_rate,0) AS cancel_rate,
  date_range.from_date AS from_date,
  date_range.until_date AS until_date
FROM `sgl_publicpublic.stores` s
CROSS JOIN date_range
LEFT JOIN cmr ON cmr.store_name = s.alternative_name AND cmr.month_year = date_range.month_year
LEFT JOIN serving_time ON serving_time.store_name = s.alternative_name AND serving_time.month_year = date_range.month_year
LEFT JOIN complain_data ON complain_data.store_name = s.alternative_name AND complain_data.month_year = date_range.month_year
LEFT JOIN cancellation ON cancellation.store_name = s.alternative_name AND cancellation.month_year = date_range.month_year
WHERE s.is_online_sales IS TRUE
ORDER BY store, until_date
