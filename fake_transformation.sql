SELECT
  event_name,
  SUM(items.item_quantity) AS total_items_per_event
FROM
  `marcoycaza-tutoriales.ga4_fake_events.events_*`
CROSS JOIN
  UNNEST(ecommerce.items) AS items
WHERE
  _table_suffix = FORMAT_DATE('%Y%m%d',@run_date)
GROUP BY
  event_name