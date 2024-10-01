WITH cirque_du_jaffle_shows AS (
  SELECT
    show_id,
    show_name,
    show_description,
    location_id,
    total_concession_revenue,
    total_jaffle_revenue,
    first_performed_date,
    last_performed_date,
    total_performances,
    total_ticket_revenue
  FROM {{ ref('cirque_du_jaffle_shows') }}
), cirque_du_jaffle_show_cast_lists AS (
  SELECT
    show_id,
    show_name,
    performer_id,
    first_name,
    last_name,
    talent
  FROM {{ ref('cirque_du_jaffle_show_cast_lists') }}
), join_1 AS (
  SELECT
    cirque_du_jaffle_show_cast_lists.first_name,
    cirque_du_jaffle_show_cast_lists.last_name,
    cirque_du_jaffle_show_cast_lists.talent,
    cirque_du_jaffle_shows.total_concession_revenue,
    cirque_du_jaffle_shows.total_jaffle_revenue,
    cirque_du_jaffle_shows.total_performances,
    cirque_du_jaffle_shows.total_ticket_revenue
  FROM cirque_du_jaffle_show_cast_lists
  JOIN cirque_du_jaffle_shows
    ON cirque_du_jaffle_show_cast_lists.show_id = cirque_du_jaffle_shows.show_id
), aggregate_1 AS (
  SELECT
    first_name AS first_name,
    last_name AS last_name,
    talent AS talent,
    SUM(total_jaffle_revenue) AS total_jaffle_revenue,
    SUM(total_performances) AS total_performances
  FROM join_1
  GROUP BY
    1,
    2,
    3
), formula_1 AS (
  SELECT
    first_name,
    last_name,
    talent,
    total_jaffle_revenue,
    total_performances,
    total_jaffle_revenue / total_performances AS avg_jaffle_revenue_per_performance
  FROM aggregate_1
), order_1 AS (
  SELECT
    *
  FROM formula_1
  ORDER BY
    avg_jaffle_revenue_per_performance DESC
), limit_1 AS (
  SELECT
    *
  FROM order_1
  LIMIT 3
)
SELECT
  *
FROM limit_1