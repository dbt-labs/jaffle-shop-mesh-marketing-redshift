WITH cirque_du_jaffle_show_cast_lists AS (
  SELECT
    show_id,
    show_name,
    performer_id,
    first_name,
    last_name,
    talent
  FROM {{ ref('cirque_du_jaffle_show_cast_lists') }}
), cirque_du_jaffle_shows AS (
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
), join_1 AS (
  SELECT
    cirque_du_jaffle_shows.show_id,
    cirque_du_jaffle_shows.show_name,
    cirque_du_jaffle_shows.show_description,
    cirque_du_jaffle_shows.location_id,
    cirque_du_jaffle_shows.total_concession_revenue,
    cirque_du_jaffle_shows.total_jaffle_revenue,
    cirque_du_jaffle_shows.first_performed_date,
    cirque_du_jaffle_shows.last_performed_date,
    cirque_du_jaffle_shows.total_performances,
    cirque_du_jaffle_shows.total_ticket_revenue,
    cirque_du_jaffle_show_cast_lists.show_id AS show_id_1,
    cirque_du_jaffle_show_cast_lists.show_name AS show_name_1,
    cirque_du_jaffle_show_cast_lists.performer_id,
    cirque_du_jaffle_show_cast_lists.first_name,
    cirque_du_jaffle_show_cast_lists.last_name,
    cirque_du_jaffle_show_cast_lists.talent
  FROM cirque_du_jaffle_shows
  JOIN cirque_du_jaffle_show_cast_lists
    ON cirque_du_jaffle_shows.show_id = cirque_du_jaffle_show_cast_lists.show_id
), aggregate_1 AS (
  SELECT
    first_name AS first_name,
    last_name AS last_name,
    talent AS talent,
    COUNT(performer_id) AS count_performer_id,
    SUM(total_jaffle_revenue) AS sum_total_jaffle_revenue
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
    count_performer_id,
    sum_total_jaffle_revenue,
    sum_total_jaffle_revenue / count_performer_id AS avg_jaffles_per_show
  FROM aggregate_1
)
SELECT
  *
FROM formula_1