-- Describe the month without filtering or assuming an acceptable anomaly rate.
select count(*) as staged_rows,
       count(case when pickup_at is null or dropoff_at is null then 1 end) as null_timestamps,
       count(case when pickup_zone_id is null or dropoff_zone_id is null then 1 end) as null_zones,
       count(case when passenger_count is null then 1 end) as null_passengers,
       count(case when trip_distance is null then 1 end) as null_distances,
       count(case when duration_min <= 0 or duration_min > 180 then 1 end) as invalid_durations,
       count(case when pickup_zone_id not between 1 and 263 or dropoff_zone_id not between 1 and 263 then 1 end) as invalid_zones,
       count(case when trip_distance < 0 then 1 end) as negative_distances,
       count(case when passenger_count < 0 then 1 end) as negative_passengers
from staging.stg_trips where trip_month = '{month}';

-- Count excess copies of identical staged rows, rather than assuming a trip ID exists.
select coalesce(sum(copies - 1), 0) as duplicate_rows
from (
    select count(*) as copies
    from staging.stg_trips where trip_month = '{month}'
    group by vendor_id, pickup_at, dropoff_at, duration_min, pickup_hour, pickup_dow,
             pickup_zone_id, dropoff_zone_id, passenger_count, trip_distance, ratecode_id,
             payment_type, fare_amount, total_amount
) grouped;
