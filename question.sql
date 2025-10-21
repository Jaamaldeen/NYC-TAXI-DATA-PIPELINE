SELECT trip_date, total_revenue
FROM gold.daily_summary
ORDER BY trip_date;

SELECT vendor_name, total_revenue, avg_fare
FROM gold.vendor_summary
ORDER BY total_revenue DESC
LIMIT 5;

SELECT payment_description, avg_tip_percent
FROM gold.payment_summary
ORDER BY avg_tip_percent DESC;



SELECT month, total_trips, total_revenue
FROM gold.monthly_summary
ORDER BY month;

SELECT PULocationID, pickups, revenue_from_pickups
FROM gold.zone_summary
ORDER BY pickups DESC
LIMIT 10;
 
