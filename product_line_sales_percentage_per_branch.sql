.headers on
.mode column

SELECT 
    dl.branch AS branch,
    dp.product_line,
    SUM(fs.total) AS total_sales,
    SUM(SUM(fs.total)) OVER (PARTITION BY dl.branch) AS branch_total,
    (SUM(fs.total) / SUM(SUM(fs.total)) OVER (PARTITION BY dl.branch)) * 100 AS sales_percentage
FROM 
    fact_sales fs
JOIN 
    dim_location dl ON fs.location_id = dl.location_id
JOIN 
    dim_product dp ON fs.product_id = dp.product_id
GROUP BY 
    dl.branch, dp.product_line
ORDER BY 
    dl.branch, total_sales DESC;