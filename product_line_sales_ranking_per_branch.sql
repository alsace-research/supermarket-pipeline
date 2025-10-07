.headers on
.mode column

SELECT 
    dl.branch AS branch,
    dp.product_line,
    SUM(fs.total) AS total_sales,
    RANK() OVER (PARTITION BY dl.branch ORDER BY SUM(fs.total) DESC) AS sales_rank
FROM 
    fact_sales fs
JOIN 
    dim_location dl ON fs.location_id = dl.location_id
JOIN 
    dim_product dp ON fs.product_id = dp.product_id
GROUP BY 
    dl.branch, dp.product_line
ORDER BY 
    dl.branch, sales_rank;