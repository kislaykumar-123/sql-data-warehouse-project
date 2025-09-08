/*
-- Use the dimensions surrogate keys instead of ID's to easily connect facts with dimensions (fast)
-- Data look up : join krenge ham do table ko in order to get 1 information 
-- Listen gold layer mai hota hai surrogate key , thus to join we have to use gold layer not the silver 
-- Note : sls_prd_key is used to connect the silver.crm_sales_details to product table , so one time we just connect them and acquire the surrogate key so agle bar se surrogate key se hi find kr lenge
-- Note : Similarly hame left join krke customers ka bi use kr lia and remove the cust_id , instead hamne use kr lia surrogate key that we own genereate from system isse ham source ke dependency ko hata paye , csustomer _key
-- After creating it in a view and then Qunlity Check of Gold Table is a must 
*/
-- Create Fact Sales (Not Dimension This Time)
IF OBJECT_ID ('gold.fact_sales' , 'V') IS NOT NULL 
   DROP VIEW gold.fact_sales ;
GO 
CREATE VIEW gold.fact_sales AS (
SELECT 
sd.sls_ord_num AS order_number, 
--sd.sls_prd_key ,
pr.product_key ,
--sd.sls_cust_id , 
cu.customer_key ,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date, 
sd.sls_dur_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity, 
sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr 
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id
)
