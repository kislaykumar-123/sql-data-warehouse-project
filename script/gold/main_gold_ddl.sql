/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

--=======================================================
-- creating the VIEW for the Customers Dimension 
--========================================================
IF OBJECT_ID ('gold.dim_customers' , 'V') IS NOT NULL 
	DROP VIEW  gold.dim_customers ;
GO
CREATE VIEW gold.dim_customers AS (
SELECT 
    ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key ,
	ci.cst_id AS customer_id, 
	ci.cst_key AS customer_number, 
	ci.cst_firstname AS first_name, 
	ci.cst_lastname AS last_name, 
	la.cntry AS country ,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr !='n/a' THEN ci.cst_gndr -- CRM is the Master for gender Info
		 ELSE COALESCE(ca.gen , 'n/a')
	END as gender ,
	ca.bdate AS birth_date ,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON        ci.cst_key = la.cid )
	
--=======================================================
-- creating the VIEW for the Products Dimension 
--========================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS (
SELECT 
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_date , pn.prd_key) AS product_key , 
	pn.prd_id AS product_id, 
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id, 
	pc.cat AS category, 
	pc.subcat AS sub_category,
	pc.maintance AS maintenance,
	pn.prd_cost AS product_cost, 
	pn.prd_line AS product_line,
	pn.prd_start_date AS start_date 
--pn.prd_end_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_date IS NULL -- IF we want current only , not intrested in history -- Filter out all historical data
)

--=======================================================
-- creating the VIEW for the Sales Facts 
--========================================================
	
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





