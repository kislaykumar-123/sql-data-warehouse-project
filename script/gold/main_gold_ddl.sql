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





