--=======================================================
-- creating the VIEW for the CUSTOMERS Dimension 
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
-- creating the VIEW for the PRODUCTS Dimension 
--========================================================
