/*
Gold1 : Creating The Customer Dimesion 

-- Tip : After Joining Tble , Check if any duplicates were introduced by the join logic 
-- After Doing So , We are getting nill rows means our join is good and we dont have any duplicates

--After doing all the stuff and handle the gender thing , we will give the firendly , Meaning ful names to it and store it in view
-- After creating this query put this in view and after that check tha data quilty

*/



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



