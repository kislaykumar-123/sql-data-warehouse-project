
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/





CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	BEGIN TRY
	--=======================================
	-- Transformation for crm_cust_info table
	--=======================================
	PRINT '>> Truncating Data Into : silver.crm_cust_info ';
	TRUNCATE TABLE silver.crm_cust_info ;
	PRINT '>> Inserting Data Into : silver.crm_cust_info ';
	INSERT INTO silver.crm_cust_info (
			cst_id ,
			cst_key ,
			cst_firstname ,
			cst_lastname ,
			cst_marital_status ,
			cst_gndr ,
			cst_create_date)

	SELECT 
	t.cst_id, 
	t.cst_key , 
	TRIM(t.cst_firstname) AS cst_firstname , 
	TRIM(t.cst_lastname) AS cst_lastname ,
	CASE 
	WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
	WHEN UPPER(cst_material_status) = 'M' THEN 'Married'
	ELSE 'n/a'
	END cst_maretial_status , 

	CASE 
	WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
	WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
	ELSE 'n/a'
	END cst_gndr ,

	cst_create_date  
	FROM (
		SELECT 
		*, 
		DENSE_RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS final_flag
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL) t 
	WHERE t.final_flag = 1

	--=======================================
	-- Transformation for crm_prd_info table
	--=======================================
	PRINT '>> Truncating Data Into : silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into : silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
		prd_id ,
		cat_id ,
		prd_key,
		prd_nm,
		prd_cost, 
		prd_line,
		prd_start_date,
		prd_end_date
	)

	SELECT 
			prd_id ,
			REPLACE(SUBSTRING(prd_key , 1 , 5) , '-' , '_') as cat_id,--Extract category ID 
			SUBSTRING(prd_key , 7 , LEN(prd_key)) AS prd_key , -- Extract product key
			prd_nm ,
			COALESCE(prd_cost , 0) AS prd_cost , -- Handeling nulls over here
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Roads'
				WHEN UPPER(TRIM(prd_line)) = 's' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line , -- Data Normalisation (Map to freindly name , not avverbiations)  
			CAST(prd_start_date AS DATE) AS prd_start_date,
			CAST(LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date ASC)-1 AS DATE) AS prd_end_date -- Data enrichment , because end date< start date . no overlapping
	FROM bronze.crm_prd_info

	-- AFTER INSETING THE DATA , MAKE SURE KI AAP QUALITY CHECK KRO SILVER LAYER KA 

	--=======================================
	-- Transformation for silver.crm_sales_details table
	--=======================================
	PRINT '>> Truncating Data Into : silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details
	PRINT '>> Inserting Data Into : silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_dur_dt ,
		sls_sales , 
		sls_quantity , 
		sls_price
	)
	SELECT 
	sls_ord_num , 
	sls_prd_key , 
	sls_cust_id , 
	--sls_order_dt , 
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8  THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END sls_order_dt,
	--sls_ship_dt ,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8  THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END sls_ship_dt,
	--sls_dur_dt ,
	CASE WHEN sls_dur_dt = 0 OR LEN(sls_dur_dt) != 8  THEN NULL
		 ELSE CAST(CAST(sls_dur_dt AS VARCHAR) AS DATE)
	END sls_due_dt,
	--sls_sales ,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity*ABS(sls_price) 
			 THEN sls_quantity*ABS(sls_price) 
		 ELSE sls_sales
	END AS sls_sales ,

	sls_quantity , 

	--sls_price 
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity , 0)
		 ELSE sls_price 
	END AS sls_price
	FROM bronze.crm_sales_details

	--=======================================
	-- Transformation for silver.erp_cust_az12 table
	--=======================================
     PRINT '>> Truncating Table: silver.erp_cust_az12';
	 TRUNCATE TABLE silver.erp_cust_az12;
	 PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	 INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
	 SELECT
		CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
		ELSE cid
		END AS cid, 
	 CASE
	 WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
	 END AS bdate, -- Set future birthdates to NULL
	 CASE
	 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
	 END AS gen -- Normalize gender values and handle unknown cases
	 FROM bronze.erp_cust_az12;
	--=======================================
	-- Transformation for silver.erp_loc_a101 table
	--=======================================
	 PRINT '>> Truncating Data Into : silver.erp_loc_a101';
	 TRUNCATE TABLE silver.erp_loc_a101
	 PRINT '>> Inserting Data Into : silver.erp_loc_a101';
	 INSERT INTO silver.erp_loc_a101 (
	   cid , 
	   cntry
	 ) 
	 SELECT 
	 REPLACE(cid , '-' , '') AS cid ,
	 CASE WHEN TRIM(cntry) IN ('US' , 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' 
		 ELSE TRIM(cntry) 
	 END AS cntry
	 FROM bronze.erp_loc_a101

	--=======================================
	-- Transformation for silver.erp_px_cat_g1v21 table
	--=======================================
	PRINT '>> Truncating Data Into : silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT '>> Inserting Data Into : silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (
		id , 
		cat , 
		subcat,
		maintance
	)
	SELECT 
		id , 
		cat , 
		subcat,
		maintance
	FROM bronze.erp_px_cat_g1v2
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END

