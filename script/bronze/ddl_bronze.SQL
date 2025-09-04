/*
========================================================================================
DDL Scripts : Create Bronze Tables
========================================================================================
Script Purpose : 
       This Script creates Tables in the 'BRONZE' Schema , dropping exisiting tables 
       if the already Exisits.
       Run this Script to re-define the DDL Structure of 'Bronze' Tables
========================================================================================
*/


-- Create Sql DDL Script for ALL CSV files , IN our Bronze Layer : TOTAL 6 tables banega 

CREATE TABLE bronze.crm_prd_info(
	prd_id INT ,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT, 
	prd_line NVARCHAR(50), 
	prd_start_date DATETIME, 
	prd_end_date DATETIME
);

-- SALES DETAILS WALE CSV FILE KE LIYTE CREATE A DDL COMMAND 
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50) ,
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT, 
	sls_ship_dt INT, 
	sls_dur_dt INT, 
	sls_sales INT , 
	sls_quantity INT , 
	sls_price INT 
);

-- Create table for erp source , loc_a101
CREATE TABLE bronze.erp_loc_a101(
	cid   NVARCHAR(50), 
	cntry NVARCHAR(50)
);

-- Create table for erp source , cust_A2Z
CREATE TABLE bronze.erp_cust_az12(
	cid   NVARCHAR(50) , 
	bdate DATE,
	gen   NVARCHAR(50) 
);

-- Create table for erp source , px_cat_giv2
CREATE TABLE bronze.erp_px_cat_g1v2(
	id           NVARCHAR(50), 
	cat          NVARCHAR(50),
	subcat       NVARCHAR(50),
	maintance    NVARCHAR(50) 
);




