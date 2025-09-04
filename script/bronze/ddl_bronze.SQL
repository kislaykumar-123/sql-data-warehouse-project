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




--- stored procedure hai ye iske niche ka ditto copy krke dekh lena

-- Create The Stored Procedures
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	PRINT '=================================';
	PRINT 'Loading the Bronze Layer';
	PRINT '=================================';


	-- Writting Scripts in Order to Load the data whole at one go from csv file , txt file directly to Data base
	PRINT '---------------------------------------------------';
	PRINT 'Loading CRM tables';
	PRINT '---------------------------------------------------';

	PRINT 'Truncating the table crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info ;-- This is like refreshing each time we access the table/full load
    PRINT 'Loading the table crm_cust_info';
	BULK INSERT bronze.crm_cust_info 
	FROM 'C:\Users\kisla\OneDrive - iitjammu.ac.in\Desktop\data_wareHouse_dataDet\datasets\source_crm\cust_info.csv'
	WITH (
	-- Here you have to write basically how would you handle our file
		 FIRSTROW = 2, 
		 FIELDTERMINATOR =',',
		 TABLOCK
	);
	PRINT 'Truncating DATA FROM CRM Source to Table -> bronze.crm_prd_info'; 
	TRUNCATE TABLE bronze.crm_prd_info ;
	PRINT 'LOADING DATA FROM CRM Source to Table -> bronze.crm_prd_info'; 
	BULK INSERT bronze.crm_prd_info 
	FROM 'C:\Users\kisla\OneDrive - iitjammu.ac.in\Desktop\data_wareHouse_dataDet\datasets\source_crm\prd_info.csv'
	WITH (
		 FIRSTROW = 2, 
		 FIELDTERMINATOR =',',
		 TABLOCK
	);

	PRINT 'Truncating Data Set FROM crm source for Table -> bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details
	PRINT 'Loading Data Set FROM crm source for Table -> bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\kisla\OneDrive - iitjammu.ac.in\Desktop\data_wareHouse_dataDet\datasets\source_crm\sales_details.csv'
	WITH (
		 FIRSTROW = 2, 
		 FIELDTERMINATOR =',',
		 TABLOCK
	);

	PRINT '---------------------------------------------------';
	PRINT 'Loading erp tables';
	PRINT '---------------------------------------------------';

	PRINT 'Truncate the bronze.erp_cust_az12 TABLE DATA SET';
	TRUNCATE TABLE bronze.erp_cust_az12;
	PRINT 'Loading bronze.erp_cust_az12 TABLE DATA SET';
	BULK INSERT bronze.erp_cust_az12 
	FROM 'C:\Users\kisla\OneDrive - iitjammu.ac.in\Desktop\data_wareHouse_dataDet\datasets\source_erp\CUST_AZ12.csv'
	WITH (
	-- Here you have to write basically how would you handle our file
		 FIRSTROW = 2, 
		 FIELDTERMINATOR =',',
		 TABLOCK
	);

    PRINT 'Truncating bronze.erp_px_cat_g1v2 DATA SET';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	PRINT 'Loading bronze.erp_px_cat_g1v2 DATA SET';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\kisla\OneDrive - iitjammu.ac.in\Desktop\data_wareHouse_dataDet\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
	-- Here you have to write basically how would you handle our file
		 FIRSTROW = 2, 
		 FIELDTERMINATOR =',',
		 TABLOCK
	);

    PRINT 'Truncating ERP bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	PRINT 'Loading ERP bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\kisla\OneDrive - iitjammu.ac.in\Desktop\data_wareHouse_dataDet\datasets\source_erp\LOC_A101.csv'
	WITH (
	-- Here you have to write basically how would you handle our file
		 FIRSTROW = 2, 
		 FIELDTERMINATOR =',',
		 TABLOCK
	);

END 


