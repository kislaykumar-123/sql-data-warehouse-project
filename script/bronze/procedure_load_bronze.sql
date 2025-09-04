
-- Create The Stored Procedures
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=================================';
		PRINT 'Loading the Bronze Layer';
		PRINT '=================================';

		-- Writting Scripts in Order to Load the data whole at one go from csv file , txt file directly to Data base
		PRINT '---------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
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
		SET @end_TIME = GETDATE();
		PRINT 'Load table time is ' + CAST(DATEDIFF(MILLISECOND , @start_time , @end_time) AS NVARCHAR);
	    PRINT '-------------------';

		SET @start_time = GETDATE();
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
		SET @end_time = GETDATE();
		PRINT 'Load table time is ' + CAST(DATEDIFF(MILLISECOND , @start_time , @end_time) AS NVARCHAR);
		PRINT '-------------------';

		SET @start_time = GETDATE();
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
		SET @end_time = GETDATE();
		PRINT 'Load table time is ' + CAST(DATEDIFF(MILLISECOND , @start_time , @end_time) AS NVARCHAR);
	    PRINT '-------------------';

		PRINT '---------------------------------------------------';
		PRINT 'Loading erp tables';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
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
		SET @end_time = GETDATE();
		PRINT 'Load table time is ' + CAST(DATEDIFF(MILLISECOND , @start_time , @end_time) AS NVARCHAR);
		PRINT '-------------------';

		SET @start_time = GETDATE();
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
		SET @end_time = GETDATE();
		PRINT 'Load table time is ' + CAST(DATEDIFF(MILLISECOND , @start_time , @end_time) AS NVARCHAR);
		PRINT '-------------------';

     	SET @start_time = GETDATE();
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
		SET @end_time = GETDATE();
		PRINT 'Load table time is ' + CAST(DATEDIFF(MILLISECOND , @start_time , @end_time) AS NVARCHAR);
		PRINT '-------------------';
		SET @batch_end_time = GETDATE();
		PRINT '=============================================';
		PRINT 'The Loading BRONZE Layer is Completed';
	    PRINT 'TOTAL TIME DURATION FOR LOADING BRONZE LAYER : ' + CAST(DATEDIFF(MILLISECOND , @batch_start_time , @batch_end_time) AS NVARCHAR);
		PRINT '=============================================';

	END TRY
	BEGIN CATCH 
	PRINT '====================================================';
	PRINT 'ERROR WHILE LOADING THE BRONZE LAYER'; 
	PRINT 'ERROR MESSAGE IS ' + ERROR_MESSAGE();
	PRINT 'ERROR NUMBER IS ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR STATE IS' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '====================================================';
	END CATCH 

END 
