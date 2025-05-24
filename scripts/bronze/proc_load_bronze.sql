/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - For SQL Server: Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
    - For PostgreSQL: Uses the 'COPY' command. IMPORTANT! Datasets must be in the same directory as pgAdmin to load.  

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    SQL Server: EXEC bronze.load_bronze;  
    PostgreSQL: CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
As $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
BEGIN
	batch_start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '====================================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '====================================================';

	RAISE NOTICE '----------------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '----------------------------------------------------';

	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	RAISE NOTICE '>> Copying Data Into: bronze.crm_cust_info';
	COPY bronze.crm_cust_info
		FROM '/Applications/PostgreSQL 17/datasets/source_crm/cust_info.csv'
			DELIMITER ','
			CSV HEADER;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in bronze.crm_cust_info: %', (SELECT COUNT(*) FROM bronze.crm_cust_info);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	RAISE NOTICE '>> Copying Data Into: bronze.crm_prd_info';
	COPY bronze.crm_prd_info
		FROM '/Applications/PostgreSQL 17/datasets/source_crm/prd_info.csv'
			DELIMITER ','
			CSV HEADER;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in bronze.crm_prd_info: %', (SELECT COUNT(*) FROM bronze.crm_prd_info);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	RAISE NOTICE '>> Copying Data Into: bronze.crm_sales_details';
	COPY bronze.crm_sales_details
		FROM '/Applications/PostgreSQL 17/datasets/source_crm/sales_details.csv'
			DELIMITER ','
			CSV HEADER;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in bronze.crm_sales_details: %', (SELECT COUNT(*) FROM bronze.crm_sales_details );
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	RAISE NOTICE '----------------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '----------------------------------------------------';

	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	RAISE NOTICE '>> Copying Data Into: bronze.erp_cust_az12';
	COPY bronze.erp_cust_az12
		FROM '/Applications/PostgreSQL 17/datasets/source_erp/CUST_AZ12.csv'
			DELIMITER ','
			CSV HEADER;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in bronze.erp_cust_az12: %', (SELECT COUNT(*) FROM bronze.erp_cust_az12);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	RAISE NOTICE '>> Copying Data Into: bronze.erp_loc_a101';
	COPY bronze.erp_loc_a101
		FROM '/Applications/PostgreSQL 17/datasets/source_erp/LOC_A101.csv'
			DELIMITER ','
			CSV HEADER;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in bronze.erp_loc_a101: %', (SELECT COUNT(*) FROM bronze.erp_loc_a101);	
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	RAISE NOTICE '>> Copying Data Into: bronze.erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2
		FROM '/Applications/PostgreSQL 17/datasets/source_erp/PX_CAT_G1V2.csv'
			DELIMITER ','
			CSV HEADER;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in bronze.erp_px_cat_g1v2: %', (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	batch_end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '====================================================';
	RAISE NOTICE 'Loading Bronze Layer is Completed';
	RAISE NOTICE 'Error Message: %', SQLERRM;
	RAISE NOTICE '====================================================';
EXCEPTION
	WHEN OTHERS THEN 
		RAISE NOTICE '====================================================';
		RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		RAISE NOTICE ' - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
		RAISE NOTICE '====================================================';
END;
$$;
