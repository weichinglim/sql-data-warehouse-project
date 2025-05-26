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
    SQL Server: EXEC Silver.load_silver; 
    PostgreSQL:CALL silver.load_silver();
===============================================================================
*/

-- Transformation & Insert Data
-- TRUNCATE before INSERT to prevent duplicates

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
	error_text TEXT;
	error_code TEXT;
BEGIN
	batch_start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '====================================================';
	RAISE NOTICE 'Loading Silver Layer';
	RAISE NOTICE '====================================================';

	RAISE NOTICE '----------------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '----------------------------------------------------';

	-- Loading silver.crm_cust_info
	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	) 
	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname, -- Remove unwanted spaces
	TRIM(cst_lastname) AS cst_lastname,
	
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END AS cst_marital_status, -- Normalize marital status values to readable format
	
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END AS cst_gndr, -- Normalize gender values to readable format
	
	cst_create_date -- Make sure is actual date not string
	FROM (
		SELECT * ,
		ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL -- We want to keep data from latest date
	) t
	WHERE flag_last = 1; -- Select the most recent record per customer. To check: AND cst_id = 29466
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in silver.crm_cust_info: %', (SELECT COUNT(*) FROM silver.crm_cust_info);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	-- Loading silver.crm_prd_info
	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		prd_id,
		REPLACE( SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key FROM 7) AS prd_key,
		prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)- INTERVAL '1 day' AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in silver.crm_prd_info: %', (SELECT COUNT(*) FROM silver.crm_prd_info);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	-- Loading silver.crm_sales_details
	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key, 
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT
		sls_ord_num,
		sls_prd_key, 
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL 
			ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL 
			ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL 
			ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_quantity * sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;
	--For Checking: WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in silver.crm_sales_details: %', (SELECT COUNT(*) FROM silver.crm_sales_details);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	RAISE NOTICE '----------------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '----------------------------------------------------';

	-- Loading silver.erp_cust_az12
	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
		ELSE cid
	END AS cid,
	CASE WHEN bdate > CURRENT_DATE THEN NULL
		ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in silver.erp_cust_az12: %', (SELECT COUNT(*) FROM silver.erp_cust_az12);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	-- Loading silver.erp_loc_a101
	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)
	SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in silver.erp_loc_a101: %', (SELECT COUNT(*) FROM silver.erp_loc_a101);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	-- Loading silver.erp_px_cat_g1v2
	start_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	-- Data is of good quality
	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE 'Rows in silver.erp_px_cat_g1v2: %', (SELECT COUNT(*) FROM silver.erp_px_cat_g1v2);
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-------------------';

	batch_end_time := CLOCK_TIMESTAMP();
	RAISE NOTICE '====================================================';
	RAISE NOTICE 'Loading Silver Layer is Completed';
	RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
	RAISE NOTICE '====================================================';

EXCEPTION
	WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT, error_code = RETURNED_SQLSTATE;
		RAISE NOTICE '====================================================';
		RAISE NOTICE 'ERROR OCCURED DURING LOADING SILVER LAYER';
		RAISE NOTICE 'Error Message: %', error_text;
		RAISE NOTICE 'Error Code: %', error_code;
		RAISE NOTICE '====================================================';
END;
$$;
