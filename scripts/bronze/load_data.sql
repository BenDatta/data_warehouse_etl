-- Stored procedure not allowed for files in local folder. 
-- So feel free to keep files on your secure folder and use the stored procedure with chnage in path
-- I logged the load scripts into a table to track failures 
DELIMITER $$

CREATE PROCEDURE load_data()
BEGIN
    SET @start_time = NOW();

-- CRM Tables
    INSERT INTO bronze.load_log (table_name, status, start_time) VALUES ('crm_cust_info', 'IN_PROGRESS', NOW());
    TRUNCATE TABLE bronze.crm_cust_info;
    LOAD DATA LOCAL INFILE '/Users/benny/Documents/Data Warehousing/datasets/source_crm/cust_info.csv'
    INTO TABLE bronze.crm_cust_info
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date);
    INSERT INTO bronze.load_log (table_name, status, rows_loaded, end_time)
    SELECT 'crm_cust_info', 'SUCCESS', ROW_COUNT(), NOW();

    INSERT INTO bronze.load_log (table_name, status, start_time) VALUES ('crm_prd_info', 'IN_PROGRESS', NOW());
    TRUNCATE TABLE bronze.crm_prd_info;
    LOAD DATA LOCAL INFILE '/Users/benny/Documents/Data Warehousing/datasets/source_crm/prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt);
    INSERT INTO bronze.load_log (table_name, status, rows_loaded, end_time)
    SELECT 'crm_prd_info', 'SUCCESS', ROW_COUNT(), NOW();

    INSERT INTO bronze.load_log (table_name, status, start_time) VALUES ('crm_sales_details', 'IN_PROGRESS', NOW());
    TRUNCATE TABLE bronze.crm_sales_details;
    LOAD DATA LOCAL INFILE '/Users/benny/Documents/Data Warehousing/datasets/source_crm/sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price);
    INSERT INTO bronze.load_log (table_name, status, rows_loaded, end_time)
    SELECT 'crm_sales_details', 'SUCCESS', ROW_COUNT(), NOW();

    -- ERP Tables
    INSERT INTO bronze.load_log (table_name, status, start_time) VALUES ('erp_loc_a101', 'IN_PROGRESS', NOW());
    TRUNCATE TABLE bronze.erp_loc_a101;
    LOAD DATA LOCAL INFILE '/Users/benny/Documents/Data Warehousing/datasets/source_erp/loc_a101.csv'
    INTO TABLE bronze.erp_loc_a101
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (cid, cntry);
    INSERT INTO bronze.load_log (table_name, status, rows_loaded, end_time)
    SELECT 'erp_loc_a101', 'SUCCESS', ROW_COUNT(), NOW();

    INSERT INTO bronze.load_log (table_name, status, start_time) VALUES ('erp_cust_az12', 'IN_PROGRESS', NOW());
    TRUNCATE TABLE bronze.erp_cust_az12;
    LOAD DATA LOCAL INFILE '/Users/benny/Documents/Data Warehousing/datasets/source_erp/cust_az12.csv'
    INTO TABLE bronze.erp_cust_az12
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (cid, bdate, gen);
    INSERT INTO bronze.load_log (table_name, status, rows_loaded, end_time)
    SELECT 'erp_cust_az12', 'SUCCESS', ROW_COUNT(), NOW();

    INSERT INTO bronze.load_log (table_name, status, start_time) VALUES ('erp_px_cat_g1v2', 'IN_PROGRESS', NOW());
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    LOAD DATA LOCAL INFILE '/Users/benny/Documents/Data Warehousing/datasets/source_erp/px_cat_g1v2.csv'
    INTO TABLE bronze.erp_px_cat_g1v2
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (id, cat, subcat, maintenance);
    INSERT INTO bronze.load_log (table_name, status, rows_loaded, end_time)
    SELECT 'erp_px_cat_g1v2', 'SUCCESS', ROW_COUNT(), NOW();

    SET @end_time = NOW();
    SELECT CONCAT('Bronze tables loaded. Total duration: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS info;

    -- View load status
    SELECT * FROM bronze.load_log ORDER BY id DESC;

END 
DELIMITER $$