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
*/

------------------------------------------------------------------------
-- SILVER LAYER: Transform and cleanse data
------------------------------------------------------------------------
SELECT '--- Loading Silver Layer ---' AS message;

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE  
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END,
    CASE  
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,
    cst_create_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
    prd_nm,
    NULLIF(prd_cost,'') AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
        ELSE 'n/a'
    END,
    CASE WHEN prd_start_dt REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN prd_start_dt ELSE NULL END,
    CASE WHEN prd_end_dt REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN prd_end_dt ELSE NULL END
FROM bronze.crm_prd_info;

TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details (
    sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt,
    sls_sales, sls_quantity, sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt REGEXP '^[0-9]{8}$' THEN STR_TO_DATE(sls_order_dt,'%Y%m%d') ELSE NULL END,
    CASE WHEN sls_ship_dt REGEXP '^[0-9]{8}$' THEN STR_TO_DATE(sls_ship_dt,'%Y%m%d') ELSE NULL END,
    CASE WHEN sls_due_dt REGEXP '^[0-9]{8}$' THEN STR_TO_DATE(sls_due_dt,'%Y%m%d') ELSE NULL END,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN
