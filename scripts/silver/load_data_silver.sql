-- ======================================================
-- Load Silver Layer from Bronze (MySQL 8+)
-- ======================================================

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
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'Unknown'
    END,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'Unknown'
    END,
    NULLIF(cst_create_date, '0000-00-00')
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL AND cst_id != 0;
*/

-- --------------------------------------
-- CRM Product Info
-- --------------------------------------
INSERT INTO silver.crm_prd_info (
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
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7) AS prd_key,
    prd_nm,
    IFNULL(prd_cost,0),
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'Unknown'
    END AS prd_line,
    -- cast to char first, then NULLIF
    NULLIF(CAST(prd_start_dt AS CHAR), '0000-00-00 00:00:00') AS prd_start_dt,
    NULLIF(
        CAST(DATE_SUB(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        ) AS CHAR),
        '0000-00-00 00:00:00'
    ) AS prd_end_dt
FROM bronze.crm_prd_info;

-- --------------------------------------
-- CRM Sales Details
-- --------------------------------------
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
)SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN CAST(sls_order_dt AS CHAR) = '0000-00-00' THEN NULL ELSE sls_order_dt END AS sls_order_dt,
    CASE WHEN CAST(sls_ship_dt AS CHAR) = '0000-00-00' THEN NULL ELSE sls_ship_dt END AS sls_ship_dt,
    CASE WHEN CAST(sls_due_dt AS CHAR) = '0000-00-00' THEN NULL ELSE sls_due_dt END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != sls_quantity * ABS(sls_price) 
        THEN sls_quantity * ABS(sls_price) 
        ELSE sls_sales 
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL 
          OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0) 
        ELSE sls_price 
    END AS sls_price
FROM bronze.crm_sales_details;



-- --------------------------------------
-- ERP Customer AZ12
-- --------------------------------------
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid)) ELSE cid END,
    NULLIF(bdate, '0000-00-00'),
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        ELSE 'Unknown'
    END
FROM bronze.erp_cust_az12;

-- --------------------------------------
-- ERP Location A101
-- --------------------------------------
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    REPLACE(cid,'-',''),
    CASE
        WHEN TRIM(cntry)='DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
        WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'Unknown'
        ELSE TRIM(cntry)
    END
FROM bronze.erp_loc_a101;

-- --------------------------------------
-- ERP PX Category G1V2
-- --------------------------------------
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
