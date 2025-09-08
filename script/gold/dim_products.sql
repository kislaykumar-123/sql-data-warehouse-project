/*
-- Make Sure ki jaise hi aap Join kro Check for duplicated , means Uniqueness test krna hoga 
-- AS here we dont have any duplicates after joining so it is benificial , 
-- SOrt the into logical ggroups  to improve readiblity 
-- Give frienldy names to the columns
-- After taht think that is it Dimension OR Fact ?
-- Dimension : The thing is dimension says the descriptive information about the OBJECt/Buisness Model
--  FACT : it tells about transaction details or events kind of stuff then it is a
-- Here we all about description about Buisness Objects PRODUCTS , (each row is describing the product)

-- After making the views check how the views check it is working on nor 
*/


-- Creating The Dimensions PRODUCTS
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS (
SELECT 
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_date , pn.prd_key) AS product_key , 
	pn.prd_id AS product_id, 
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id, 
	pc.cat AS category, 
	pc.subcat AS sub_category,
	pc.maintance AS maintenance,
	pn.prd_cost AS product_cost, 
	pn.prd_line AS product_line,
	pn.prd_start_date AS start_date 
--pn.prd_end_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_date IS NULL -- IF we want current only , not intrested in history -- Filter out all historical data
)
