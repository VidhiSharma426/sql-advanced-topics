alter table `coffee shop sales_n` rename to coffee_sales;
select * from coffee_sales;
-- Rank transactions by unit price within each product category.
select product_category, dense_rank() over(partition by product_category order by unit_price) as rnk from coffee_sales;
-- Determine the rank of each product type based on the total quantity sold across all transactions.
select sum(transaction_qty) as qty,product_type , dense_rank() over ( order by sum(transaction_qty) ) as rnk from coffee_sales 
group by product_type;
-- Assign a rank to each transaction based on the transaction date and time, within each store location.
select transaction_date,transaction_time,store_location, dense_rank() over(partition by store_location ORDER BY transaction_date, transaction_time ) as rnk from coffee_sales;
-- Find the dense rank of each product detail based on the transaction quantity, within each product category.
select product_detail,product_category,sum(transaction_qty) as qty,dense_rank() over (partition by product_category order by sum(transaction_qty) desc) as rnk from coffee_sales group by product_category,product_detail ;
-- List the row number for each transaction based on the transaction date and time, within each store location.
select transaction_date,transaction_time,store_location, row_number() over(partition by store_location ORDER BY transaction_date, transaction_time ) as rnk from coffee_sales;
-- Calculate the dense rank of each store based on the total unit price of products sold, within each product type.
WITH SalesSummary AS (
    SELECT 
        store_location,
        product_type,
        SUM(unit_price) AS total
    FROM coffee_sales
    GROUP BY store_location, product_type
)
SELECT 
    store_location,
    product_type,
    total,
    DENSE_RANK() OVER(PARTITION BY store_location ORDER BY total DESC) AS rnk
FROM SalesSummary;
-- Determine the rank of each product ID based on the average unit price within each product category.
SELECT 
    product_id,
    ROUND(AVG(unit_price), 2) AS avgg,
    product_category,
    DENSE_RANK() OVER(PARTITION BY product_category ORDER BY ROUND(AVG(unit_price), 2)) AS rnk
FROM coffee_sales
GROUP BY product_category, product_id;

select product_id,round(avg(unit_price),2) as avgg,product_category , dense_rank() over(partition by product_category order by avg(unit_price)) as rnk from coffee_sales group by product_category,product_id; 
-- Assign a row number to each transaction based on the transaction quantity, within each store location and product category.
select transaction_qty ,store_location,product_category , row_number() over( partition by store_location,product_category order by transaction_qty ) as rown from coffee_sales ;
-- Find the rank of each transaction based on the unit price and transaction time, within each store location.
SELECT 
    transaction_date,
    transaction_time,
    unit_price,
    store_location,
    RANK() OVER (
        PARTITION BY store_location 
        ORDER BY unit_price DESC, transaction_time
    ) AS rnk
FROM coffee_sales;

-- Determine the dense rank of each product type based on the total transaction quantity, within each product category.

select  dense_rank() over(partition by product_category order by sm desc) as rnk,sm,product_type,product_category from(
select sum(transaction_qty) as sm, product_type ,product_category from coffee_sales group by product_type,product_category)a;

select * from coffee_sales;

-- Rank each product by unit price within each store location and product category.
select unit_price,store_location , product_category , rank() over (partition by store_location , product_category order by unit_price) as rnk from coffee_sales;
-- Assign a row number to each transaction based on transaction time, within each product category.
select product_category,transaction_time ,`ï»¿transaction_id`, row_number() over(partition by product_category) as row_num from coffee_sales;
-- Find the dense rank of each product based on total revenue (unit price multiplied by transaction quantity) within each store location.
select store_location, product_id,sum(unit_price*transaction_qty) as total_rev , dense_rank() over(partition by store_location order by sum(unit_price*transaction_qty)) as rnk from coffee_sales group by product_id,store_location;
-- Rank transactions by transaction quantity within each product type and store location.
select `ï»¿transaction_id` ,transaction_qty , rank() over(partition by product_type,store_location) from coffee_sales;
-- Determine the row number of each transaction based on transaction date, within each product detail.
select `ï»¿transaction_id`,transaction_date,transaction_time ,product_detail,rank() over (partition by product_detail ORDER BY transaction_date) as rnk from coffee_sales;
-- Calculate the rank of each store location based on the total quantity sold, within each product category.
select store_location ,product_category,sum(transaction_qty),rank() over(partition by product_category order by sum(transaction_qty)) as rnk from coffee_sales group by store_location,product_category;
-- Find the dense rank of each product based on the average unit price within each product category.
select product_category,product_id,round(avg(unit_price),2),dense_rank() over(partition by product_category   ORDER BY AVG(unit_price) DESC) as rnk from coffee_sales group by product_id,product_category;
-- Assign a row number to each product type based on total sales amount, within each product detail.
select product_detail,product_type ,row_number() over(partition by product_detail order by sum(unit_price*transaction_qty)desc),sum(unit_price*transaction_qty) as total_rev from coffee_sales group by product_detail,product_type;
     -- Find the dense rank of each product detail based on the total number of transactions, within each product category.
     select sum(no_of_tct) as t ,product_category,product_detail ,dense_rank() over (partition by product_category order by sum(no_of_tct) desc ) as rnk from(
     select count(`ï»¿transaction_id`) as no_of_tct , product_detail ,product_category from coffee_sales group by  product_detail ,product_category )a
     group by  product_detail ,product_category ;
  