SELECT ss.ss_item_sk, i.i_product_name
FROM store_sales ss
JOIN item i ON ss.ss_item_sk = i.i_item_sk
WHERE i.i_category = 'Electronics';
