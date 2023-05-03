gcloud sql connect retail-ecommerce --user=root

LOAD DATA LOCAL INFILE "orders.csv" INTO TABLE ecommerce_db.orders FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE "order_items.csv" INTO TABLE ecommerce_db.order_items FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;