CREATE TABLE ecommerce.orders (
	order_id  int NOT NULL PRIMARY KEY, 
	user_id int NOT NULL, 
	status VARCHAR(10) NOT NULL, 
	gender VARCHAR(1) NOT NULL, 
	created_at timestamp NOT NULL, 
	returned_at timestamp default NULL, 
	shipped_at timestamp default NULL, 
	delivered_at timestamp default NULL, 
	num_items int NOT NULL
);

CREATE TABLE ecommerce.order_items (
    id  int NOT NULL PRIMARY KEY, 
	order_id  int NOT NULL , 
	user_id integer NOT NULL, 
	product_id int NOT NULL, 
	inventory_item_id int NOT NULL, 
	status varchar(10) NOT NULL ,
	created_at timestamp NOT NULL, 
	shipped_at timestamp default NULL, 
	delivered_at timestamp default NULL,
    returned_at timestamp default NULL,
	sales_price decimal not null,
	FOREIGN KEY(order_id) REFERENCES ecommerce.orders(order_id)
);



