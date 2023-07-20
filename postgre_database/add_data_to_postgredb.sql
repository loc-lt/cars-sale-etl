-- inventory table 
COPY Public."inventory"(quantity) FROM '/home/loclt/Desktop/car_sales_etl_project/data/inventory.csv' WITH (FORMAT CSV, HEADER);

-- products table
COPY Public."products"(Year, Make, Model, Category, inventory_id, created_at) FROM '/home/loclt/Desktop/car_sales_etl_project/data/product.csv' WITH (FORMAT CSV, HEADER);

-- orders table
COPY Public."orders"(product_id, quantity, created_at) FROM '/home/loclt/Desktop/car_sales_etl_project/data/order.csv' WITH (FORMAT CSV, HEADER);

-- users table
COPY Public."users"(username, firstname, lastename, email) FROM '/home/loclt/Desktop/car_sales_etl_project/data/users.csv' WITH (FORMAT CSV, HEADER);

-- user_details table
COPY Public."user_details"(user_id, address, city, postcode, country) FROM '/home/loclt/Desktop/car_sales_etl_project/data/user_detail.csv' WITH (FORMAT CSV, HEADER);

-- order_detail table
COPY Public."order_detail"(order_id, user_id, total, payment) FROM '/home/loclt/Desktop/car_sales_etl_project/data/order_detail.csv' WITH (FORMAT CSV, HEADER);
