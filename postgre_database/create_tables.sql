-----------------------------------------------------
--          Create Car Sales Database              --
-----------------------------------------------------

/*****************
1. inventory table
+ id - id của hàng tồn kho
+ quantity - số lượng
*****************/
create table inventory (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	quantity INT NOT NULL
);

/*****************
2. products table
+ id - id của sảm phẩm (product)
+ Year - năm sản xuất => năm sản xuất sản phẩm xe này
+ Make - hãng xe => hãng xe đã mua 
+ Model - mô hình xe => mô hình của xe hơi
+ Category - loại xe 
+ inventory_id - mã hàng tồn kho của sản phẩm
+ created_at - ngày tạo
NOTE: hàng tồn kho sẽ có nhiều sản phẩm hay sản phẩm là con của hàng tồn kho tức
là inventory_id references inventory(id)
*****************/
create table products (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	Year INT NOT NULL,
	Make VARCHAR(100) NOT NULL,
	Model VARCHAR(150) NOT NULL,
	Category VARCHAR(100) NOT NULL,
	created_at DATE NOT NULL,
	inventory_id BIGSERIAL REFERENCES inventory (id)
);

/*****************
3. orders table
+ id - id của đơn
+ product_id - id của sản phẩm xe hơi
+ quantity - số lượng mua
+ created_at - ngày tạo
*****************/
create table orders(
	id BIGSERIAL NOT NULL PRIMARY KEY,
	quantity INT NOT NULL,
	created_at DATE NOT NULL,
	product_id BIGSERIAL REFERENCES products (id)
);

/*****************
4. users table
+ id - id của người mua xe
+ username - họ tên đầy đủ của người mua xe
+ firstname - tên
+ lastname - họ
+ email - email của người đó
*****************/
create table users (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	username VARCHAR(50),
	firstname VARCHAR(50),
	lastename VARCHAR(50),
	email VARCHAR(100)
);

/*****************
5. user_details (thông tin chi tiết người mua xe)
+ id - id của thông tin chi tiết người mua xe
+ user_id - id của người mua xe => references to users(id)
+ address - địa chỉ của người mua xe
+ city - thành phố
+ postcode
+ country - quốc gia
*****************/
create table user_details (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	address VARCHAR(200) NOT NULL,
	city VARCHAR(100) NOT NULL,
	postcode INT NOT NULL,
	country VARCHAR(100) NOT NULL,
	user_id BIGSERIAL REFERENCES users (id)
);

/*****************
6. orders_detail (thông tin chi tiết về các đơn bán xe)
+ id - id của thông tin chi tiết đơn bán xe
+ order_id - id của đơn bán xe (references to order(id))
+ user_id - id của người đặt mua xe (references to user(id))
+ total - tổng số lượng xe đặt mua trong đơn hàng này
+ payment
*****************/
create table order_detail (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	total BIGSERIAL NOT NULL,
	payment VARCHAR(50) NOT NULL,
	order_id BIGSERIAL REFERENCES orders (id),
	user_id BIGSERIAL REFERENCES users (id)
);



