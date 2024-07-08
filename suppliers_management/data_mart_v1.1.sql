--создание схемы data_mart

DROP SCHEMA IF EXISTS data_mart;
CREATE SCHEMA data_mart AUTHORIZATION rodion_dekhtyarev;


--создание представлений

--список поставщиков

CREATE VIEW data_mart.suppliers_list AS
SELECT 
    id,
    "name",
    contact_person,
    phone,
    email,
    postcode,
    country,
    region,
    city,
    street_address
FROM dds.suppliers
WHERE is_active = TRUE;

--список товаров поставщиков

CREATE VIEW data_mart.supplier_products AS
SELECT 
    p.id,
    p.product_id,
    p.supplier_id,
    s."name" AS supplier_name,
    p."name" AS product_name,
    p.price,
    p.quantity,
    p.last_update
FROM dds.products p
JOIN dds.suppliers s ON p.supplier_id = s.id
WHERE s.is_active = TRUE;


--создание функций для data mart слоя

SELECT max(id) FROM dds.products; 

DROP SEQUENCE IF EXISTS products_id_seq CASCADE;
CREATE SEQUENCE IF NOT EXISTS products_id_seq
	START WITH 501;

--добавление нового товара
CREATE OR REPLACE FUNCTION data_mart.add_product(
    supplier_id int,
    product_name varchar,
    price float8,
    quantity int
) RETURNS void AS $$
BEGIN
    INSERT INTO dds.products (id, supplier_id, "name", price, quantity, last_update)
    VALUES (nextval('products_id_seq'), supplier_id, product_name, price, quantity, now());
END;
$$ LANGUAGE plpgsql;


SELECT data_mart.add_product(1, 'New Product', 9.99, 1);
SELECT data_mart.add_product(3, 'Product', 9.99, 100);
SELECT * FROM dds.products;

--изменение существующего товара

DROP FUNCTION IF EXISTS data_mart.update_product(int, varchar, float8, int);
CREATE OR REPLACE FUNCTION data_mart.update_product(
    product_id int,
    new_product_name varchar,
    new_price float8,
    new_quantity int
) RETURNS void AS $$
BEGIN
    UPDATE dds.products
    SET "name" = new_product_name,
        price = new_price,
        quantity = new_quantity,
        last_update = now()
    WHERE id = product_id;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM dds.products WHERE id = 1;
SELECT data_mart.update_product(1, 'Updated Product', 99.99, 99);
SELECT * FROM dds.products WHERE id = 1;

SELECT * FROM dds.products WHERE id = 3;
SELECT data_mart.update_product(3, 'Updated Product', 100.99, 100);
SELECT * FROM dds.products WHERE id = 3;

--удаление товара

CREATE OR REPLACE FUNCTION data_mart.delete_product(
    product_id int
) RETURNS void AS $$
BEGIN
    DELETE FROM dds.products
    WHERE id = product_id;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM dds.products WHERE id = 2;
SELECT data_mart.delete_product(2);
SELECT * FROM dds.products WHERE id = 2;

