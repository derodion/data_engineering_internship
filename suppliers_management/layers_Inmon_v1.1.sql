--создание схемы staging

DROP SCHEMA IF EXISTS staging;
CREATE SCHEMA staging AUTHORIZATION rodion_dekhtyarev;


--создание таблиц для staging слоя

DROP TABLE IF EXISTS staging.products;
CREATE TABLE staging.products (
	id int NOT NULL,
	supplier_id int4 NOT NULL,
	"name" varchar(255) NOT NULL,
	price float8 NULL,
	quantity int4 NULL,
	last_update timestamp NOT NULL 
);


DROP TABLE IF EXISTS staging.suppliers;
CREATE TABLE staging.suppliers (
	id int NOT NULL,
	"name" varchar(255) NOT NULL,
	contact_person varchar(255) NULL,
	phone varchar(50) NULL,
	email varchar(255) NULL,
	postcode text NULL,
	country text NULL,
	region text NULL,
	city text NULL,
	street_address text NULL,
	last_update timestamp NOT NULL
);


DROP TABLE IF EXISTS staging.last_update;
CREATE TABLE staging.last_update (
				table_name varchar(50) NOT NULL,
				update_dt timestamp NOT NULL
				);
				

--создание процелур для staging слоя

CREATE OR REPLACE FUNCTION staging.get_last_update(table_name varchar) RETURNS timestamp
AS $$
	BEGIN 
		RETURN coalesce(
		(
		SELECT max(update_dt)
		FROM staging.last_update lu
		WHERE lu.table_name = get_last_update.table_name
		),
		'1900-01-01'::date
		);
	END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE staging.set_load_time(table_name varchar, current_update_dt timestamp DEFAULT now())
AS $$
	BEGIN
		INSERT INTO staging.last_update
		(
		table_name,
		update_dt
		)
		VALUES (
		table_name,
		current_update_dt
		);
	END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE staging.products_load(current_update_dt timestamp)
AS $$
	DECLARE last_update_dt timestamp;
	BEGIN
		last_update_dt = staging.get_last_update('staging.products');
		DELETE FROM staging.products;
		INSERT INTO staging.products (
							id, 
							supplier_id, 
							"name", 
							price, 
							quantity,
							last_update
							)
		SELECT id, 
			   supplier_id, 
			   "name", 
			   price, 
			   quantity,
			   last_update
		FROM src.products
	WHERE last_update > last_update_dt;
		CALL staging.set_load_time('staging.products', current_update_dt);
	END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE staging.suppliers_load(current_update_dt timestamp)
AS $$
	BEGIN 
		DELETE FROM staging.suppliers;
		INSERT INTO staging.suppliers (
							id, 
							"name", 
							contact_person, 
							phone, 
							email, 
							postcode, 
							country, 
							region, 
							city, 
							street_address,
							last_update
							)
		SELECT id, 
		   	   "name", 
		       contact_person, 
		       phone, 
		       email, 
		       postcode, 
			   country, 
			   region, 
			   city, 
			   street_address,
		       last_update
		FROM src.suppliers;
		CALL staging.set_load_time('staging.suppliers', current_update_dt);
	END;
$$ LANGUAGE plpgsql;


--создание схемы ods
DROP SCHEMA IF EXISTS ods;
CREATE SCHEMA ods AUTHORIZATION rodion_dekhtyarev;


--создание таблиц для ods слоя

DROP TABLE IF EXISTS ods.products;
CREATE TABLE ods.products (
	id int NOT NULL,
	supplier_id int4 NOT NULL,
	"name" varchar(255) NOT NULL,
	price float8 NULL,
	quantity int4 NULL,
	last_update timestamp NOT NULL 
);


DROP TABLE IF EXISTS ods.suppliers;
CREATE TABLE ods.suppliers (
	id int NOT NULL,
	"name" varchar(255) NOT NULL,
	contact_person varchar(255) NULL,
	phone varchar(50) NULL,
	email varchar(255) NULL,
	postcode text NULL,
	country text NULL,
	region text NULL,
	city text NULL,
	street_address text NULL,
	last_update timestamp NOT NULL
);
				
			
--создание процелур для ods слоя
			
CREATE OR REPLACE PROCEDURE ods.products_load()
AS $$
	BEGIN
		DELETE FROM ods.products odsp
		WHERE odsp.id IN (
				SELECT sp.id
				FROM staging.products sp);
		INSERT INTO ods.products (
							id, 
							supplier_id, 
							"name", 
							price, 
							quantity,
							last_update
							)
		SELECT id, 
			   supplier_id, 
			   "name", 
			   price, 
			   quantity,
			   last_update
		FROM staging.products;
	END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE ods.suppliers_load()
AS $$
	BEGIN 
		DELETE FROM ods.suppliers;
		INSERT INTO ods.suppliers (
							id, 
							"name", 
							contact_person, 
							phone, 
							email, 
							postcode, 
							country, 
							region, 
							city, 
							street_address,
							last_update
							)
		SELECT id, 
		   	   "name", 
		       contact_person, 
		       phone, 
		       email, 
		       postcode, 
			   country, 
			   region, 
			   city, 
			   street_address,
		       last_update
		FROM staging.suppliers;
	END;
$$ LANGUAGE plpgsql;


--DELETE FROM staging.last_update WHERE table_name = 'staging.products';


--создание схемы reference
DROP SCHEMA IF EXISTS reference;
CREATE SCHEMA reference AUTHORIZATION rodion_dekhtyarev;


--создание таблиц для reference слоя

DROP TABLE IF EXISTS reference.suppliers;
CREATE TABLE reference.suppliers (
								supplier_sk serial NOT NULL,
								supplier_nk int NOT NULL
								);

DROP TABLE IF EXISTS reference.products;
CREATE TABLE reference.products (
								product_sk serial NOT NULL,
								product_nk int NOT NULL
								);


--создание процелур для reference слоя
							
CREATE OR REPLACE PROCEDURE supplier_id_sync()
AS $$
	BEGIN
		INSERT INTO reference.suppliers (
								supplier_nk
								)
		SELECT s.id
		FROM ods.suppliers s
		LEFT JOIN reference.suppliers rs ON s.id = rs.supplier_nk
		WHERE rs.supplier_nk IS NULL
		ORDER BY s.id;
	END;
	$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE product_id_sync()
AS $$
	BEGIN
		INSERT INTO reference.products (
								product_nk
								)
		SELECT p.id
		FROM ods.products p
		LEFT JOIN reference.products rp ON p.id = rp.product_nk
		WHERE rp.product_nk IS NULL
		ORDER BY p.id;
	END;
	$$ LANGUAGE plpgsql;


--создание схемы integration

DROP SCHEMA IF EXISTS integration;
CREATE SCHEMA integration AUTHORIZATION rodion_dekhtyarev;


--создание таблиц для integration слоя

DROP TABLE IF EXISTS integration.products;
CREATE TABLE integration.products (
	id int NOT NULL,
	supplier_id int4 NOT NULL,
	"name" varchar(255) NOT NULL,
	price float8 NULL,
	quantity int4 NULL,
	last_update timestamp NOT NULL 
);


DROP TABLE IF EXISTS integration.suppliers;
CREATE TABLE integration.suppliers (
	id int NOT NULL,
	"name" varchar(255) NOT NULL,
	contact_person varchar(255) NULL,
	phone varchar(50) NULL,
	email varchar(255) NULL,
	postcode text NULL,
	country text NULL,
	region text NULL,
	city text NULL,
	street_address text NULL,
	last_update timestamp NOT NULL
);


--создание процедур для integration слоя

CREATE OR REPLACE PROCEDURE integration_suppliers_load()
AS $$
	BEGIN 
		DELETE FROM integration.suppliers;
		INSERT INTO integration.suppliers
					(
					id,
					"name",
					contact_person,
					phone,
					email,
					postcode,
					country,
					region,
					city,
					street_address,
					last_update
					)
		SELECT rs.supplier_sk,
			   "name",
			   contact_person,
			   phone,
			   email,
			   postcode,
			   country,
		       region,
			   city,
			   street_address,
			   last_update
		FROM ods.suppliers s 
		JOIN reference.suppliers rs ON s.id = rs.supplier_nk;
	END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE integration_products_load()
AS $$
	DECLARE last_update_dt timestamp;
	BEGIN
		--дата и время последней изменённой записи, загруженной в предыдущий раз
		last_update_dt = (
						SELECT COALESCE (max(p.last_update), '1900-01-01'::date)
						FROM integration.products p
						);
		
		--идентификаторы всех изменённых товаров с предыдущей загрузки 
		CREATE TEMPORARY TABLE updated_integration_products_id_list ON COMMIT DROP AS 
		SELECT p.id
		FROM ods.products p
		WHERE p.last_update > last_update_dt;
		DELETE FROM integration.products p
		WHERE p.id IN (
				SELECT id
				FROM updated_integration_products_id_list
				);
			
		--вставляем все созданные или изменённые товары
		INSERT INTO integration.products (
											id,
											supplier_id,
											"name",
											price,
											quantity,
											last_update
											)
		SELECT rp.product_sk,
			   rs.supplier_sk AS supplier_id,
			   "name",
			   price,
		       quantity,
			   last_update 
		FROM ods.products p
		JOIN reference.products rp ON p.id = rp.product_nk
		JOIN updated_integration_products_id_list upd ON upd.id = p.id
		JOIN reference.suppliers rs ON rs.supplier_nk = p.supplier_id; 
		
	END	
$$ LANGUAGE plpgsql;


--создание схемы dds

DROP SCHEMA IF EXISTS dds;
CREATE SCHEMA dds AUTHORIZATION rodion_dekhtyarev;


--создание таблиц для dds слоя

DROP TABLE IF EXISTS dds.products;
CREATE TABLE dds.products (
	id int NOT NULL,
	supplier_id int4 NOT NULL,
	"name" varchar(255) NOT NULL,
	price float8 NULL,
	quantity int4 NULL,
	last_update timestamp NOT NULL 
);


DROP TABLE IF EXISTS dds.suppliers;
CREATE TABLE dds.suppliers (
	id int NOT NULL,
	"name" varchar(255) NOT NULL,
	contact_person varchar(255) NULL,
	phone varchar(50) NULL,
	email varchar(255) NULL,
	postcode text NULL,
	country text NULL,
	region text NULL,
	city text NULL,
	street_address text NULL,
	date_effective_from timestamp NOT NULL,
	date_effective_to timestamp NOT NULL,
	is_active boolean NOT NULL,
	hash varchar(32)
);

 
--создание процедур для dds слоя

CREATE OR REPLACE PROCEDURE dds_suppliers_load()
AS $$
	BEGIN
		
		--список id новых поставщиков
		CREATE TEMPORARY TABLE supplier_new_id_list ON COMMIT DROP AS
		SELECT rs.supplier_sk AS id
		FROM reference.suppliers rs
		LEFT JOIN dds.suppliers s ON rs.supplier_sk = s.id
		WHERE s.id IS NULL;
	
		--вставляем новых поставщиков
		INSERT INTO dds.suppliers
						(
						id,
						"name",
						contact_person,
						phone,
						email,
						postcode,
						country,
						region,
						city,
						street_address,
						date_effective_from,
						date_effective_to,
						is_active,
						hash
						)
		SELECT s.id,
			   "name",
			   contact_person,
			   phone,
			   email,
			   postcode,
			   country,
			   region,
			   city,
			   street_address,
			   '1900-01-01'::date AS date_effective_from,
			   '9999-01-01'::date AS date_effective_to,
			   TRUE AS is_active,
			   md5(s::TEXT) AS hash
		FROM integration.suppliers s
		JOIN supplier_new_id_list ns ON s.id = ns.id; 
	
		--id удалённых поставщиков
		CREATE TEMPORARY TABLE supplier_deleted_id_list ON COMMIT DROP AS
		SELECT s.id
		FROM dds.suppliers s 
		LEFT JOIN integration.suppliers ints ON s.id = ints.id
		WHERE ints.id IS NULL;
		
		--помечаем удалённых поставщиков
		UPDATE dds.suppliers s
		SET is_active = FALSE,
			date_effective_to = now()
		FROM supplier_deleted_id_list sd
		WHERE sd.id = s.id AND s.is_active IS TRUE;
		
		--находим id изменённых поставщиков
		CREATE TEMPORARY TABLE supplier_update_id_list ON COMMIT DROP AS
		SELECT ints.id 
		FROM dds.suppliers s
		JOIN integration.suppliers ints ON s.id = ints.id
		WHERE s.is_active IS TRUE AND s.hash <> md5(ints::TEXT);
		
		--делаем неактульными предыдущие строки по изменённым поставщикам
		UPDATE dds.suppliers s
		SET is_active = FALSE,
			date_effective_to =ints.last_update 
		FROM integration.suppliers ints
		JOIN supplier_update_id_list ups ON ups.id = ints.id
		WHERE ints.id = s.id AND s.is_active = TRUE; 
	
		--добавляем новые строки по изменённым поставщикам
		INSERT INTO dds.suppliers
						(
						id,
						"name",
						contact_person,
						phone,
						email,
						postcode,
						country,
						region,
						city,
						street_address,
						date_effective_from,
						date_effective_to,
						is_active,
						hash
						)
		SELECT s.id,
			   "name",
			   contact_person,
			   phone,
			   email,
			   postcode,
			   country,
			   region,
			   city,
			   street_address,
			   last_update AS date_effective_from,
			   '9999-01-01'::date AS date_effective_to,
			   TRUE AS is_active,
			   md5(s::TEXT) AS hash
		FROM integration.suppliers s
		JOIN supplier_update_id_list ups ON s.id = ups.id; 
	END;
	
$$ LANGUAGE plpgsql;


--check

UPDATE integration.suppliers
SET name = concat(name, ' 10')
WHERE id = 10;

CALL dds_suppliers_load();

SELECT * FROM dds.suppliers WHERE id = 10;


CREATE OR REPLACE PROCEDURE dds_products_load()
AS $$
	DECLARE last_update_dt timestamp;
	BEGIN
		--дата и время последней изменённой записи, загруженной в предыдущий раз
		last_update_dt = (
						SELECT COALESCE (max(p.last_update), '1900-01-01'::date)
						FROM dds.products p
						);
		
		--идентификаторы всех изменённых товаров с предыдущей загрузки 
		CREATE TEMPORARY TABLE updated_dds_products_id_list ON COMMIT DROP AS 
		SELECT p.id
		FROM integration.products p
		WHERE p.last_update > last_update_dt;
		
		DELETE FROM dds.products p
		WHERE p.id IN (
				SELECT id
				FROM updated_dds_products_id_list
				);
			
		--вставляем все созданные или изменённые товары
		INSERT INTO dds.products (
											id,
											supplier_id,
											"name",
											price,
											quantity,
											last_update
											)
		SELECT p.id ,
			   supplier_id,
			   "name",
			   price,
		       quantity,
			   last_update 
		FROM integration.products p
		JOIN updated_dds_products_id_list upd ON upd.id = p.id; 
		
	END	
$$ LANGUAGE plpgsql;


--check

UPDATE integration.products
SET last_update = now()
WHERE id = 6;

CALL dds_products_load();

SELECT * FROM dds.products WHERE id = 6;




--полная загрузка данных

CREATE OR REPLACE PROCEDURE full_load()
AS $$
	DECLARE current_update_dt timestamp = now();
	BEGIN 
		CALL staging.products_load(current_update_dt);
		CALL staging.suppliers_load(current_update_dt);
	
		CALL ods.products_load();
		CALL ods.suppliers_load();
	
		CALL supplier_id_sync();
		CALL product_id_sync();
	
		CALL integration_suppliers_load();
		CALL integration_products_load();
	
		CALL dds_suppliers_load();
		CALL dds_products_load();
	END;
$$ LANGUAGE plpgsql;

CALL full_load();



	