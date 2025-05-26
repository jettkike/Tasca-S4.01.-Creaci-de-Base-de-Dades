-- Nivel 1
-- Descarga los archivos CSV, estudialas y diseña una base de datos con un esquema de estrella que contenga,
-- al menos 4 tablas de las que puedas realizar las siguientes consultas:
CREATE DATABASE IF NOT EXISTS transacciones;
USE transacciones;

-- creamos las tablas
CREATE TABLE IF NOT EXISTS companies (
	company_id VARCHAR(255) PRIMARY KEY,
    company_name VARCHAR(255),
	phone VARCHAR(255),
    email VARCHAR(255),
    country VARCHAR(255),
    website VARCHAR(255)
);

DESC companies;

CREATE TABLE IF NOT EXISTS credit_cards (
	id VARCHAR(255) PRIMARY KEY,
    user_id INT,
    iban VARCHAR(255),
    pan VARCHAR(255),
    pin VARCHAR(255),
    cvv INT,
    track1 VARCHAR(255),
    track2 VARCHAR(255),
    expiring_date VARCHAR(255)
);

DESC credit_cards;

CREATE TABLE IF NOT EXISTS products (
	id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(255),
    price VARCHAR(255),
    colour VARCHAR(255),
    weight VARCHAR(255),
    warehouse_id VARCHAR(255)
);

DESC products;

CREATE TABLE IF NOT EXISTS transactions (
    id VARCHAR(255) PRIMARY KEY,
    card_id VARCHAR(255),
    business_id VARCHAR(255),
    timestamp TIMESTAMP,
    amount DECIMAL(10, 2),
    declined TINYINT,
    product_ids INT,
    user_id INT,
    lat FLOAT,
    longitude FLOAT
);

DESC transactions;

-- los campos de las tablas user_ca, user_uk, users_usa son iguales en estructura, asi que uniremos los 3 archivos csv en una sola tabla users
CREATE TABLE IF NOT EXISTS users (
	id INT PRIMARY KEY AUTO_INCREMENT,
	name VARCHAR(255),
	surname VARCHAR(255),
	phone VARCHAR(255),
	email VARCHAR(255),
	birth_date VARCHAR(255),
	country VARCHAR(255),
	city VARCHAR(255),
	postal_code VARCHAR(255),
	address VARCHAR(255)
);

DESC users;

-- carga de archivos csv
-- tabla companies
SELECT * FROM companies; -- miramos si existe información en la tabla companies antes de cargar en introducir los datos.

LOAD DATA
INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\companies.csv'
INTO TABLE companies 
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS;

SELECT * FROM companies; -- comprobamos que se hayan cargado en introducido los datos en la tabla companies

-- tabla credit_cards
SELECT * FROM credit_cards;

LOAD DATA
INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\credit_cards.csv'
INTO TABLE credit_cards 
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS;

SELECT * FROM credit_cards;

-- tabla products
SELECT * FROM products;

LOAD DATA
INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS;

SELECT * FROM products;

-- tabla transactions
SELECT * FROM transactions;

LOAD DATA
INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM transactions;

-- tabla users
SELECT * FROM users;

LOAD DATA
INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\users_uk.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SELECT * FROM users;

LOAD DATA
INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\users_ca.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA
INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\users_usa.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SELECT * FROM users;


-- se crean los constraint para relacionar las tablas
ALTER TABLE transactions
ADD CONSTRAINT fk_trans_users
FOREIGN KEY(user_id) REFERENCES users(id);

ALTER TABLE transactions
ADD CONSTRAINT fk_trans_comp
FOREIGN KEY(business_id) REFERENCES companies(company_id);

ALTER TABLE transactions
ADD CONSTRAINT fk_trans_credit
FOREIGN KEY(card_id) REFERENCES credit_cards(id);

ALTER TABLE transactions
ADD CONSTRAINT fk_trans_prod
FOREIGN KEY(product_ids) REFERENCES products(id);

SELECT product_ids
FROM TRANSACTIONS;

-- - Ejercicio 1
-- Realiza una subconsulta que muestre a todos los usuarios con más de 30 transacciones utilizando al menos 2 tablas.

SELECT t.user_id, COUNT(*) AS Num_Transacciones
FROM transactions t
WHERE t.user_id = ANY (SELECT u.id FROM users u)
GROUP BY t.user_id
HAVING Num_Transacciones > 30
ORDER BY Num_Transacciones DESC;

-- - Ejercicio 2
-- Muestra la media de amount por IBAN de las tarjetas de crédito en la compañía Donec Ltd., utiliza por lo menos 2 tablas.

SELECT round(AVG(t.amount),2) AS Media_Amount_Donet
FROM transactions t
INNER JOIN credit_cards cr ON t.card_id = cr.id
WHERE t.business_id = (SELECT company_id FROM companies WHERE company_name = 'Donec Ltd');


-- ********************************************************************************************************************************************************
-- Nivell 2 
-- Crea una nova taula que reflecteixi l'estat de les targetes de crèdit basat 
-- en si les últimes tres transaccions van ser declinades i genera la següent consulta:
CREATE TABLE activ_card AS
SELECT card_id,
		CASE
			WHEN sum(declined) >= 3 THEN ' no activo'
            ELSE 'activo'
		END AS actividad
FROM (SELECT card_id, declined,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS row_num
		FROM transactions
						) AS hist_card
WHERE row_num <=3
GROUP BY card_id;

SELECT * FROM activ_card;

-- Relaciono esta tabla con el  modelo
ALTER TABLE activ_card
ADD CONSTRAINT fk_credit FOREIGN KEY(card_id) REFERENCES credit_cards(id);

-- - Exercici 1 
-- Quantes targetes estan actives? 
SELECT count(*)
FROM activ_card
WHERE actividad = 'activo';

-- *********************************************************************************************************************************
-- Nivell 3
-- Crea una tabla con la que podamos unir los datos del nuevo archivo products.csv con la base de datos creada, 
-- teniendo en cuenta que desde transaction tienes product_ids. Genera la siguiente consulta:

CREATE TABLE trans_product AS
SELECT id,
	SUBSTRING_INDEX(product_ids, ',', 1) AS producto
FROM transactions
WHERE product_ids LIKE '%'
UNION ALL
   SELECT id,
       SUBSTRING_INDEX(SUBSTRING_INDEX(product_ids, ',', -2), ',', -1) AS producto
   FROM transactions
   WHERE product_ids LIKE '%,%'
   UNION ALL
   SELECT id,
		SUBSTRING_INDEX(SUBSTRING_INDEX(product_ids, ',', 2), ',', -1) AS producto
   FROM transactions
   WHERE product_ids LIKE '%,%,%'
UNION ALL
   SELECT id,
		SUBSTRING_INDEX(SUBSTRING_INDEX(product_ids, ',', -2), ',', 1) AS producto
   FROM transactions
   WHERE product_ids LIKE '%,%,%,%';
       
SELECT *
FROM trans_product;

ALTER TABLE trans_product MODIFY producto INT; -- modificar tipo de dato a INT

-- creaamos pk compuesta
ALTER TABLE trans_product ADD CONSTRAINT primary key (id,producto);

ALTER TABLE trans_product
ADD CONSTRAINT fk_product FOREIGN KEY(producto) REFERENCES products(id);

ALTER TABLE trans_product
ADD CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES transactions(id);


-- Exercici 1
-- Necesitamos conocer el número de veces que se ha vendido cada producto.

SELECT tp.producto as product, COUNT(tp.id) AS VECES_VENDIDO
FROM trans_product tp
INNER JOIN transactions t ON tp.id = t.id
WHERE declined = '0'
GROUP BY product
ORDER BY product;
