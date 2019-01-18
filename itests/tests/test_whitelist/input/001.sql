CREATE TABLE customers (id serial primary key, first_name text, last_name text);
CREATE TABLE inventory (id serial primary key, name text);
CREATE TABLE passwords (id serial primary key, password text);

INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO inventory (name) VALUES ('Thingy');
INSERT INTO passwords (password) VALUES ('secret');
