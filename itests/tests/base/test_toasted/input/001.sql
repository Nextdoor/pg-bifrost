CREATE TABLE toast_test (id SERIAL PRIMARY KEY, value TEXT, ivalue INTEGER);

INSERT INTO toast_test (value, ivalue) VALUES('small text', 1);
INSERT INTO toast_test (value, ivalue) VALUES('small text 2', 1);

UPDATE toast_test SET ivalue = 2 where id = 1;
UPDATE toast_test SET ivalue = 2 where id = 2;

ALTER TABLE toast_test REPLICA IDENTITY FULL;
INSERT INTO toast_test (value, ivalue) VALUES('small text 3', 1);
UPDATE toast_test SET ivalue = 2 where id = 3;

UPDATE toast_test SET value = 'updated small text 3' where id = 3;