CREATE TABLE null_test (id SERIAL PRIMARY KEY, value1 TEXT, value2 TEXT);
ALTER TABLE null_test REPLICA IDENTITY FULL;

INSERT INTO null_test (value1, value2) VALUES('text1', 'text2');

UPDATE null_test SET value1 = NULL, value2 = NULL where id = 1;

-- With inferred nulls enabled we can differentiate between
-- an unchanged value and a value that was updated from NULL

-- This update to value1 should have an "old" entry in the output,
-- showing the change from NULL to the string 'null'.
-- value2 is unchanged and should not have an "old" entry in the output.
UPDATE null_test SET value1 = 'null' where id = 1;

-- This update should have an "old" entry in the output, showing the
-- change from NULL to the string 'text'
UPDATE null_test SET value2 = 'text' where id = 1;