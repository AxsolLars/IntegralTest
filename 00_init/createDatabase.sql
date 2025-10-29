USE ROLE ROLE_TEST_ETL;
USE WAREHOUSE compute_wh;

CREATE OR REPLACE DATABASE testing_db COMMENT = 'Testing sandbox database';
CREATE OR REPLACE SCHEMA integral_test COMMENT = 'Schema for integration and test experiments';

USE DATABASE testing_db;
USE SCHEMA integral_test;

SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
