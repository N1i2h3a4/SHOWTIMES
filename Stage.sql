create or replace database ingest_data;

use database ingest_data;

create table organize_json(json_data VARIANT);

CREATE STAGE my_stage
URL = 's3://showti/showtimes.json'
CREDENTIALS = (AWS_KEY_ID = 'AKIAR6U2AZTFW3N2UD5Q' AWS_SECRET_KEY = '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');

LIST @my_stage;

COPY INTO organize_json
FROM @my_stage
FILE_FORMAT = (TYPE = 'JSON');

select * from organize_json;


CREATE OR REPLACE TABLE raw_tab (
  j_d VARIANT);


INSERT INTO raw_tab(j_d)
SELECT VALUE json_data
FROM organize_json,
LATERAL FLATTEN(input => parse_json(organize_json.json_data:results));

select * from raw_tab;

















