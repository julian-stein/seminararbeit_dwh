---------------- CREATE TABLES -----------------------------------------------------------------------------------------
create table staging.business_import_json(doc json);

create table staging.business_import(
business_id varchar(255) PRIMARY KEY,
name varchar(255),
address varchar(255),
city varchar(255),
state varchar(4),
postal_code varchar(16),
latitude decimal,
longitude decimal,
stars decimal,
review_count integer,
is_open integer,
attributes jsonb,
categories varchar(255)[],
hours jsonb
);

---------------- LOAD JSON-DATA FROM FILE -----------------------------------------------------------------------------
copy staging.business_import_json from '/usr/src/seminararbeit/json_data/business_json_files/yelp_academic_dataset_business_array_0.json';

---------------- CREATE TUPLES FROM JSON -------------------------------------------------------------------------------
insert into staging.business_import
select businesses.*
from staging.business_import_json
  cross join lateral json_populate_recordset(null::staging.business_import, doc) as businesses;

---------------- UNNEST ARRAYS TO DEDICATED RELATIONS ------------------------------------------------------------------
-- Create separate table for business categories

create table staging.categories (
    business_id varchar(255),
    category varchar(255)
);

insert into staging.categories
select businesses.business_id, unnest(businesses.categories) from staging.business_import as businesses;


---------------- DROP CONVERTED COLUMNS --------------------------------------------------------------------------------
alter table staging.business_import
    drop column categories;
