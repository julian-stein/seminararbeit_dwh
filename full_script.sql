------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------- INITIAL CREATIONS ---------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
CREATE DATABASE seminararbeit;
\c seminararbeit;
CREATE SCHEMA staging;
CREATE SCHEMA core;

------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------- IMPORT BUSINESSES ---------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

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

------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------- IMPORT USERS -----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

---------------- CREATE TABLES -----------------------------------------------------------------------------------------
create table staging.user_import_json (doc json);

create table staging.user_import(
    user_id varchar(255) primary key,
    name varchar(255),
    review_count int,
    yelping_since timestamp,
    friends varchar(255)[],
    useful integer,
    funny integer,
    cool integer,
    fans integer,
    elite integer[],
    average_stars decimal,
    compliment_hot integer,
    compliment_more integer,
    compliment_profile integer,
    compliment_cute integer,
    compliment_list integer,
    compliment_note integer,
    compliment_plain integer,
    compliment_cool integer,
    compliment_funny integer,
    compliment_writer integer,
    compliment_photos integer
);

---------------- LOAD JSON-DATA FROM FILES -----------------------------------------------------------------------------
copy staging.user_import_json
    from '/usr/src/seminararbeit/json_data/user_json_files/yelp_academic_dataset_user_array_0.json';
copy staging.user_import_json
    from '/usr/src/seminararbeit/json_data/user_json_files/yelp_academic_dataset_user_array_1.json';
copy staging.user_import_json
    from '/usr/src/seminararbeit/json_data/user_json_files/yelp_academic_dataset_user_array_2.json';
copy staging.user_import_json
    from '/usr/src/seminararbeit/json_data/user_json_files/yelp_academic_dataset_user_array_3.json';
copy staging.user_import_json
    from '/usr/src/seminararbeit/json_data/user_json_files/yelp_academic_dataset_user_array_4.json';
copy staging.user_import_json
    from '/usr/src/seminararbeit/json_data/user_json_files/yelp_academic_dataset_user_array_5.json';
copy staging.user_import_json
    from '/usr/src/seminararbeit/json_data/user_json_files/yelp_academic_dataset_user_array_6.json';
copy staging.user_import_json
    from '/usr/src/seminararbeit/json_data/user_json_files/yelp_academic_dataset_user_array_7.json';

---------------- CREATE TUPLES FROM JSON -------------------------------------------------------------------------------
insert into staging.user_import
select users.*
from staging.user_import_json
  cross join lateral json_populate_recordset(null::staging.user_import, doc) as users;

---------------- UNNEST ARRAYS TO DEDICATED RELATIONS ------------------------------------------------------------------
-- Create separate table for users' friends

create table staging.friends (
    user_id varchar(255),
    friends_with varchar(255),
    primary key (user_id, friends_with)
);

insert into staging.friends
select users.user_id, unnest(users.friends) from staging.user_import as users;

-- Create separate table for users' elite years

create table staging.elite_years (
    user_id varchar(255),
    year integer
);

insert into staging.elite_years
select users.user_id, unnest(users.elite) from staging.user_import as users;

---------------- DROP CONVERTED COLUMNS --------------------------------------------------------------------------------

alter table staging.user_import
    drop column friends,
    drop column elite;

------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------- IMPORT REVIEWS ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

create table staging.review_import_json(doc json);
create table staging.review_import_json_as_text(doc text);

create table staging.review_import(
    review_id varchar(255),
    business_id varchar(255),
    user_id varchar(255),
    stars float,
    date timestamp,
    text text,
    useful integer,
    funny integer,
    cool integer
);

copy staging.review_import_json_as_text from '/usr/src/seminararbeit/json_data/review_json_files/yelp_academic_dataset_review_array_0.json';
copy staging.review_import_json_as_text from '/usr/src/seminararbeit/json_data/review_json_files/yelp_academic_dataset_review_array_1.json';
copy staging.review_import_json_as_text from '/usr/src/seminararbeit/json_data/review_json_files/yelp_academic_dataset_review_array_2.json';
copy staging.review_import_json_as_text from '/usr/src/seminararbeit/json_data/review_json_files/yelp_academic_dataset_review_array_3.json';

insert into staging.review_import_json select (replace(doc, E'\n', '\n')::json) from staging.review_import_json_as_text;

insert into staging.review_import
select reviews.*
from staging.review_import_json l
  cross join lateral json_populate_recordset(null::staging.review_import, doc) as reviews;

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------- PARTITION BUSINESSES -------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

---------------- CREATE TABLES ------------------------------------------------------------------------------------------
create table core.business(
    business_id varchar(255),
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
    hours jsonb
) partition by list(state);

create table core.categories (
    business_id varchar(255),
    category varchar(255)
) partition by hash(business_id);

---------------- CREATE BUSINESS REGION PARTITIONS ---------------------------------------------------------------------
create table core.business_north partition of core.business
    for values in ('AK', 'AB', 'BC', 'MB', 'NL', 'NS', 'NB', 'ON', 'PE', 'QC', 'SK', 'NT', 'YT', 'NU')
    partition by range(stars);

create table core.business_east partition of core.business
    for values in ('CT', 'DE', 'DC', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT', 'VA')
    partition by range(stars);

create table core.business_south partition of core.business
    for values in ('AL', 'AR', 'FL', 'GA', 'KY', 'LA', 'MS', 'NC', 'SC', 'TN', 'TX')
    partition by range(stars);

create table core.business_west partition of core.business
    for values in ('AZ', 'CA', 'CO', 'ID', 'MT', 'NV', 'NM', 'OR', 'UT', 'WA', 'WY')
    partition by range(stars);

create table core.business_central partition of core.business
    for values in ('IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'OK', 'SD', 'WV', 'WI')
    partition by range(stars);

create table core.business_islands_oversea partition of core.business
    for values in ('AS', 'GU', 'HI', 'MP', 'PR', 'VI')
    partition by range(stars);

create table core.business_default partition of core.business default
    partition by range(stars);

---------------- CREATE BUSINESS STAR SUBPARTITIONS --------------------------------------------------------------------
CREATE TABLE core.business_north_1to2stars PARTITION OF core.business_north
    FOR VALUES FROM (1.0) TO (2.0);
CREATE TABLE core.business_north_2to3stars PARTITION OF core.business_north
    FOR VALUES FROM (2.0) TO (3.0);
CREATE TABLE core.business_north_3to4stars PARTITION OF core.business_north
    FOR VALUES FROM (3.0) TO (4.0);
CREATE TABLE core.business_north_4to5stars PARTITION OF core.business_north
    FOR VALUES FROM (4.0) TO (5.1);

CREATE TABLE core.business_east_1to2stars PARTITION OF core.business_east
    FOR VALUES FROM (1.0) TO (2.0);
CREATE TABLE core.business_east_2to3stars PARTITION OF core.business_east
    FOR VALUES FROM (2.0) TO (3.0);
CREATE TABLE core.business_east_3to4stars PARTITION OF core.business_east
    FOR VALUES FROM (3.0) TO (4.0);
CREATE TABLE core.business_east_4to5stars PARTITION OF core.business_east
    FOR VALUES FROM (4.0) TO (5.1);

CREATE TABLE core.business_south_1to2stars PARTITION OF core.business_south
    FOR VALUES FROM (1.0) TO (2.0);
CREATE TABLE core.business_south_2to3stars PARTITION OF core.business_south
    FOR VALUES FROM (2.0) TO (3.0);
CREATE TABLE core.business_south_3to4stars PARTITION OF core.business_south
    FOR VALUES FROM (3.0) TO (4.0);
CREATE TABLE core.business_south_4to5stars PARTITION OF core.business_south
    FOR VALUES FROM (4.0) TO (5.1);

CREATE TABLE core.business_west_1to2stars PARTITION OF core.business_west
    FOR VALUES FROM (1.0) TO (2.0);
CREATE TABLE core.business_west_2to3stars PARTITION OF core.business_west
    FOR VALUES FROM (2.0) TO (3.0);
CREATE TABLE core.business_west_3to4stars PARTITION OF core.business_west
    FOR VALUES FROM (3.0) TO (4.0);
CREATE TABLE core.business_west_4to5stars PARTITION OF core.business_west
    FOR VALUES FROM (4.0) TO (5.1);

CREATE TABLE core.business_central_1to2stars PARTITION OF core.business_central
    FOR VALUES FROM (1.0) TO (2.0);
CREATE TABLE core.business_central_2to3stars PARTITION OF core.business_central
    FOR VALUES FROM (2.0) TO (3.0);
CREATE TABLE core.business_central_3to4stars PARTITION OF core.business_central
    FOR VALUES FROM (3.0) TO (4.0);
CREATE TABLE core.business_central_4to5stars PARTITION OF core.business_central
    FOR VALUES FROM (4.0) TO (5.1);

CREATE TABLE core.business_islands_oversea_1to2stars PARTITION OF core.business_islands_oversea
    FOR VALUES FROM (1.0) TO (2.0);
CREATE TABLE core.business_islands_oversea_2to3stars PARTITION OF core.business_islands_oversea
    FOR VALUES FROM (2.0) TO (3.0);
CREATE TABLE core.business_islands_oversea_3to4stars PARTITION OF core.business_islands_oversea
    FOR VALUES FROM (3.0) TO (4.0);
CREATE TABLE core.business_islands_oversea_4to5stars PARTITION OF core.business_islands_oversea
    FOR VALUES FROM (4.0) TO (5.1);

CREATE TABLE core.business_default_1to2stars PARTITION OF core.business_default
    FOR VALUES FROM (1.0) TO (2.0);
CREATE TABLE core.business_default_2to3stars PARTITION OF core.business_default
    FOR VALUES FROM (2.0) TO (3.0);
CREATE TABLE core.business_default_3to4stars PARTITION OF core.business_default
    FOR VALUES FROM (3.0) TO (4.0);
CREATE TABLE core.business_default_4to5stars PARTITION OF core.business_default
    FOR VALUES FROM (4.0) TO (5.1);

---------------- CREATE CATEGORIES HASH PARTITIONS ---------------------------------------------------------------------
CREATE TABLE core.categories_h0 PARTITION OF core.categories FOR VALUES WITH (modulus 4, remainder 0);
CREATE TABLE core.categories_h1 PARTITION OF core.categories FOR VALUES WITH (modulus 4, remainder 1);
CREATE TABLE core.categories_h2 PARTITION OF core.categories FOR VALUES WITH (modulus 4, remainder 2);
CREATE TABLE core.categories_h3 PARTITION OF core.categories FOR VALUES WITH (modulus 4, remainder 3);

---------------- INSERT DATA FROM STAGING ------------------------------------------------------------------------------
insert into core.business
select business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count,
       is_open, attributes, hours
from staging.business_import;

insert into core.categories
select business_id, category
from staging.categories;

------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------- PARTITION USERS ---------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

---------------- CREATE TABLES -----------------------------------------------------------------------------------------
create table core.user(
    user_id varchar(255) primary key,
    name varchar(255),
    review_count int,
    yelping_since timestamp,
    useful integer,
    funny integer,
    cool integer,
    fans integer,
    average_stars decimal,
    compliment_hot integer,
    compliment_more integer,
    compliment_profile integer,
    compliment_cute integer,
    compliment_list integer,
    compliment_note integer,
    compliment_plain integer,
    compliment_cool integer,
    compliment_funny integer,
    compliment_writer integer,
    compliment_photos integer
) partition by hash(user_id);

create table core.friends(
    user_id varchar(255),
    friends_with varchar(255),
    primary key (user_id, friends_with)
) partition by hash(user_id);

create table core.elite_years(
    user_id varchar(255),
    year integer,
    primary key (user_id, year)
) partition by hash(user_id);

---------------- CREATE USER PARTITIONS --------------------------------------------------------------------------------
CREATE TABLE core.user_h0 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 0);
CREATE TABLE core.user_h1 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 1);
CREATE TABLE core.user_h2 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 2);
CREATE TABLE core.user_h3 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 3);

---------------- CREATE FRIENDS PARTITIONS -----------------------------------------------------------------------------
CREATE TABLE core.friends_h0 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 0);
CREATE TABLE core.friends_h1 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 1);
CREATE TABLE core.friends_h2 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 2);
CREATE TABLE core.friends_h3 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 3);
CREATE TABLE core.friends_h4 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 4);
CREATE TABLE core.friends_h5 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 5);
CREATE TABLE core.friends_h6 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 6);
CREATE TABLE core.friends_h7 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 7);
CREATE TABLE core.friends_h8 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 8);
CREATE TABLE core.friends_h9 PARTITION OF core.friends FOR VALUES WITH (modulus 10, remainder 9);

---------------- CREATE ELITE_YEARS PARTITIONS -------------------------------------------------------------------------
CREATE TABLE core.elite_years_h0 PARTITION OF core.elite_years FOR VALUES WITH (modulus 4, remainder 0);
CREATE TABLE core.elite_years_h1 PARTITION OF core.elite_years FOR VALUES WITH (modulus 4, remainder 1);
CREATE TABLE core.elite_years_h2 PARTITION OF core.elite_years FOR VALUES WITH (modulus 4, remainder 2);
CREATE TABLE core.elite_years_h3 PARTITION OF core.elite_years FOR VALUES WITH (modulus 4, remainder 3);

---------------- INSERT DATA FROM STAGING -----------------------------------------------------------------------------
insert into core.user
select * from staging.user_import;

insert into core.friends
select * from staging.friends;

insert into core.elite_years
select * from staging.elite_years;

------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------- PARTITION REVIEWS --------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

---------------- CREATE TABLES -----------------------------------------------------------------------------------------
create table core.review(
    review_id varchar(255),
    business_id varchar(255),
    user_id varchar(255),
    stars float,
    date timestamp,
    text text,
    useful integer,
    funny integer,
    cool integer
) partition by range (date_part('year', date));

---------------- CREATE YEAR PARTITIONS ------------------------------------------------------------------------------
create table core.review_2019 partition of core.review FOR VALUES FROM (2019) TO (2020) partition by list(stars);

create table core.review_2018 partition of core.review FOR VALUES FROM (2018) TO (2019) partition by list(stars);

create table core.review_2017 partition of core.review FOR VALUES FROM (2017) TO (2018) partition by list(stars);

create table core.review_older partition of core.review default partition by list(stars);

---------------- CREATE STAR SUBPARTITIONS -----------------------------------------------------------------------------
create table core.review_2019_1star partition of core.review_2019 for values in (1);
create table core.review_2019_2star partition of core.review_2019 for values in (2);
create table core.review_2019_3star partition of core.review_2019 for values in (3);
create table core.review_2019_4star partition of core.review_2019 for values in (4);
create table core.review_2019_5star partition of core.review_2019 for values in (5);

create table core.review_2018_1star partition of core.review_2018 for values in (1);
create table core.review_2018_2star partition of core.review_2018 for values in (2);
create table core.review_2018_3star partition of core.review_2018 for values in (3);
create table core.review_2018_4star partition of core.review_2018 for values in (4);
create table core.review_2018_5star partition of core.review_2018 for values in (5);

create table core.review_2017_1star partition of core.review_2017 for values in (1);
create table core.review_2017_2star partition of core.review_2017 for values in (2);
create table core.review_2017_3star partition of core.review_2017 for values in (3);
create table core.review_2017_4star partition of core.review_2017 for values in (4);
create table core.review_2017_5star partition of core.review_2017 for values in (5);

create table core.review_older_1star partition of core.review_older for values in (1);
create table core.review_older_2star partition of core.review_older for values in (2);
create table core.review_older_3star partition of core.review_older for values in (3);
create table core.review_older_4star partition of core.review_older for values in (4);
create table core.review_older_5star partition of core.review_older for values in (5);

---------------- INSERT DATA FROM STAGING ------------------------------------------------------------------------------
insert into core.review
select *
from staging.review_import;





