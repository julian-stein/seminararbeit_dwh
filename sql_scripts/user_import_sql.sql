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