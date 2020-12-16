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

---------------- CREATE USER PARTITIONS ------------------------------------------------------------------------------
CREATE TABLE core.user_h0 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 0);
CREATE TABLE core.user_h1 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 1);
CREATE TABLE core.user_h2 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 2);
CREATE TABLE core.user_h3 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 3);

---------------- CREATE FRIENDS PARTITIONS ------------------------------------------------------------------------------
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

---------------- CREATE ELITE_YEARS PARTITIONS ------------------------------------------------------------------------------
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