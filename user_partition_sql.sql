-- TODO: friend und elite_years in core ueberfuehren und partitionieren
---------------- CREATE TABLE -----------------------------------------------------------------------------------------
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

---------------- CREATE REGION PARTITIONS ------------------------------------------------------------------------------
CREATE TABLE core.user_h0 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 0);
CREATE TABLE core.user_h1 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 1);
CREATE TABLE core.user_h2 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 2);
CREATE TABLE core.user_h3 PARTITION OF core.user FOR VALUES WITH (modulus 4, remainder 3);

---------------- INSERT DATA FROM STAGING -----------------------------------------------------------------------------
insert into core.user
select * from staging.user_import