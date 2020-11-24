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

---------------- INSERT DATA FROM STAGING -----------------------------------------------------------------------------
insert into core.review
select *
from staging.review_import;






