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
    for values in ('AK', 'AB', 'BC', 'MB', 'NL', 'NS', 'NB', 'ON', 'PE', 'QC', 'SK', 'NT', 'YT', 'NU') partition by range(stars);

create table core.business_east partition of core.business
    for values in ('CT', 'DE', 'DC', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT', 'VA') partition by range(stars);

create table core.business_south partition of core.business
    for values in ('AL', 'AR', 'FL', 'GA', 'KY', 'LA', 'MS', 'NC', 'SC', 'TN', 'TX') partition by range(stars);

create table core.business_west partition of core.business
    for values in ('AZ', 'CA', 'CO', 'ID', 'MT', 'NV', 'NM', 'OR', 'UT', 'WA', 'WY') partition by range(stars);

create table core.business_central partition of core.business
    for values in ('IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'OK', 'SD', 'WV', 'WI') partition by range(stars);

create table core.business_islands_oversea partition of core.business
    for values in ('AS', 'GU', 'HI', 'MP', 'PR', 'VI') partition by range(stars);

create table core.business_default partition of core.business default partition by range(stars);

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
select business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, attributes, hours
from staging.business_import;

insert into core.categories
select business_id, category
from staging.categories;