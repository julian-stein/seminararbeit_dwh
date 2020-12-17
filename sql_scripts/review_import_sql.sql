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

