CREATE TABLE IF NOT EXISTS gold.region_dim (

    region_key  varchar(100) primary key,
    country_region varchar(200) not null,
    province_state varchar(200) null ,
    CONSTRAINT country_province UNIQUE (country_region , province_state) 

);