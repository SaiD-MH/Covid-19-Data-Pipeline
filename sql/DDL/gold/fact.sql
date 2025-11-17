CREATE TABLE IF NOT EXISTS gold.fact(

    confirmed bigint not null ,
    deaths bigint not null ,
    active bigint not null,
    recovered bigint not null,
    incident_rate float not null,
    case_fatality_ratio float not null, 
    region_key VARCHAR(100),
    date_key int not null,
    FOREIGN KEY (region_key) REFERENCES gold.region_dim(region_key),
    FOREIGN KEY (date_key) REFERENCES gold.date_dim(date_key)

);