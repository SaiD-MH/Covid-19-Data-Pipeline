CREATE TABLE IF NOT EXISTS gold.date_dim(


    date_key int primary key ,
    full_date date not null ,
    day_of_week int not null,
    day_of_month int not null,
    day_name varchar(10) not null,
    week_of_year int not null,
    month int not null,
    month_name varchar(10) not null , 
    quarter int not null , 
    year int not null,
    is_weekend boolean not null 
);