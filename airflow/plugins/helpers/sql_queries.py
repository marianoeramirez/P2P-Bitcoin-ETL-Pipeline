class SqlQueries:
    create_table = ("""
    
CREATE TABLE IF NOT EXISTS public.staging_bisq (
	price decimal(16, 8),
	amount decimal(16, 8),
	volume decimal(16, 8),
	payment_method varchar(100),
	trade_date BIGINT,
	market varchar(100)
);

CREATE TABLE IF NOT EXISTS public.staging_paxful (
	id BIGINT,
	date BIGINT,
	amount decimal(16, 8),
	price decimal(16, 8),
	payment_method varchar(255),
	payment_method_group varchar(255),
	currency varchar(20),
	type varchar(50),
	advertiser_cc varchar(20),
	user_cc varchar(20),
	crypto_rate_usd decimal(16, 8),
	crypto_code varchar(100)
);

CREATE TABLE IF NOT EXISTS public.transaction (
	id varchar(32) NOT NULL,
	date timestamp NOT NULL,
	provider int2,
	price decimal(16, 8),
	amount decimal(16, 8),
	payment_method varchar(255),
	currency1 int2,
	currency2 int2,
    type varchar(10),
	CONSTRAINT transaction_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.staging_currency (
	id int4 NOT null IDENTITY(0,1),
	name varchar(256),
	CONSTRAINT currency_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.currency (
	id int4 NOT null IDENTITY(0,1),
	name varchar(256),
	type varchar(255),
	CONSTRAINT currency_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."time" (
	date timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (date)
);

CREATE TABLE IF NOT EXISTS public.provider (
	id int4 NOT NULL,
	name varchar(256),
	CONSTRAINT provider_pkey PRIMARY KEY (id)
);

truncate table provider;

INSERT INTO public.provider
(id, "name")
VALUES(1, 'bisq'), (2,'paxful');



CREATE TABLE IF NOT EXISTS public.currency (
	id int4 NOT NULL,
	name varchar(256),
	type varchar(255),
	CONSTRAINT currency_pkey PRIMARY KEY (id)
);

    """)

    bisq_staging_currency_table_insert = """insert into staging_currency (name)
    SELECT
       distinct SPLIT_PART(sb.market,  '_', 1)
    from staging_bisq sb  where [filter]
    UNION
    SELECT
       distinct  SPLIT_PART(sb.market,  '_', 2)
    from staging_bisq sb  where [filter];
        """

    bisq_currency_table_insert = """insert into currency (name)
    SELECT
       distinct LOWER(name)
    from staging_currency where LOWER(name) not in (select name from currency);
        """

    bisq_transaction_table_insert = """insert into transaction (date, price, amount, payment_method, currency1, currency2, type, provider, id)
SELECT
    timestamp 'epoch' + sb.trade_date/1000 * interval '1 second' as date,
    sb.price, sb.amount, sb.payment_method, 
     c2.id as currency1, c3.id as currency2,
     'na', 1 as provider, 
      md5(sb.trade_date || c2.id || c3.id || provider  ) id
from staging_bisq sb join currency c2 on  SPLIT_PART(sb.market,  '_', 1) = c2.name
join currency c3 on  SPLIT_PART(sb.market,  '_', 2) = c3.name;
    """

    bisq_time_table_insert = ("""insert into time
        SELECT timestamp 'epoch' + trade_date/1000 * interval '1 second' as date, extract(hour from date), extract(day from date), extract(week from date), 
               extract(month from date), extract(year from date), extract(dayofweek from date)
        FROM staging_bisq;
    """)
