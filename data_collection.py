from share import db_conn as db

#imports from amazon redshift

import simplejson
import json
import pandas as pd
from datetime import datetime, time, timedelta
from pytz import timezone
from pyspark import SparkConf, SparkContext 
from pyspark.sql import SQLContext, SparkSession
from sqlalchemy import create_engine

with open("/home/data/rami/notebook/acc/redshift.json") as fh:
    creds = simplejson.loads(fh.read())
    pw = creds['password']
    host = creds['host_name']
    user = creds['user_name']
    port = creds['port_num']
    
with open("/home/data/rami/notebook/acc/redshift-ue.json") as ue:
    creds = simplejson.loads(ue.read())
    ue_pw = creds['password']
    ue_host = creds['host_name']
    ue_user = creds['user_name']
    ue_port = creds['port_num']
    
with open("/home/data/rami/utils/postgres_con.json") as pg:
    creds = simplejson.loads(pg.read())
    pg_pw = creds['password']
    pg_host = creds['host']
    pg_user = creds['user']
    pg_port = creds['port']

yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def sparksession():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("data") \
        .config("spark.driver.extraClassPath",\
             "/home/data/share/postgresql-9.4.1207.jre7.jar")\
        .config("spark.driver.memory", "32g")\
        .config("spark.driver.cores", "16")\
        .config("spark.executor.memory", "32g")\
        .config("spark.executor.pyspark.memory", "32g")\
        .config("spark.python.worker.memory", "32g")\
        .config("spark.driver.maxResultSize", "32g")\
        .config("spark.sql.execution.arrow.enabled", "true")\
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark

def spark_sql() :
    sql = SQLContext(sparksession())
    print(sparksession().sparkContext.uiWebUrl)
    return sql

def pg_make_df(table):
    print(datetime.now())
    
#     with open(f'/home/data/rami/dags/sql/{file}', 'r') as fd:
#         query = fd.read()
                 
    spark = sparksession()
                 
    df = spark.read.format("jdbc")\
      .option("url", f"jdbc:postgresql://{pg_host}:{pg_port}/postgres")\
      .option("dbtable", f"{table}")\
      .option("user", f'{pg_user}')\
      .option("password", f"{pg_pw}")\
      .option("driver", "org.postgresql.Driver")\
      .load()
    
    table = f'{table}'.split('.')[1]
    
    sql = SQLContext(spark)
    sql.registerDataFrameAsTable(df, f'{table}')
        
    print(f'{table}')
    return df


def dw_make_df(table, query):
    print(datetime.now())
                
    spark = sparksession()
    
#     with open(f'/home/data/rami/dags/sql/{file}', 'r') as fd:
#         query = fd.read()

    df = spark.read.format("jdbc")\
      .option("url", f"jdbc:postgresql://{host}:{port}/dev")\
      .option("dbtable", f"({query})")\
      .option("user", f'{user}')\
      .option("password", f"{pw}")\
      .option("driver", "org.postgresql.Driver")\
      .load()
        
    sql = SQLContext(spark)
    sql.registerDataFrameAsTable(df, f'{table}')
                     
    print(f'{table}')
    return df


def ue_make_df(table, query):
    print(datetime.now())
    
#     with open(f'/home/data/rami/dags/sql/{file}', 'r') as fd:
#         query = fd.read()
               
    spark = sparksession()                 

    df = spark.read.format("jdbc")\
      .option("url", f"jdbc:postgresql://{ue_host}:{ue_port}/dev")\
      .option("dbtable", f"({query})")\
      .option("user", f'{ue_user}')\
      .option("password", f"{ue_pw}")\
      .option("driver", "org.postgresql.Driver")\
      .load()
        
    sql = SQLContext(spark)
    sql.registerDataFrameAsTable(df, f'{table}')
       
    print(f'{table}')
    return df


def write_db(data, tablename, mode='overwrite'):
    print(datetime.now())
    
    with open("/home/data/rami/utils/postgres_con.json", "r") as pst_json:
        pst_json = json.load(pst_json)
    
    dbname=pst_json['dbname']
    user=pst_json['user']
    host=pst_json['host']
    pw=pst_json['password']
    port=pst_json['port']    
        
    data.write.mode(mode).format('jdbc')\
        .options(url=f"jdbc:postgresql://{host}:{port}/{dbname}",
                driver="org.postgresql.Driver",
                dbtable=tablename,
                user=user,
                password=pw,
                batchsize=1000000)\
        .save()

    print(datetime.now())
    
    
def pg_engine() :
    with open("/home/data/rami/utils/postgres_con.json", "r") as pst_json:
        pst_json = json.load(pst_json)
    
    dbname=pst_json['dbname']
    user=pst_json['user']
    host=pst_json['host']
    pw=pst_json['password']
    port=pst_json['port']
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}')
    return engine


def rs_engine() :
    with open("/home/data/rami/notebook/acc/redshift.json") as fh:
        creds = simplejson.loads(fh.read())
        pw = creds['password']
        host = creds['host_name']
        user = creds['user_name']
        port = creds['port_num']
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{pw}@{host}:{port}/dev')
    return engine


def ue_engine() :
    with open("/home/data/rami/notebook/acc/redshift-ue.json") as ue:
        creds = simplejson.loads(ue.read())
        ue_pw = creds['password']
        ue_host = creds['host_name']
        ue_user = creds['user_name']
        ue_port = creds['port_num']
        
    user=ue_user
    host=ue_host
    pw=ue_pw
    port=ue_port
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{pw}@{host}:{port}/dev')
    return engine


sql = db.spark_sql().sql

def csv_make_df(table):
    
    spark = sparksession()
    
    df = spark.read.csv(table)
    sql = SQLContext(spark)
    sql.registerDataFrameAsTable(df, 'df_mongo')
    
    print(f'{table}')
    return df



df_aw = db.dw_make_df('df_aw',
f"""



select
    distinct a.*,
    b.brand,
    b.boundness,
    b.internal_category,
    b.package_name,
    b.amount,
    b.discount_amount,
    b.klass_state,
    case 
        when b.klass_state = 'basuga' then dateadd(hour, 9, b.occurred_at) 
        when b.klass_state = 'earlybird' then dateadd(hour, 9, b.basuga_at) 
    else null end as orders_avail_at_kst,
    datediff(minute, first_avail_kst, orders_avail_at_kst) as order_ticket_diff,
    
    case when b.product_f_id in ('rxi3tBXaDn15KIbcKpvp',
                                       'KuXds4cRdqsP28J8EzRT',
                                       'yQEyTuTyjdkyFDwCRvBJ', 
                                       'nkzfA7AG5vqjXKyFNYpo',
                                       '636tOlPFOrBSTiu6nw2A', 
                                       '5iunVXbZZLUGrmSDuoX5',
                                       '0op5oCZQa92BeOi8VzC8',
                                       'dSo0jklY4frWRz76LnSS',
                                       'NqCPZ2EIg43Ny6NXInyV',
                                       'Sc4zTMMLchQP3ps1sazo',
                                       'xMv9mRK6Ql2u1iYOsPje',
                                       'LfuSl2SdlshuOfNffbLg',
                                       'mHUaGm8pxxUZBkuzn2OU',
                                       'uBaXSzeifCl9MRphoW99',
                                       'I0FyAZSf7HswbewOiDsW') then 'chuseok'
               WHEN b.product_f_id IN ('0MPzSsUIvA2FzkhBzr8k',
									'dTZOgJBN00KZrOrbXfZq',
									'DMqhqLv0AHfby3UW21aS',
									'tq0f5N51oXsFgwVU43Ar') THEN 'tutorial101'
			   WHEN b.product_f_id IN ('price101Klass08',
									'price101Klass07',
									'price101Klass05',
									'price101Klass10',
									'price101Klass03',
									'GAm4C1Z3vmSYYxdxGAiW',
									'price101Klass06',
									'price101Klass09',
									'price101Klass02',
									'3BMKeWkodNUCuTMrXZ3S',
									'JuMhGo5fcRxbj1tZKmbn',
									'price101Klass04',
									'price101Klass11',
									'Vfj5LNsKwUYiwYyYT9q6') THEN 'price101'
               WHEN b.product_f_id = 'VqrEyNm59NfvYYEDQi41' THEN 'hang_gong_gwon'
               WHEN b.product_f_id = 'WLbJk0PRMk7KhCl7yd7j' THEN 'straw_queen'
               WHEN b.product_f_id IN ('uHMGNVjOppKCNUdnHiZB',
									'koreagrandma',
									'GWRNbjpJ8hPA2M685Qmz') THEN 'donation'
               WHEN b.product_f_id IN ('ur1deuIyLaMGeFQFus47',
									'FFgIsJnIfWGsHXhG0JYG',
									'muyB2zj4BjG60gXRgH1E',
									'rQWABXv9j6AMEvD9VlYO',
									'7n7JeIDVakjMgZNDqlpN',
									'TCYSx4mHGyvyKFEQT08u',
									'nSJSAPWSqmWHQ8MObGsH',
									'lz1ZhQdctsXZVhVXPGr6') THEN 'tasting101'
               WHEN b.product_title LIKE '%101원 맛보기%' THEN 'tasting101'
               WHEN b.product_f_id IN ('payment',
									'sales2',
									'presale',
									'rpXgQW7eiucGP4UG6ZKH',
									'sales4',
									'han-lab',
									'Igbfp7QoxhP9vgyUSZvM',
									'X012boW2VLNdw3jArG5d',
									'Wu0FQRnJVk0JMZ3s2UAt',
									'GF81UJvBjnD9VXks8mZ6',
									'donut-lab') THEN 'test'
                else 'other'
                end is_event,

    case when b.product_f_id in (select distinct product_f_id
                from (
                
                    select 
                        product_f_id,
                        min(occurred_at) period
                    from (
                        select 
                            product_f_id,
                            occurred_at + interval '9 hours' occurred_at,
                            sum(amount) over(partition by product_f_id order by occurred_at rows unbounded preceding) cumorderamount
                        from 
                            premart_production.orders oo
                        where 
                            klass_state = 'earlybird')
                    where 
                        cumorderamount >= 50000000
                    group by 1
                    
                    union all
                    
                    select 
                        product_f_id,
                        min(occurred_at) period
                    from (
                        select 
                            product_f_id,
                            occurred_at + interval '9 hours' occurred_at,
                            sum(ordercnt) over(partition by product_f_id order by occurred_at rows unbounded preceding) cumordercnt
                        from (
                            select 
                                product_f_id, 
                                occurred_at, 
                                count(distinct order_m_id) ordercnt
                            from 
                                premart_production.orders oo
                            where 
                                klass_state = 'earlybird'
                                and amount > 0
                                and valid
                            group by 1,2)
                            )
                    where 
                        cumordercnt >= 250
                    group by 1)
                    ) then 1 else 0 end as is_hero,
                    
    case when b.product_f_id in ('09GSRFWW1luDeCYF8BnH',
										'2xnP6X19UoflTu10YOI8',
										'6cYSU9lJOcx619NQbuhb',
										'7HgOnALNS50weWf4lHo9',
										'82ztVQjVVHqj6foQ7VXN',
										'AyRAr423hpZQqC12xli6',
										'bzNBYc7FzLkAW9fx3jSi',
										'cRfhyRrJQPbRq1er28pX',
										'CXiK0oTkrl59Rj6ZEhjJ',
										'dEBtmcVZqa02bK65UVyS',
										'GlakGRgxWd33dG9Eoit5',
										'HNi96776HQRRBT4ckXEW',
										'IIwcU5v9INIIdXWOhT5U',
										'iQZsFmHs8EftrC5MryTu',
										'Jg9cKbxbmUupktaGDVig',
										'LcYvAMeE5fXVomwXCP8D',
										'PHsGy7fQNVA3br9YHo2m',
										'Q7u18FOR1NaYcisgDF1b',
										'rZh71KBPE5puitG1JPHY',
										'U2rXKi3U5B2xY00QmmIA',
										'V7d8qdYoITA8RAdABSW0',
										'xLl6SclDTl0NNOlzTyPs',
										'yGuwkHJtrwkyx2X6KiQO',
										'galaxynote10',
										'rGeYJRGZ1HW1Of2K8jEK',
										'V7d8qdYoITA8RAdABSW0',
										'xsP7RjH7L89dBAIu8lNl',
                                        'V4Vjr810vWPY4JrEiDxE') then 1 else 0 end as is_branded
                
from (
    select
        a.*, -- 53 rows here,
        sum(case when dateadd(hour, 9, b.occurred_at) < DATEADD(hour, 24*182, first_avail_kst)
            then amount else 0 end) as sum_revenue_182days
    from (
        select
            distinct a.*, -- 52 rows here,
            b.order_m_id
        from (
        
            select
                a.*,
                sum(case when created_at < first_avail_kst then 1 else 0 end) wish_before,
                sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_10days_kst
                    then 1 else 0 end) wish_10days,
                sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_30days_kst
                    then 1 else 0 end) wish_30days,
                sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_90days_kst
                    then 1 else 0 end) wish_90days,
                sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_150days_kst
                    then 1 else 0 end) wish_150days
                    
            from (
                select
                    a.*,
                    sum(case when cheer_at < first_avail_kst then 1 else 0 end) cheers_before,
                    sum(case when dateadd(hour, 9, b.cheer_at) >= first_avail_kst and dateadd(hour, 9, b.cheer_at) < first_avail_10days_kst
                        then 1 else 0 end) cheers_10days,
                    sum(case when dateadd(hour, 9, b.cheer_at) >= first_avail_kst and dateadd(hour, 9, b.cheer_at) < first_avail_30days_kst
                        then 1 else 0 end) cheers_30days,
                    sum(case when dateadd(hour, 9, b.cheer_at) >= first_avail_kst and dateadd(hour, 9, b.cheer_at) < first_avail_90days_kst
                        then 1 else 0 end) cheers_90days,
                    sum(case when dateadd(hour, 9, b.cheer_at) >= first_avail_kst and dateadd(hour, 9, b.cheer_at) < first_avail_150days_kst
                        then 1 else 0 end) cheers_150days
                from (
                    select
                        a.*,
                        sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_1day_kst
                            then 1 else 0 end) comments_1day,
                        sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_3days_kst
                            then 1 else 0 end) comments_3days,
                        sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_10days_kst
                            then 1 else 0 end) comments_10days,
                        sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_30days_kst
                            then 1 else 0 end) comments_30days,
                        sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_90days_kst
                            then 1 else 0 end) comments_90days,
                        sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_150days_kst
                            then 1 else 0 end) comments_150days
                    from (
                        select
                            a.*,
                            
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_1day_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures where lecture_completed_condition = 'VIEWED_CONTENT')
                                then 1 else 0 end) nonmission_posts_1day,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_3days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures where lecture_completed_condition = 'VIEWED_CONTENT')
                                then 1 else 0 end) nonmission_posts_3days,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_10days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures where lecture_completed_condition = 'VIEWED_CONTENT')
                                then 1 else 0 end) nonmission_posts_10days,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_30days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures where lecture_completed_condition = 'VIEWED_CONTENT')
                                then 1 else 0 end) nonmission_posts_30days,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_90days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures where lecture_completed_condition = 'VIEWED_CONTENT')
                                then 1 else 0 end) nonmission_posts_90days,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_150days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures where lecture_completed_condition = 'VIEWED_CONTENT')
                                then 1 else 0 end) nonmission_posts_150days,
                            
                            
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_1day_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures)
                                then 1 else 0 end) posts_1day,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_3days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures)
                                then 1 else 0 end) posts_3days,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_10days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures)
                                then 1 else 0 end) posts_10days,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_30days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures)
                                then 1 else 0 end) posts_30days,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_90days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures)
                                then 1 else 0 end) posts_90days,
                            sum(case when dateadd(hour, 9, b.created_at) >= first_avail_kst and dateadd(hour, 9, b.created_at) < first_avail_150days_kst
                                and lecture_m_id in (select lecture_m_id from premart_production.lectures)
                                then 1 else 0 end) posts_150days
                            
                        from (
                            select
                                a.*,
                                b.non_secret_chamber_total_lecture_count,
                                b.difficulty,
                                
                                sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_1day_kst
                                    then 1 else 0 end) completed_lectures_1day,
                                sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_3days_kst
                                    then 1 else 0 end) completed_lectures_3days,
                                sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_10days_kst
                                    then 1 else 0 end) completed_lectures_10days,
                                sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_30days_kst
                                    then 1 else 0 end) completed_lectures_30days,
                                sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_90days_kst
                                    then 1 else 0 end) completed_lectures_90days,
                                sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_150days_kst
                                    then 1 else 0 end) completed_lectures_150days,
                                
                                cast(sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_1day_kst
                                    then 1 else 0 end) as float) / b.non_secret_chamber_total_lecture_count as completed_lectures_ratio_1day,
                                    
                                cast(sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_3days_kst
                                    then 1 else 0 end) as float)  / b.non_secret_chamber_total_lecture_count as completed_lectures_ratio_3days,
                                    
                                cast(sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_10days_kst
                                    then 1 else 0 end) as float)  / b.non_secret_chamber_total_lecture_count as completed_lectures_ratio_10days,
                                    
                                cast(sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_30days_kst
                                    then 1 else 0 end) as float)  / b.non_secret_chamber_total_lecture_count as completed_lectures_ratio_30days,
                                    
                                cast(sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_90days_kst
                                    then 1 else 0 end) as float)  / b.non_secret_chamber_total_lecture_count as completed_lectures_ratio_90days,
                                    
                                cast(sum(case when dateadd(hour, 9, b.completed_at) >= first_avail_kst and dateadd(hour, 9, b.completed_at) < first_avail_150days_kst
                                    then 1 else 0 end) as float)  / b.non_secret_chamber_total_lecture_count as completed_lectures_ratio_150days
                                
                            from (
                                select
                                    distinct a.*,
                                    b.account_type
                                from
                                    (select
                                        a.user_f_id,
                                        a.klass_m_id,
                                        a.ticket_start_at_kst as first_avail_kst,
                                        DATEADD(hour, 24, first_avail_kst) as first_avail_1day_kst,
                                        DATEADD(hour, 24*3, first_avail_kst) as first_avail_3days_kst,
                                        DATEADD(hour, 24*10, first_avail_kst) as first_avail_10days_kst,
                                        DATEADD(hour, 24*30, first_avail_kst) as first_avail_30days_kst,
                                        DATEADD(hour, 24*90, first_avail_kst) as first_avail_90days_kst,
                                        DATEADD(hour, 24*150, first_avail_kst) as first_avail_150days_kst
                                    from (
                                        select
                                            a.user_f_id,
                                            b.ticket_start_at_kst,
                                            a.klass_m_id
                                        from
                                            raw_production.klass_tickets a
                                        left join
                                            (select 
                                                user_f_id, 
                                                min(DATEADD(hour, 9, start_at)) as ticket_start_at_kst
                                            from 
                                                raw_production.klass_tickets
                                            where
                                                start_at IS NOT NULL
                                                AND is_payment_completed IS TRUE
                                                AND ticket_valid_day>=50
                                            group by 1) b
                                            on a.user_f_id = b.user_f_id
                                            and DATEADD(hour, 9, a.start_at) = b.ticket_start_at_kst
                                            ) a
                                    where
                                        a.ticket_start_at_kst >= '2020-09-01'
                                        and a.ticket_start_at_kst < '2020-12-01') a
                                left join
                                    premart_production.users b
                                    on a.user_f_id = b.user_f_id
                                where 
                                    is_admin is false) a
                                    
                            left join
                                (select
                                	a.user_f_id, 
                                	a.klass_m_id,
                                	a.ticket_m_id,
                                	b.non_secret_chamber_total_lecture_count,
                                	b.difficulty,
                                	c.lecture_m_id,
                                	c.completed_at
                                from
                                	(select
                                		*
                                	from
                                		premart_production.tickets
                                	where
                                		is_payment_completed is TRUE
                                		and deleted_at is null) as a
                                left outer join 
                                    (select
                                		*
                                	from
                                		mart_production.klasses ) as b 
                                	on a.klass_m_id = b.klass_m_id
                                left outer join
                                	(select
                                		ticket_m_id, 
                                		klass_m_id, 
                                		lecture_m_id,
                                		completed_at
                                	from
                                		mart_production.completed_lectures cl
                                	where 
                                	    is_secret_chamber is not true) as c 
                                	on a.ticket_m_id = c.ticket_m_id
                                	and a.klass_m_id = c.klass_m_id) b
                                on a.user_f_id = b.user_f_id
                                and a.klass_m_id = b.klass_m_id
                            where
                                b.non_secret_chamber_total_lecture_count > 0
                            group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12) a
            
                        left join
                            raw_production.posts b
                            on a.user_f_id = b.user_f_id
                        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24) a
                    left join
                        premart_production.comments b
                        on a.user_f_id = b.user_f_id
                    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36) a
                left join
                    premart_production.cheer_up b
                    on a.user_f_id = b.cheer_user_f_id
                group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42) a
            left join
                raw_production.reservations b
                on a.user_f_id = b.user_f_id
            group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47) a
        left join
            (select 
                distinct user_f_id,
                klass_m_id,
                order_m_id
            from 
                raw_production.klass_item_orders 
            where 
                order_state = 'PAID') b
            on a.user_f_id = b.user_f_id
            and a.klass_m_id = b.klass_m_id) a 
    left join
        premart_production.orders b
        on a.user_f_id = b.user_f_id
    where
        amount > 10000
        and confirm_state = 'approved'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
        36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53) a
left join
    premart_production.orders b
    on a.user_f_id = b.user_f_id
where 
    amount > 10000
    and confirm_state = 'approved'
    
""")

df_aw_spark = sql(
"""
select 
    a.*,
    b.min_order_ticket_diff,
    case when a.completed_lectures_ratio_150days = 1 then 1 else 0 end as completed_class
from
    df_aw a
left join
    (select 
        user_f_id, 
        min(order_ticket_diff) as min_order_ticket_diff 
    from 
        df_aw
    group by 1) b
    on a.user_f_id = b.user_f_id
where
    a.order_ticket_diff = b.min_order_ticket_diff

""")

#imports from user events db


df_ue = db.ue_make_df('df_ue',
f"""
select
    distinct user_id as user_f_id, 
    class_id,
    device,
    lv_kst as access_kst,
    'lv' as access_type
    
from (
    select user_id, class_id, date_add('hour',9,timestamp) as lv_kst, 'android' as device
    from class101_app_android_production.lecture_viewed
    where lecture_is_free is false
    union
    select user_id, class_id, date_add('hour',9,timestamp) as lv_kst, 'ios'
    from class101_app_ios_production.lecture_viewed
    where lecture_is_free is false
    union
    select user_id, class_id, date_add('hour',9,timestamp) as lv_kst, 'web'
    from class101_web_production.lecture_viewed
    where lecture_is_free is false
    union
    select user_id, class_id, date_add('hour',9,timestamp) as lv_kst, 'ios'
    from class101_app_ios_production.viewed_content
    union
    select user_id, class_id, date_add('hour',9,timestamp) as lv_kst, 'android'
    from class101_app_android_production.viewed_content)
where
    lv_kst >= '2020-01-01'

union



select
    distinct user_id as user_f_id, 
    class_id,
    device,
    pv_kst as access_kst,
    'pv'
from (
    select user_id, class_id, date_add('hour',9,timestamp) as pv_kst, 'android' as device
    from class101_app_android_production.lecture_proceeded
    where lecture_is_free is false
    union
    select user_id, class_id, date_add('hour',9,timestamp) as pv_kst, 'ios'
    from class101_app_ios_production.lecture_proceeded
    where lecture_is_free is false
    union
    select user_id, class_id, date_add('hour',9,timestamp) as pv_kst, 'web'
    from class101_web_production.lecture_proceeded
    where lecture_is_free is false)
where
    pv_kst >= '2020-01-01'
""")

#merging data sets

df_aw_ue_spark = sql(
"""
select
    a.user_f_id,
    klass_m_id,
    first_avail_kst,
    first_avail_1day_kst,
    first_avail_3days_kst,
    first_avail_10days_kst,
    first_avail_30days_kst,
    first_avail_90days_kst,
    first_avail_150days_kst,
    account_type,
    completed_lectures_1day,
    completed_lectures_3days,
    completed_lectures_10days,
    completed_lectures_30days,
    completed_lectures_90days,
    completed_lectures_150days,
    completed_lectures_ratio_1day,
    completed_lectures_ratio_3days,
    completed_lectures_ratio_10days,
    completed_lectures_ratio_30days,
    completed_lectures_ratio_90days,
    completed_lectures_ratio_150days,
    nonmission_posts_1day,
    nonmission_posts_3days,
    nonmission_posts_10days,
    nonmission_posts_30days,
    nonmission_posts_90days,
    nonmission_posts_150days,
    posts_1day,
    posts_3days,
    posts_10days,
    posts_30days,
    posts_90days,
    posts_150days,
    comments_1day,
    comments_3days,
    comments_10days,
    comments_30days,
    comments_90days,
    comments_150days,
    cheers_before,
    cheers_10days,
    cheers_30days,
    cheers_90days,
    cheers_150days,
    wish_before,
    wish_10days,
    wish_30days,
    wish_90days,
    wish_150days,
    completed_class,
    order_m_id,
    sum_revenue_182days,
    brand,
    boundness,
    internal_category,
    package_name,
    discount_amount,
    is_event,
    is_hero,
    is_branded,
    difficulty,
    amount,
    klass_state,
    orders_avail_at_kst,
    order_ticket_diff,
    min_order_ticket_diff,
    
    count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_1day_kst
        then b.user_f_id else null end) as lv_count_1day,
    count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_3days_kst
        then b.user_f_id else null end) as lv_count_3days,
    count(case when
            b.access_kst >= a.first_avail_kst
            and b.access_kst < a.first_avail_10days_kst
        then b.user_f_id else null end) as lv_count_10days,
    count(case when
            b.access_kst >= a.first_avail_kst
            and b.access_kst < a.first_avail_30days_kst
        then b.user_f_id else null end) as lv_count_30days,
    count(case when
            b.access_kst >= a.first_avail_kst
            and b.access_kst < a.first_avail_90days_kst
        then b.user_f_id else null end) as lv_count_90days,
    count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
        then b.user_f_id else null end) as lv_count_150days,



    case when count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
        then b.user_f_id else null end)=0 then 0
        else 
            cast(count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
                and device = 'web'
                then b.user_f_id else null end) as float) /
            count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
                then b.user_f_id else null end) 
        end as lv_web_ratio_150days,
                
    case when count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
        then b.user_f_id else null end)=0 then 0
        else 
            cast(count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
                and device = 'ios'
                then b.user_f_id else null end) as float) /
            count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
                then b.user_f_id else null end) end as lv_ios_ratio_150days,
    case when count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
            then b.user_f_id else null end)=0 then 0
            else 
                cast(count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
                    and device = 'android'
                    then b.user_f_id else null end) as float) /
                count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
                    then b.user_f_id else null end) end as lv_android_ratio_150days,
    case when count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
            then b.user_f_id else null end)=0 then 0
            else 
                cast(count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
                    and device in ('ios', 'android')
                    then b.user_f_id else null end) as float) /
                count(case when b.access_kst >= a.first_avail_kst and b.access_kst < a.first_avail_150days_kst
                    then b.user_f_id else null end) end as lv_app_ratio_150days,



    count(distinct(case when
            b.access_kst >= a.first_avail_kst
            and b.access_kst < a.first_avail_150days_kst
        then extract(hour from b.access_kst) else null end)) as lv_distinct_hours_150days,
    
    count(distinct(case when
            b.access_kst >= a.first_avail_kst
            and b.access_kst < a.first_avail_3days_kst
        then date(b.access_kst) else null end)) as count_distinctdays_3days,
    count(distinct(case when
            b.access_kst >= a.first_avail_kst
            and b.access_kst < a.first_avail_10days_kst
        then date(b.access_kst) else null end)) as count_distinctdays_10days,
    count(distinct(case when
            b.access_kst >= a.first_avail_kst
            and b.access_kst < a.first_avail_30days_kst
        then date(b.access_kst) else null end)) as count_distinctdays_30days,
    count(distinct(case when
            b.access_kst >= a.first_avail_kst
            and b.access_kst < a.first_avail_90days_kst
        then date(b.access_kst) else null end)) as count_distinctdays_90days,
    count(distinct(case when
            b.access_kst >= a.first_avail_kst
            and b.access_kst < a.first_avail_150days_kst
        then date(b.access_kst) else null end)) as count_distinctdays_150days


from (
    select 
        a.*,
        b.min_order_ticket_diff,
        case when a.completed_lectures_ratio_150days = 1 then 1 else 0 end as completed_class
    from
        df_aw a
    left join
        (select 
            user_f_id, 
            min(order_ticket_diff) as min_order_ticket_diff 
        from 
            df_aw
        group by 1) b
        on a.user_f_id = b.user_f_id
    where
        a.order_ticket_diff = b.min_order_ticket_diff) a
        
left join
    df_ue b
    on a.user_f_id = b.user_f_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
        36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67
        
""")

#turn merged data into pandas
df_aw_ue_pandas = df_aw_ue_spark.toPandas()
