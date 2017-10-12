import pgdb
import pandas as pd


def connect_db():
    db = pgdb.connect(host='sd-postgresql-rds-instance.cubvhuigbw4h.us-west-1.rds.amazonaws.com',
                      user='smartdeposit2017', password='SmartDeposit', database='smartdeposit2017', port='5432')
    return db


def get_fundamental_data(issuer_id, item_id, periodicity, from_date, to_date, db):
    issuer_id = ', '.join(["'{}'".format(value) for value in issuer_id])
    from_date = from_date.strftime('%Y%m%d')
    to_date = to_date.strftime('%Y%m%d')
    sql = "SELECT issuer_id, period_end_date, start_date, numeric_value " \
        "FROM (" \
        "SELECT *, " \
        "RANK() OVER(PARTITION BY start_date ORDER BY period_end_date DESC) AS rank " \
        "FROM fundamental_data WHERE issuer_id IN (" + issuer_id + ") " \
        ") AS x " \
        "WHERE fundamental_item_id = '" + str(item_id) + "' " \
        "AND periodicity_id = '" + str(periodicity) + "' "\
        "AND to_date(start_date, 'YYYYMMDD') >= to_date('" + str(from_date) + "','YYYYMMDD') " \
        "AND to_date(start_date, 'YYYYMMDD') <= to_date('" + str(to_date) + "','YYYYMMDD') " \
        "AND rank = 1"
    df = pd.read_sql(sql, db)
    df = df.rename(columns={'start_date': 'dates', 'issuer_id': 'ids'})
    df['dates'] = pd.to_datetime(df['dates'], format="%Y%m%d")
    return df


def get_market_data(security_id, item_id, periodicity, from_date, to_date, db):
    security_id = ', '.join(["'{}'".format(value) for value in security_id])
    from_date = from_date.strftime('%Y%m%d')
    to_date = to_date.strftime('%Y%m%d')
    sql = "SELECT security_id, start_date, numeric_value " \
          "FROM market_data " \
          "WHERE security_id IN (" + security_id + ") " \
          "AND market_item_id = '" + str(item_id) + "' " \
          "AND periodicity_id = '" + str(periodicity) + "' " \
          "AND to_date(start_date, 'YYYYMMDD') >= to_date('" + str(from_date) + "','YYYYMMDD') " \
          "AND to_date(start_date, 'YYYYMMDD') <= to_date('" + str(to_date) + "','YYYYMMDD') "
    df = pd.read_sql(sql, db)
    df = df.rename(columns={'start_date': 'dates', 'security_id': 'ids'})
    df['dates'] = pd.to_datetime(df['dates'], format="%Y%m%d")

    return df


def company(issuer_id):
    db = connect_db()
    issuer_id = ', '.join(["'{}'".format(value) for value in issuer_id])
    sql = "SELECT issuer_id, issuer_name " \
          "FROM issuer_master " \
          "WHERE issuer_id in (" + issuer_id + ")"

    df = pd.read_sql(sql, db)
    return df


def get_all_security_ids():
    db = connect_db()
    sql = "SELECT security_id " \
          "FROM security_master"
    ids = pd.read_sql(sql, db)
    ids = ids.astype(int)
    ids = ids['security_id'].tolist()
    return ids


def get_all_issuer_ids():
    db = connect_db()
    sql = "SELECT issuer_id " \
          "FROM issuer_master"
    ids = pd.read_sql(sql, db)
    ids = ids.astype(int)
    ids = ids['issuer_id'].tolist()
    return ids

