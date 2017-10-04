import pgdb
import pandas as pd


def connect_db():
    db = pgdb.connect(host='sd-postgresql-rds-instance.cubvhuigbw4h.us-west-1.rds.amazonaws.com',
                      user='smartdeposit2017', password='SmartDeposit', database='smartdeposit2017', port='5432')
    return db


def get_fundamental_data(issuer_id, item_id, periodicity, from_date, to_date, db):
          sql = "select issuer_id, period_end_date, start_date, numeric_value " \
                "from fundamental_data where issuer_id in (" + issuer_id + \
                ") and fundamental_item_id = '" + str(item_id) + "' and periodicity_id = '" + str(periodicity) + \
                "' and to_date(start_date, 'YYYYMMDD') > to_date('" + str(from_date) + \
                "','YYYYMMDD') and to_date(start_date, 'YYYYMMDD') < to_date('" + str(to_date) + "','YYYYMMDD') "
    df = pd.read_sql(sql, db)
    df = df.rename(columns={'start_date': 'dates', 'issuer_id': 'ids'})
    df['dates'] = pd.to_datetime(df['dates'], format="%Y%m%d")

    return df


def get_market_data(security_id, item_id, periodicity, from_date, to_date, db):
    sql = "select security_id, start_date, numeric_value " \
          "from market_data where security_id in (" + security_id + \
          ") and market_item_id = '" + str(item_id) + "' and periodicity_id = '" + str(periodicity) + \
          "' and to_date(start_date, 'YYYYMMDD') > to_date('" + str(from_date) + \
          "','YYYYMMDD') and to_date(start_date, 'YYYYMMDD') < to_date('" + str(to_date) + "','YYYYMMDD') "
    df = pd.read_sql(sql, db)
    df = df.rename(columns={'start_date': 'dates', 'security_id': 'ids'})
    df['dates'] = pd.to_datetime(df['dates'], format="%Y%m%d")

    return df


def company(issuer_id):
    db = connect_db()
    sql = "select issuer_id, issuer_name " \
          "from issuer_master where issuer_id in (" + issuer_id + ")"

    df = pd.read_sql(sql, db)
    return df 