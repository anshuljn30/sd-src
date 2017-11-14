import pgdb
import pandas as pd
import basic_tools


def connect_db():
    db = pgdb.connect(host='sd-postgresql-rds-instance.cubvhuigbw4h.us-west-1.rds.amazonaws.com',
                      user='smartdeposit2017', password='SmartDeposit', database='smartdeposit2017', port='5432')
    return db


def get_fundamental_data(issuer_id, item_id, periodicity, from_date, to_date, db):
    issuer_id = ', '.join(["'{}'".format(value) for value in issuer_id])
    from_date = from_date.strftime('%Y%m%d')
    to_date = to_date.strftime('%Y%m%d')
    sql = "SELECT fd.issuer_id, fd.period_end_date, fd.start_date, fd.numeric_value, fi.fundamental_statement_type " \
        "FROM (" \
        "SELECT *, " \
        "RANK() OVER(PARTITION BY start_date ORDER BY period_end_date DESC) AS rank " \
        "FROM fundamental_data WHERE issuer_id IN (" + issuer_id + ") " \
        ") AS fd " \
        "JOIN fundamental_item fi " \
        "ON fi.fundamental_item_id = fd.fundamental_item_id " \
        "WHERE fd.fundamental_item_id = '" + str(item_id) + "' " \
        "AND fd.periodicity_id = '" + str(periodicity) + "' "\
        "AND to_date(fd.start_date, 'YYYYMMDD') >= to_date('" + str(from_date) + "','YYYYMMDD') " \
        "AND to_date(fd.start_date, 'YYYYMMDD') <= to_date('" + str(to_date) + "','YYYYMMDD') " \
        "AND fd.rank = 1"
    data = pd.read_sql(sql, db)

    data = data.rename(columns={'start_date': 'dates', 'issuer_id': 'ids'})
    data['dates'] = pd.to_datetime(data['dates'], format="%Y%m%d")
    data['period_end_date'] = pd.to_datetime(data['period_end_date'], format="%Y%m%d", infer_datetime_format=True)
    data.ids = data.ids.astype(int)
    data.numeric_value = data.numeric_value.astype(float)
    data = data.drop_duplicates(keep='last')
    return data


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
    data = pd.read_sql(sql, db)

    data = data.rename(columns={'start_date': 'dates', 'security_id': 'ids'})
    data['dates'] = pd.to_datetime(data['dates'], format="%Y%m%d")
    data.ids = data.ids.astype(int)
    data.numeric_value = data.numeric_value.astype(float)
    data = data.drop_duplicates(keep='last')
    return data


def get_index_market_data(index_id, item_id, periodicity, from_date, to_date, db):
    security_id = ', '.join(["'{}'".format(value) for value in index_id])
    from_date = from_date.strftime('%Y%m%d')
    to_date = to_date.strftime('%Y%m%d')
    sql = "SELECT security_id, start_date, numeric_value " \
          "FROM market_data " \
          "WHERE security_id IN (" + security_id + ") " \
          "AND market_item_id = '" + str(item_id) + "' " \
          "AND periodicity_id = '" + str(periodicity) + "' " \
          "AND to_date(start_date, 'YYYYMMDD') >= to_date('" + str(from_date) + "','YYYYMMDD') " \
          "AND to_date(start_date, 'YYYYMMDD') <= to_date('" + str(to_date) + "','YYYYMMDD') "
    data = pd.read_sql(sql, db)

    data = data.rename(columns={'start_date': 'dates', 'security_id': 'ids'})
    data['dates'] = pd.to_datetime(data['dates'], format="%Y%m%d")
    data.ids = data.ids.astype(int)
    data.numeric_value = data.numeric_value.astype(float)
    data = data.drop_duplicates(keep='last')
    return data


def get_company_reference_data(issuer_id):
    db = connect_db()
    if type(issuer_id) is not list: issuer_id = list([issuer_id])
    issuer_id = ', '.join(["'{}'".format(value) for value in issuer_id])
    sql = "SELECT im.issuer_id, im.issuer_name, c.country_name, s.gics_sector_name, " \
          "ig.gics_industry_group_name, i.gics_industry_name, si.gics_subindustry_name " \
          "FROM issuer_master im " \
          "JOIN country c " \
          "ON im.country_of_domicile = c.country_code " \
          "JOIN gics_subindustry si " \
          "ON im.gics_subindustry_id = si.gics_subindustry_id " \
          "JOIN gics_industry i " \
          "ON si.gics_industry_id = i.gics_industry_id " \
          "JOIN gics_industry_group ig " \
          "ON i.gics_industry_group_id = ig.gics_industry_group_id " \
          "JOIN gics_sector s " \
          "ON ig.gics_sector_id = s.gics_sector_id " \
          "WHERE im.issuer_id in (" + issuer_id + ")"

    df = pd.read_sql(sql, db)
    df['issuer_id'] = df['issuer_id'].astype(int)
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

