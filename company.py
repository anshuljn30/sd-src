class Company:
    issuer_id = []
    issuer_name = []
    country = []
    sector = []
    industry_group = []
    industry = []
    subindustry = []

    def __init__(self, ids):
        import db_tools as db
        if type(ids) is not list: issuer_id = list([ids])
        data = db.get_company_reference_data(ids)

        self.issuer_id = ids
        self.issuer_name = data['issuer_name']
        self.country = data[['issuer_id', 'country_name']].set_index('issuer_id').T.to_dict(orient='records')[0]
        self.sector = data[['issuer_id', 'gics_sector_name']].set_index('issuer_id').T.to_dict(orient='records')[0]
        self.industry_group = data[['issuer_id', 'gics_industry_group_name']].set_index('issuer_id').T.to_dict(orient='records')[0]
        self.industry = data[['issuer_id', 'gics_industry_name']].set_index('issuer_id').T.to_dict(orient='records')[0]
        self.subindustry = data[['issuer_id', 'gics_subindustry_name']].set_index('issuer_id').T.to_dict(orient='records')[0]

