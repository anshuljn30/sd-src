class Company:
    issuer_id = []
    issuer_name = []
    country = []
    sector = []
    industry_group = []
    industry = []
    sub_industry = []

    def __init__(self, ids):
        import db_tools as db
        if type(ids) is not list:
            issuer_id = list([ids])
        data = db.get_company_reference_data(ids)

        self.issuer_id = ids
        self.issuer_name = data['issuer_name']
        self.country = data[['issuer_id', 'country_name']]
        self.sector = data[['issuer_id', 'gics_sector_name']]
        self.industry_group = data[['issuer_id', 'gics_industry_group_name']]
        self.industry = data[['issuer_id', 'gics_industry_name']]
        self.sub_industry = data[['issuer_id', 'gics_subindustry_name']]

