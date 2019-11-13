import boto3


class AWSGlueStarter:

    def __init__(self):

        self._session = boto3.Session()
        self._glue = self._session.client('glue')
        self._lakeformation = self._session.client('lakeformation')

        self._iam_allowed_principal = {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}

    def _modify_lake_settings(self):
        print('1. Modifying Data Lake Settings to use IAM Controls only...')
        data_lake_setting = self._lakeformation.get_data_lake_settings()['DataLakeSettings']
        data_lake_setting['CreateDatabaseDefaultPermissions'] = [{'Principal': self._iam_allowed_principal,
                                                            'Permissions': ['ALL']}]
        data_lake_setting['CreateTableDefaultPermissions'] = [{'Principal': self._iam_allowed_principal,
                                                               'Permissions': ['ALL']}]
        self._lakeformation.put_data_lake_settings(DataLakeSettings=data_lake_setting)

    def _de_register(self):
        res = self._lakeformation.list_resources()
        resources = res['ResourceInfoList']
        while 'NextToken' in res:
            res = self._lakeformation.list_resources(NextToken=res['NextToken'])
            resources.extend(res['ResourceInfoList'])
        for r in resources:
            print('... Deregistering ' + r['ResourceArn'] + '...')
            self._lakeformation.deregister_resource(ResourceArn=r['ResourceArn'])


    def _grant_db_access(self):
        catalog_resource = {'Catalog': {}}
        self._lakeformation.grant_permissions(Principal=self._iam_allowed_principal,
                                        Resource=catalog_resource,
                                        Permissions=['CREATE_DATABASE'],
                                        PermissionsWithGrantOption=[])

    def _iam_allowed_principals(self):
        databases = []
        get_databases_paginator = self._glue.get_paginator('get_databases')
        for page in get_databases_paginator.paginate():
            databases.extend(page['DatabaseList'])
        for d in databases:
            print('...Granting permissions on database ' + d['Name'] + '...')

            database_resource = {'Database': {'Name': d['Name']}}
            self._lakeformation.grant_permissions(Principal=self._iam_allowed_principal,
                                            Resource=database_resource,
                                            Permissions=['ALL'],
                                            PermissionsWithGrantOption=[])

            location_uri = d.get('LocationUri')
            if location_uri is not None and location_uri != '':
                database_input = {
                    'Name': d['Name'],
                    'Description': d.get('Description', ''),
                    'LocationUri': location_uri,
                    'Parameters': d.get('Parameters', {}),
                    'CreateTableDefaultPermissions': [
                        {
                            'Principal': self._iam_allowed_principal,
                            'Permissions': ['ALL']
                        }
                    ]
                }
            else:
                database_input = {
                    'Name': d['Name'],
                    'Description': d.get('Description', ''),
                    'Parameters': d.get('Parameters', {}),
                    'CreateTableDefaultPermissions': [
                        {
                            'Principal': self._iam_allowed_principal,
                            'Permissions': ['ALL']
                        }
                    ]
                }
            self._glue.update_database(Name=d['Name'],
                             DatabaseInput=database_input)


    def _table_access(self):
        tables = []
        get_tables_paginator = self._glue.get_paginator('get_tables')
        for page in get_tables_paginator.paginate(DatabaseName= d['Name']):
            tables.extend(page['TableList'])

        databases = []
        get_databases_paginator = self._glue.get_paginator('get_databases')
        for page in get_databases_paginator.paginate():
            databases.extend(page['DatabaseList'])

        for d in databases:
            for t in tables:
                print('...Granting permissions on table ' + d['Name'] + '...')
                table_resource = {'Table': {'DatabaseName': d['Name'], 'Name': t['Name']}}
                self._lakeformation.grant_permissions(Principal=self._iam_allowed_principal,
                                                Resource=table_resource,
                                                Permissions=['ALL'],
                                                PermissionsWithGrantOption=[])


    def _revoke_permissions (self):
        print('5. Revoking all the permissions except IAM_ALLOWED_PRINCIPALS...')
        res = self._lakeformation.list_permissions()
        permissions = res['PrincipalResourcePermissions']
        while 'NextToken' in res:
            res = self._lakeformation.list_permissions(NextToken=res['NextToken'])
            permissions.extend(res['PrincipalResourcePermissions'])

        databases = []
        get_databases_paginator = self._glue.get_paginator('get_databases')
        for page in get_databases_paginator.paginate():
            databases.extend(page['DatabaseList'])

        for d in databases:
            for p in permissions:
                if p['Principal']['DataLakePrincipalIdentifier'] != 'IAM_ALLOWED_PRINCIPALS':
                    print('...Revoking permissions of ' + p['Principal']['DataLakePrincipalIdentifier'] + ' on table ' +
                          d['Name'] + '...')
                    self._lakeformation.revoke_permissions(Principal=p['Principal'],
                                                     Resource=p['Resource'],
                                                     Permissions=p['Permissions'],
                                                     PermissionsWithGrantOption=p['PermissionsWithGrantOption'])


def main():
    gl = AWSGlueStarter()