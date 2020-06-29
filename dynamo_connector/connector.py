import time
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr

class DynomoConnector(object):
    def __init__(self, **param):
        param = {key: value for key, value in param.items()}
        self.aws_access_key_id = param["aws_access_key_id"]
        self.aws_secret_access_key = param["aws_secret_access_key"]
        self.region_name = param["region_name"]
        self.resource = boto3.resource('dynamodb', aws_access_key_id=self.aws_access_key_id,
                                       aws_secret_access_key=self.aws_secret_access_key, region_name=self.region_name)
        self.client = boto3.client('dynamodb', aws_access_key_id=self.aws_access_key_id,
                                   aws_secret_access_key=self.aws_secret_access_key, region_name=self.region_name)
        self.table_list = self.client.list_tables()["TableNames"]
        print(self.table_list)

    def create_table(self, tablename, keyschema, attributedefinitions, provisionedthroughput, volume_mode="on-demand"):
        if volume_mode == 'on-demand':
            table = self.client.create_table(
                TableName=tablename,
                KeySchema=keyschema,
                AttributeDefinitions=attributedefinitions,
                BillingMode='PAY_PER_REQUEST',
            )
        else:
            table = self.client.create_table(
                TableName=tablename,
                KeySchema=keyschema,
                AttributeDefinitions=attributedefinitions,
                ProvisionedThroughput=provisionedthroughput
            )

        response_code = table["ResponseMetadata"]["HTTPStatusCode"]

        if response_code == 200:
            print("create success!")
        else:
            print("create fail! HTTPStatusCode {}".format(response_code))

    def connect(self, table_name):
        self.conn = self.resource.Table(table_name)
        self.name = self.conn.name
        self.key_schema = self.conn.key_schema
        self.attribute_definitions = self.conn.attribute_definitions
        self.provisioned_throughput = self.conn.provisioned_throughput
        self.billing_mode_summary = self.conn.billing_mode_summary

        return self.key_schema

    def get(self, output='df'):
        # default output is JSON format
        response = self.conn.scan()
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = self.conn.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        if output == 'df':
            return pd.DataFrame(data)
        else:
            return data

    def get_with_condition(self, output='df', condition_type='orddt', start='2020-01-01 00:00:00',
                           end='2020-12-31 23:59:95'):
        # default output is JSON format
        # condition_type
        # orddt : use reange start_date ~ end_date
        # else : get all
        if condition_type == 'orddt':
            response = self.conn.scan(
                FilterExpression=Attr('orddt').between(start, end)
            )
        else:
            response = self.conn.scan()

        if response["Count"] >= 80:
            data = response['Items']
            while 'LastEvaluatedKey' in response:
                response = self.conn.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                data.extend(response['Items'])
        else:
            data = response['Items']

        if output == 'df':
            return pd.DataFrame(data)
        else:
            return data

    def insert_record(self, item):
        # put item must json foramt : DataFrame.to_dict()
        self.conn.put_item(
            Item=item
        )

    def bulk_insert(self, df):
        with self.conn.batch_writer() as batch:
            for idx in range(df.shape[0]):
                batch.put_item(Item=df.iloc[idx].to_dict())

        print("bulk succecss")

    def delete_record(self, key):
        self.conn.delete_item(Key=key)

    def delete_table(self):
        self.conn.delete()
        print("{0} DELETED!".format(self.name))

    def recreate(self):
        if self.billing_mode_summary['BillingMode'] == 'PAY_PER_REQUEST':
            self.create_table(tablename=self.name, keyschema=self.key_schema,
                              attributedefinitions=self.attribute_definitions,
                              provisionedthroughput=self.provisioned_throughput, volume_mode='on-demand')
        else:
            self.create_table(tablename=self.name, keyschema=self.key_schema,
                              attributedefinitions=self.attribute_definitions,
                              provisionedthroughput=self.provisioned_throughput, volume_mode=self.billing_mode_summary)

    def truncate(self):
        self.conn.delete()
        while self.name in self.client.list_tables()["TableNames"]:
            print("table deleting...")
            time.sleep(5)

        if self.billing_mode_summary['BillingMode'] == 'PAY_PER_REQUEST':
            self.create_table(tablename=self.name, keyschema=self.key_schema,
                              attributedefinitions=self.attribute_definitions,
                              provisionedthroughput=self.provisioned_throughput, volume_mode='on-demand')
        else:
            self.create_table(tablename=self.name, keyschema=self.key_schema,
                              attributedefinitions=self.attribute_definitions,
                              provisionedthroughput=self.provisioned_throughput, volume_mode=self.billing_mode_summary)

        while self.name not in self.client.list_tables()["TableNames"]:
            print("table creating...")

        print("truncate success!")