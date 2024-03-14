import pandas as pd
import warnings
import boto3
from botocore.exceptions import ClientError
from src.glue.lease_status_update.templates import template_leasepar, template_amort_table, template_update
import pymysql
import json

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")


class LeaseStatusUpdater:

    def __init__(self, secret: str) -> None:
        self.valores: dict = {}
        self.secret = self.get_secret(secret)

    def ejecutar_query(self, query: str, select: bool = False) -> pd.DataFrame:
        try:
            conexion = pymysql.connect(
                host='localhost',
                port=int(self.secret['DB_PORT']),
                user=self.secret['DB_USER'],
                password=self.secret['DB_PASSWORD'],
                database=self.secret['DB_NAME']
            )
            print('Conexion a la Base establecida')

            if not select:
                with conexion.cursor() as cursor:
                    cursor.execute(query)
                conexion.commit()
                print("Update ejecutado exitosamente")
                return None
            else:
                df_result = pd.read_sql(query, conexion)
                print("Select ejecutado exitosamente")
                return df_result
        except pymysql.connector.Error as error:
            print("Error al ejecutar el query:", error)
            return None
        finally:
            conexion.close()

    @staticmethod
    def get_secret(secret_name) -> dict:

        region_name = "us-east-1"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e

        return json.loads(get_secret_value_response['SecretString'])

    def main(self):

        df = self.ejecutar_query(template_leasepar, True)

        print(df)

        for index, row in df.iterrows():
            lease_id = row['idLeaseParameters']
            query = template_amort_table.substitute({'id': lease_id})
            df_amort = self.ejecutar_query(query, True)
            filtro = ((df_amort['dMonth'] == max(df_amort['dMonth'])) &
                      (df_amort['is_initial'] == 0) &
                      (df_amort['is_residualvalue'] == 0) &
                      (df_amort['invoice_date'].notnull()) &
                      (df_amort['payment_date'].notnull()))
            if df_amort[filtro].empty:
                print(f"""
                Para el id = {lease_id} aun no se terminan los plazos, por lo que mantenemos el mismo status""")
            else:
                # if lease_id == 133:
                #    print(df_amort[filtro])
                #    self.ejecutar_query(template_update.substitute({'lease_id': lease_id}))
                print(f"""
                 Para el id = {lease_id} se terminaron los plazos, 
                 Por lo que cambio de status: 'ACTIVE' a 'TERMINATED'
                 status_detail: 'PERFORMING' a 'DEFAULT""")






