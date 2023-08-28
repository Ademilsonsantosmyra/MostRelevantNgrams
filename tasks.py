import mysql.connector
from datetime import datetime, timedelta
import json
import psutil


class DatabaseQuery:
    def __init__(self, host,user,password,database,table_name):
        self.conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
        self.cursor = self.conn.cursor()
        self.table_name = table_name
    
    def execute_q(self, sql_query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_query)
            self.conn.commit()
        except mysql.connector.Error as err:
            print(f"Erro ao executar a consulta SQL: {err}")
            
    def execute_query(self, query, params=None):
        if params is None:
            params = ()
        self.cursor.execute(query, params)
        result = self.cursor.fetchall()
        # Obter os nomes das colunas a partir do cursor
        column_names = [desc[0] for desc in self.cursor.description]
        self.close_connection()
        return column_names,result
    
    def executemany_query(self, queries):
        for query in queries:
            self.cursor.execute(query)
        self.conn.commit()

    def close_connection(self):
        self.conn.close()
    
    def connection_commit(self):
        self.conn.commit()

    def CriarStaging(self):
        # Verificar se tabela existe
        self.cursor.execute(f"SHOW TABLES LIKE '{self.table_name}'")
        result = self.cursor.fetchone()

        if result:
            self.cursor.execute(f"DROP TABLE {self.table_name}")
            self.conn.commit()
            print("tabela Excluida com sucesso.")    

        # Criar tabela
        self.cursor.execute(f"CREATE TABLE {self.table_name} LIKE NGrama;")
        self.conn.commit()
        print("tabela criada com sucesso.")
        self.cursor.close()
        self.conn.close()

    def execute_sql_file(self, sql_filename):
        try:
            # Abra o arquivo SQL
            with open(sql_filename, 'r') as arquivo:
                sql = arquivo.read()
            self.cursor.execute(sql)
            self.conn.commit()            
            print("Instruções SQL executadas com sucesso.")
        except Exception as e:
            print(f"Ocorreu um erro ao executar o SQL: {str(e)}")

class DatabaseCredentials:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password

    @classmethod
    def from_config_file(cls, filename, item):
        with open(filename, 'r') as json_file:
            config_data = json.load(json_file)
            db_config = config_data.get(item, {})
            return cls(
                db_config.get('host', ''),
                db_config.get('user', ''),
                db_config.get('password', '')
            )

class DateManager:
    def __init__(self, days):
        self.days = days

    def get_current_date(self):
        return datetime.now()

    def get_previous_date(self):
        previous_date = datetime.now() - timedelta(days=self.days)
        return previous_date
    
    def get_date_difference(self):
        current_date = datetime.now()
        previous_date = current_date - timedelta(days=self.days)
        difference = (current_date - previous_date).days
        return difference
    @staticmethod
    def add_one_day_to_date(previous_date,day):
        # Adicionar um dia
        next_day = previous_date + timedelta(day)
        return next_day.strftime("%Y-%m-%d")

class ResourceMonitor:
    def __init__(self):
        self.process = psutil.Process()

    def get_memory_usage(self):
        """Retorna o uso de memória em bytes."""
        return self.process.memory_info().rss

    def get_cpu_usage(self, interval=1):
        """Retorna o uso de CPU em porcentagem durante o intervalo especificado."""
        return psutil.cpu_percent(interval=interval)
