from tasks import DatabaseQuery,DateManager,DatabaseCredentials
import json
from datetime import datetime, timedelta
import pandas as pd
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from api_model.nlsuper import NlExtractorProcess
from api_model.nlvisualization import NlVisualization
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_rows', None)
import findspark
findspark.init()
import sys
import mysql.connector
import mysql
from datetime import datetime as dt
from datetime import timedelta
import numpy as np

class DataProcessor:
    def __init__(self,Servicos, Credentials, date_difference, ini_date):
        self.Servicos = Servicos
        self.Credentials = Credentials
        self.data_difference = date_difference
        self.ini_date = ini_date
    def process_service(self,Servicos, ini, instancia, periodo):
        print(ini)
        result = ExtractData(Servicos, self.Credentials, ini, instancia, periodo)
        if result is None:
            return
        TableStaging(self.Credentials)
        df = GenerateNgram(Servicos, result,instancia,ini)
        InsertStaging(self.Credentials, Servicos, df)
        insertUploadNgrama(self.Credentials)

    def process_data(self, periodo):
        for dias in range(0,date_difference, periodo):
            ini = self.ini_date.strftime("%Y-%m-%d")

            for Service in self.Servicos.values():
                if Service.get('database') == 'nexidia_export':
                    for instancia in Service.get('instancias'):
                        self.process_service(Service, ini, instancia, periodo)
                else:
                    instancia = Service.get('instancias')
                    self.process_service(Service, ini, instancia, periodo)
            self.ini_date += timedelta(days=periodo)
Servicos = {"Nexidia":{"database":"nexidia_export","instancias":[2,1],"sistemaLegado":1,"id":"SourceMediaID","text":"Phrase","interlocutor":"Party",
                       "dtText":"StartOffset","dtSTT":"DiscoveredDate"}
            #"CPQD":{"database":"cpqd_export","instancias":3,"sistemaLegado":2,"id":"InteractionId","text":"Text","interlocutor":"Speaker",
            #           "dtText":"DataInsercao","dtSTT":"CreatedAt"},
            #"BoomAI":{"database":"boomai_export","instancias":4,"sistemaLegado":3,"id":"InteractionId","text":"Text","interlocutor":"Speaker",
            #           "dtText":"DataInsercao","dtSTT":"CreatedAt"}
                       }
def ExtractData(Service,Credentials,ini,instancia,periodo):    
    Credentials = DatabaseQuery(
        Credentials.host,
        Credentials.user,
        Credentials.password,
        Service.get('database'),
        ""
    )
    with open(r"C:\Users\Ade\Nuvem-de-Palavras\sql\select.sql", "r") as sql_file:
        query_template  = sql_file.read()
    query = query_template.format(id=Service.get('id'),
                                  text=Service.get('text'),
                                  interlocutor = Service.get('interlocutor'),
                                  dtText = Service.get('dtText'),
                                  dtSTT = Service.get('dtSTT'),
                                  instancias = instancia,
                                  ini = ini,
                                  end = date_manager.add_one_day_to_date(datetime.strptime(ini, "%Y-%m-%d"),periodo)                                  
                                  )
    # Executando a consulta e obtendo as informações
    column_names,result = Credentials.execute_query(query)
    if len(result) < 1:
        return result
    receita_df = pd.DataFrame(result)
    receita_df.columns = column_names
    Credentials.close_connection()
    return receita_df
def TableStaging(Credentials):
    Credentials = DatabaseQuery(
        Credentials.host,
        Credentials.user,
        Credentials.password,
        'myra_data',
        "_NGrama"
    )
    #Cria a tabela Staginng que servirá de bucket até update das infos na NGRAMA
    Credentials.CriarStaging()
def GenerateNgram(Service,result,ini,instancia):
    filename= f"teste_{Service.get('database')}_{instancia}_{ini}"
    prefix = 'csv'
    prefix_sep = ';'
    column_text = Service.get('text')
    whats_process = 'partial'
    id_database = Service.get('id') 
    type_find = 'aproximado' 
    activate_stopwords = 'sim'
    encoding = 'UTF-8'
    text_finds = {}
    additional_stop_words = []
    if Service.get('sistemaLegado') == 1:
        interlocutor = {'Party': ['AGENT', 'COSTUMER','UNKNOWN']}
    else:
        interlocutor = {'Speaker': ['1', '2']} 
    response_time = Service.get('dtText') 
    format_data = '%d/%m/%Y %H:%M:%S|%d/%m/%Y %H:%M|%Y-%m-%d %H:%M:%S|%d-%m-%Y %H:%M|%d%m%Y %H:%M:%S|%d%b%Y:%H:%M:%S' # 03MAR2022:12:01:33
    df = NlExtractorProcess.call_process(filename,prefix,prefix_sep,result,column_text,
        whats_process,
        text_finds,
        id_database,
        type_find,
        additional_stop_words,
        activate_stopwords,
        interlocutor,
        response_time,
        format_data,
        encoding)
    return df
def InsertStaging(Credentials,Service,df): 
    if Service.get('sistemaLegado') == 1:
        data = [(row["Instancia"], row["issue_id"], datetime.strptime(row["DiscoveredDate"], '%Y-%m-%d %H:%M:%S'), row["countent_word"],row["countent_bigram"], row["countent_trigram"], datetime.now()) for i, row in df.iterrows()]
    else:
        data = [(row["Instancia"], row["issue_id"], datetime.strptime(row["CreatedAt"], '%Y-%m-%d %H:%M:%S'), row["countent_word"],row["countent_bigram"], row["countent_trigram"], datetime.now()) for i, row in df.iterrows()]
    with open(r"C:\Users\Ade\Nuvem-de-Palavras\sql\insert.sql", "r") as sql_file:
        query_template  = sql_file.read()
    Credentials = DatabaseQuery(
        Credentials.host,
        Credentials.user,
        Credentials.password,
        'myra_data',
        "_NGrama"
    )
    
    queries = []
    for data_item in data:       
        query = query_template.format(
            SistemaLegado=Service.get('sistemaLegado'),
            Instancia=data_item[0],
            InteractionId=data_item[1],
            DataIntegracao=data_item[2],
            Unigrama=data_item[3],
            Bigrama=data_item[4],
            Trigrama=data_item[5],
            DataAlteracao=data_item[6].strftime("%Y-%m-%d %H:%M:%S")
        )
        queries.append(query)
    
    grupos_de_queries = [queries[i:i + 100] for i in range(0, len(queries), 100)]
    for grupo in grupos_de_queries:
        Credentials.executemany_query(grupo)            
    Credentials.close_connection()  
def insertUploadNgrama(Credentials):    
    Credentials = DatabaseQuery(
        Credentials.host,
        Credentials.user,
        Credentials.password,
        'myra_data',
        "_NGrama"
    )
    Credentials.execute_sql_file(r"C:\Users\Ade\Nuvem-de-Palavras\sql\insertNews.sql")
    Credentials.execute_sql_file(r"C:\Users\Ade\Nuvem-de-Palavras\sql\update.sql")
    Credentials.execute_q("DROP TABLE `myra_data`._NGrama")
    Credentials.close_connection()

config_filename = r'E:\DEV\Mutant\NuvemPalavras\DB.json'
#Abre arquivo de CONFIG
Credentials = DatabaseCredentials.from_config_file(config_filename,item='Nexidia')

periodo = 7 #Periodo a ser executado a consulta
date_manager = DateManager(days = 18) 
current_date, previous_date = date_manager.get_current_date(), date_manager.get_previous_date()
date_difference = date_manager.get_date_difference()
ini_date = previous_date
Processador = DataProcessor(Servicos,Credentials,date_difference,ini_date)
Processador.process_data(periodo)



    
    
