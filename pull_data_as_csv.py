import logging
import os

import pandas as pd
import sqlalchemy as sql
from tqdm import tqdm

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def create_folder_if_not_exists(path):
	if not os.path.exists(path):
		os.makedirs(path)
	return path
 
class _variables:
	mysql_connector = "mysql+mysqlconnector"
	ip_address = '127.0.0.1:55555'
	db_name = 'rdp_jfc4'
	user_name =  "root"
	user_mdp =  ""
	
	OUTPUTS_FOLDER = create_folder_if_not_exists(r'./outputs')


class DataCSVGetter():
	
	_vars = _variables()

	def get_connection(self):
		url = f'{self._vars.mysql_connector}://{self._vars.user_name}:{self._vars.user_mdp}@{self._vars.ip_address}/{self._vars.db_name}'
		engine = sql.create_engine(url)
		connection = engine.connect()
		return connection
		
		
	def get_list_tables(self):
		connection = self.get_connection()
		logging.info(f'Getting list tables for dataBase {self._vars.db_name}')
		query = f"SELECT DISTINCT TABLE_NAME, COLUMN_NAME\
    				FROM INFORMATION_SCHEMA.COLUMNS\
        			WHERE TABLE_SCHEMA='{self._vars.db_name}'"

		ResultProxy = connection.execute(query)
		Results = ResultProxy.fetchall()
		tables = pd.DataFrame(Results, columns=['table', 'id_col'])
		return tables
		
	
	def get_all_tables(self):
		dict_dfs = dict()
		connection = self.get_connection()
		list_tables = self.get_list_tables()
		list_tables = list_tables['table'].unique()
		for table in tqdm(list_tables):
			query = "SELECT * FROM %s " % (table)
			ResultProxy = connection.execute(query)
			Results = ResultProxy.fetchall()
			dict_dfs[table] = pd.DataFrame(Results, columns=list(ResultProxy._metadata.keys))
			
		return dict_dfs
    	   
	def add_list_tables_to_outputs_folder(self):	
 
		list_tables = self.get_list_tables()
		list_tables.to_csv(f'{self._vars.OUTPUTS_FOLDER}/list_tables.csv', index=False)
			
 
	def add_all_tables_to_ouputs(self):
		dict_dfs = self.get_all_tables()
		for table_key in tqdm(dict_dfs.keys()):
			 dict_dfs[table_key].to_csv(os.path.join(self._vars.OUTPUTS_FOLDER, 
                                            table_key + '.csv'), index=False)
	   
	def run_all(self):
		self.add_list_tables_to_outputs_folder()
		self.add_all_tables_to_ouputs()
     
if __name__ == "__main__":
	DataCSVGetter().run_all()
		
	
