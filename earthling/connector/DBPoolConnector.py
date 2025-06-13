import os, sys, yaml
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from psycopg2 import OperationalError

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

class DBPoolConnector:
	_instance = None

	def __init__(self):
		self.engine = None

	@classmethod
	def getInstance(cls):
		if cls._instance is None:
				cls._instance = DBPoolConnector()
		return cls._instance

	def getDBOption(self, name):
		current_dir = os.path.dirname(os.path.abspath(__file__))
		root_dir = os.path.dirname(os.path.dirname(current_dir))

		with open(os.path.join(root_dir, 'earth-compose.yaml')) as f:
			db_option = yaml.load(f, Loader=yaml.FullLoader)
			db_url = db_option['db'][name]
			self.connection_string = db_url
			self.auto_commit = True
			
	def getPool(self):
		self.engine = create_engine(
			self.connection_string,
			poolclass=QueuePool,
			pool_size=5,
			max_overflow=10,
			pool_timeout=30
		)
		return self.engine

	def getConn(self):
		if self.engine is None:
			self.getPool()
		try:
			conn = self.engine.connect()
			return conn
		except Exception as err:
			print(f"Connection error: {err}")
			self.getPool()
			return self.engine.connect()

	def releasePool(self, conn):
		conn.close()

def execute(query, pool_connector):
	pool = pool_connector.getInstance()
	conn = pool.getConn()
 
	try:
		result_set = conn.execute(text(query))
		query_type = query.strip().upper().split()[0]
		
		if query_type == 'SELECT':
			rows = result_set.fetchall()
			columns = list(result_set.keys())
			result = {'rows': rows, 'columns': columns}
		else:
			rowcount = result_set.rowcount
			result = {'rows': [], 'columns': [], 'rowcount': rowcount}
		
		if pool.auto_commit:
			conn.commit()
		
		return result
	except Exception as err:
		print(f"Query error: {err}")
		raise
	finally:
		pool.releasePool(conn)