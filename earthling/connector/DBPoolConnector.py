import os, sys, yaml
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from psycopg2 import OperationalError

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

class DBPoolConnector:
	_instance = None
	db_address = 'localhost'
	# username = 'user'
	# password = 'pass'
	# db_name = ''
	# auto_commit = True

	def __init__(self):
		self.engine = None

	@classmethod
	def getInstance(cls):
		if cls._instance is None:
				cls._instance = DBPoolConnector()
		return cls._instance

	def getDBOption(self, name):
		# 프로젝트 루트 디렉토리 찾기 (earthling 폴더의 상위 디렉토리)
		current_dir = os.path.dirname(os.path.abspath(__file__))
		# earthling/connector에서 프로젝트 루트로 이동 (2단계 상위)
		root_dir = os.path.dirname(os.path.dirname(current_dir))

		with open(os.path.join(root_dir, 'earth-compose.yml')) as f:
			db_option = yaml.load(f, Loader=yaml.FullLoader)
			db_url = db_option['db'][name]
			
			# PostgreSQL URL 형태인 경우 직접 저장
			# if isinstance(db_url, str) and db_url.startswith('postgresql://'):
			self.connection_string = db_url
			self.auto_commit = True  # 기본값
   
			print(self.connection_string)
			# else:
			# 	# 기존 방식 (개별 필드)
			# 	self.db_address = db_url['address']
			# 	self.db_name = db_url['name']
			# 	self.username = db_url['username']
			# 	self.password = db_url['password']
			# 	self.auto_commit = db_url['auto_commit']
			# 	self.connection_string = None

	def getPool(self):
		# 직접 연결 문자열이 있으면 사용, 없으면 구성
		# if hasattr(self, 'connection_string') and self.connection_string:
		# 	connection_string = self.connection_string
		# else:
		# 	connection_string = f"postgresql+psycopg2://{self.username}:{self.password}@{self.db_address}/{self.db_name}"
		
		self.engine = create_engine(
			self.connection_string,
			poolclass=QueuePool,
			pool_size=5,
			max_overflow=10,
			pool_timeout=30
		)
  
		# print(self.engine)	
  
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
 
	# result = conn.execute(text("SELECT version()"))
	# version = result.fetchone()[0]
	# print(f"✅ SQLAlchemy 연결 성공!")
	# print(f"PostgreSQL 버전: {version}")
 
	try:
		result_set = conn.execute(text(query))
		
		# 쿼리 타입 확인 (SELECT인지 UPDATE/INSERT/DELETE인지)
		query_type = query.strip().upper().split()[0]
		
		if query_type == 'SELECT':
			# SELECT 쿼리인 경우에만 fetchall() 호출
			rows = result_set.fetchall()
			columns = list(result_set.keys())
			result = {'rows': rows, 'columns': columns}
		else:
			# UPDATE/INSERT/DELETE 쿼리인 경우
			rowcount = result_set.rowcount  # 영향받은 행 수
			result = {'rows': [], 'columns': [], 'rowcount': rowcount}
		
		if pool.auto_commit:
			conn.commit()
		
		return result
	except Exception as err:
		print(f"Query error: {err}")
		raise
	finally:
		pool.releasePool(conn)