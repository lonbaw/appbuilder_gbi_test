from concurrent.futures import ThreadPoolExecutor
import mysql.connector
from mysql.connector import Error

class MySQLConnectionPool:
    def __init__(self, pool_size=5, **db_params):
        self.pool = ThreadPoolExecutor(max_workers=pool_size)
        self.db_params = db_params

    def execute_query(self, sql_query):
        future = self.pool.submit(self._run_query, sql_query)
        return future.result()

    def _run_query(self, sql_query):
        connection = None
        try:
            # 建立数据库连接
            connection = mysql.connector.connect(**self.db_params)
            if connection.is_connected():
                cursor = connection.cursor()
                cursor.execute(sql_query)
                records = cursor.fetchall()
                return records
        except Error as e:
            return f"Error while connecting to MySQL: {e}"
        finally:
            # 关闭数据库连接
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

# 数据库配置参数
db_config = {
    'host': '你的db_host',
    'port': '你的db_port', 
    'database': '你的db_数据库名',
    'user': '你的db_user',
    'password': '你的db_passwd'
}

# 创建一个线程池对象
mysql_connect_pool = MySQLConnectionPool(**db_config)

if __name__ == "__main__":
    # 使用线程池执行查询
    sql_query = "show create table dag_tag;" # 替换为你的查询
    result = mysql_connect_pool.execute_query(sql_query)
    print(result)