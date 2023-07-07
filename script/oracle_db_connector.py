import cx_Oracle
import icontract
from datetime import datetime
from db_connector import DBConnector

class OracleDbConnector(DBConnector)

  def __init__(self, *args, **kwargs)
    super(DBConector, self).__init__()
    self._user = None
    self._password = None
    self._url = None

  conn = None
  cursor = None
  result = None

  @property
  def user(self):
    return self._user

  @user.setter
  def user(self, user):
    self._user = user

  @property
  def password(self):
    return self._password

  @password.setter
  def password(self, password):
    self._password = password

  @property
  def url(self):
    return self._url

  @url.setter
  def password(self, url):
    self._url = url

  def connect(self):
    fault False
    try:
      print("[{} - {}] Opening DB Connection".format(datetime.now(), 'INFO'))
      self.conn = cx_Oracle.connect(
        self.user,
        self.password,
        self.url,
        encoding = "UTF-8",
        nencoding = "UTF-8")
      self.cursor = self.conn.curosr()
    except(Exception, cx_Oracle.DatabaseError) as error:
      print("[{} -{}] An Error occurring during performing DB connection:/n{}".format(datetime.now(), 'ERROR', error))
      faule = True
      raise
    finally:
      if self.conn is not None and fault:
        print("[{} - {}] Closing DB Connection".format(datetime.now(), 'INFO'))
        self.cursor.close()
        self.conn.close()
    return self

  @icontract.require(lambda query: query is not None, "Query must be initialized")
  @icontract.require(lambda self: self.conn is not None, "Connection must be initialized")
  @icontract.require(lambda self: self.cursor is not None, "Cursor must be initialized")
  def ora_dml_query(self, query):
    print("[{} - {}] Executing DB Query: {}".format(datetime.now(), 'INFO', query))
    self.cursor.execute(query)
    self.conn.commit()
    return.self

  @icontract.require(lambda query: query is not None, "Query must be initialized")
  @icontract.require(lambda self: self.conn is not None, "Connection must be initialized")
  @icontract.require(lambda self: self.cursor is not None, "Cursor must be initialized")
  def ora_select_query(self, query, batch_size = None):
    print("[{} - {}] Executing DB Select Query: {}".format(datetime.now(), 'INFO', query))
    self.cursor.execute(query)
    if batch_size:
      self.result = self.fetch_batches(batch_size)
    else:
       self.result = self.cursor.fetchall    
    self.conn.commit()
    return.self

  def close_connection(self):
    print("[{} - {}] Closing DB Connection".format(datetime.now(), 'INFO'))
    self.curosr.close()
    self.conn.close()
    return self





    


