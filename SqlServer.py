"""Created on Sept 24, 2015
    @author: del20"""

class SqlServer:
    def __init__(self, host, port, db, user, password):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.connection = None

    def getConnection(self):
        if not self.connection:
            import ceODBC
            template = 'DRIVER={FreeTDS};Server=%s;Port=%s;Database=%s;UID=%s;PWD=%s;'
            conStr = template % (self.host, self.port, self.db, self.user, self.password)
            print conStr
            self.connection = ceODBC.connect(conStr)
        return self.connection
    
    def getFullyQualifyTableName(self, tableName):
        tableFullName = tableName
        if self.cdm:
            tableFullName = self.db + ".dbo." + tableName 
        return tableFullName
    
    def close(self):
        if (self.connection):
            self.connection.close()
    
    def escape(self, tableName):
        return '[%s]' %(tableName)
    