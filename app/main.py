import sys

from dataclasses import dataclass
from struct import unpack

import sqlparse # - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

def read_varint(file,n):
    var = int.from_bytes(file.read(1), byteorder="big")
    ans = var & 0x7f
    n-=1
    while (var>>7) & 1:
        var = int.from_bytes(file.read(1), byteorder="big")
        ans = (ans<<7)+(var & 0x7f)
        n-=1
    return ans,n

#@dataclass
class Page:
    def __init__(self,database_file,offset):        
        self.database_file = database_file
        self.offset = offset
        self.type,self.freeblock,self.num_cells,self.cell_start,self.num_fragment = unpack("!BHHHB",database_file.read(8))
        self.offsets = [int.from_bytes(database_file.read(2), byteorder="big")+offset for _ in range(self.num_cells)]
    def get_cells(self):#iter
        contents_sizes=[0,1,2,3,4,6,8,8,0,0,0,0]
        cells = []
        for offset in self.offsets:            
            self.database_file.seek(offset)
            num_payload = int.from_bytes(self.database_file.read(1), byteorder="big")
            row_id = int.from_bytes(self.database_file.read(1), byteorder="big")            
            payload = num_payload-2 # for num_payload & row_id
            num_bytes = int.from_bytes(self.database_file.read(1), byteorder="big")-1 # -1 for self
            #print(hex(offset),num_payload,row_id,num_bytes)
            sizes=[]
            while num_bytes > 0:
                type,num_bytes = read_varint(self.database_file,num_bytes)
                #type = int.from_bytes(self.database_file.read(1), byteorder="big")
                #print(type,num_bytes)
                if type >= 13 and type%2:
                    sizes.append((type-13)//2)
                elif type>=12 and type%2==0:
                    sizes.append((type-12)//2)
                else:
                    sizes.append(contents_sizes[type])
                #content = int.from_bytes(self.database_file.read(contents_sizes[type]), byteorder="big")
                #print(type,content)
            # 0xf8f,0xf3d,0xec3
            cells.append([self.database_file.read(size) for size in sizes])
        return cells

class Table:
    def __init__(self,type_,name,tbl_name,rootpage,sql):
        self.type = type_
        self.name = name
        self.tbl_name = tbl_name
        self.rootpage = rootpage
        self.sql = sql
        self.columns = {}
        statement = sqlparse.parse(sql)[0]
        for i,token in enumerate(statement.tokens[-1].get_sublists()):
            if type(token) is sqlparse.sql.IdentifierList:
                self.columns[[t.value for t in token.get_sublists()][-1]]=i
            else:
                self.columns[token.value]=i

class Database:
    def __init__(self,database_file_path):
        self.database_file_path = database_file_path
        self.database_file = open(database_file_path, "rb")
        self.database_file.seek(16)  # Skip the first 16 bytes of the header
        self.page_size = int.from_bytes(self.database_file.read(2), byteorder="big")
        self.schema_table = self.get_page(1)
        self.tables = self.get_tables(self.schema_table)

    def __del__(self):
        self.database_file.close()

    def get_page(self,num):
        if num == 1:
            self.database_file.seek(100)
        else:
            self.database_file.seek(self.page_size*(num-1))
        page = Page(self.database_file,self.page_size*(num-1))
        return page

    def get_tables(self,schema_table):
        pages = {} # tbl_name->root_page
        for schema in schema_table.get_cells():
            if schema and schema[0]==b"table":
                pages[schema[2].decode('utf-8')] = Table(
                    schema[0].decode('utf-8'),
                    schema[1].decode('utf-8'),
                    schema[2].decode('utf-8'),
                    int.from_bytes(schema[3],'big'),
                    schema[4].decode('utf-8'))
        return pages

db = Database(database_file_path)

if not command.startswith("."):
    statement = sqlparse.parse(command)[0]
    columns = []
    table = ""
    if statement[0].value.upper() == "SELECT":
        columns = statement[2]
        if statement[4].value.upper() == "FROM":
            table  = statement[6].value
    print(db.tables,file=sys.stderr)
    print(db.schema_table,file=sys.stderr)
    page_num = db.tables[table].rootpage
    if columns.value == "count(*)":
        print(len(db.get_page(page_num).offsets))
    else:
        #print(db.get_page(page_num).offsets)        
        col = columns.value
        #print(db.tables[table].columns)
        idx = db.tables[table].columns[col]
        for cell in db.get_page(page_num).get_cells():
            print(cell[idx].decode("utf-8"))
elif command == ".dbinfo":
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage 
    print(f"database page size: {db.page_size}")
    print(f"number of tables: {db.schema_table.num_cells}")
elif command == ".tables":
    for table in db.tables.keys():
        print(table)
else:
    print(f"Invalid command: {command}")

