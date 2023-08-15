import sys

from dataclasses import dataclass, field
from struct import unpack
import io
import sqlparse  # - available if you need it!
import operator

database_path = sys.argv[1]
command = sys.argv[2]

def read_varint(file):
    var = int.from_bytes(file.read(1), byteorder="big")
    ans = var & 0x7f
    n = 1
    while (var >> 7) & 1:
        var = int.from_bytes(file.read(1), byteorder="big")
        ans = (ans << 7)+(var & 0x7f)
        n += 1
    return ans, n

class Database:
    def __init__(self, database_path):
        self.database_path = database_path
        self.database = open(database_path, "rb")
        self.database.seek(16)  # Skip the first 16 bytes of the header
        self.page_size = int.from_bytes(
            self.database.read(2), byteorder="big")
        self.schema_table = self.get_page(1)
        # self.tables = self.get_tables(self.schema_table)

    def __del__(self):
        self.database.close()

    def seek(self,size):
        return self.database.seek(size)

    def read(self,size):
        return self.database.read(size)
    
    def get_page(self, num):
        if num == 1:
            self.database.seek(100)
        else:
            self.database.seek(self.page_size*(num-1))
        type = int.from_bytes(self.database.read(1), byteorder="big")
        if type == 0x0d:
            return TableLeaf(self, type, self.page_size*(num-1))
        elif type == 0x05:
            return TableInterior(self, type, self.page_size*(num-1))
        else:
            raise ValueError("error!")        

    def get_table(self,tbl_name):
        return self.get_tables()[tbl_name]

    def get_tables(self):
        pages = {}  # tbl_name->root_page
        for type,name,tbl_name,rootpage,sql in self.schema_table.get_cells():
            if not tbl_name in pages:
                pages[tbl_name]={}
            pages[tbl_name][type] = Table(type,name,tbl_name,rootpage,sql,self.get_page(rootpage))
            # print(pages[name])
        return pages

@dataclass
class Page:
    database: Database
    type: bytes
    offset: int
    freeblock: bytes = 0
    num_cells: bytes = 0
    cell_start: bytes = 0
    num_fragment: bytes = 0
    def __post_init__(self):
        self.freeblock, self.num_cells, self.cell_start, self.num_fragment = unpack(
            "!HHHB", self.database.read(7))

@dataclass
class TableInterior(Page):
    # table: Table
    right_most: int = 0
    offsets: list = field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()
        self.right_most = int.from_bytes(self.database.read(4), byteorder="big")
        self.offsets = [int.from_bytes(self.database.read(
            2), byteorder="big")+self.offset for _ in range(self.num_cells)]

    def get_cells(self):  # iter
        cells = []
        for offset in self.offsets:
            self.database.seek(offset)
            left_page = int.from_bytes(self.database.read(4), byteorder="big")
            row_id,_ = read_varint(self.database)
            cells.append([left_page,row_id])
        return cells

    def get_rows(self):
        ans = []
        for left_page,_ in self.get_cells():
            page = self.database.get_page(left_page)
            ans += page.get_rows()
        return ans

@dataclass
class TableLeaf(Page):
    right_most: int = 0
    offsets: list = field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()
        self.offsets = [int.from_bytes(self.database.read(
            2), byteorder="big")+self.offset for _ in range(self.num_cells)]

    def get_rows(self):
        return self.get_cells()

    def get_cells(self):  # iter
        contents_sizes = [0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0]
        cells = []
        for offset in self.offsets:
            self.database.seek(offset)
            num_payload, _ = read_varint(self.database)
            row_id, _ = read_varint(self.database)
            payload = num_payload-2  # for num_payload & row_id
            # varint?
            num_bytes,n  = read_varint(self.database)
            num_bytes -= n
            #print(hex(offset),num_payload,row_id,num_bytes,file=sys.stderr)
            types = []
            while num_bytes > 0:
                type, n = read_varint(self.database)
                num_bytes -= n
                types.append(type)
            #print(types,file=sys.stderr)
            cell = []
            for type in types:
                size = 0
                if type >= 13 and type % 2:
                    size = (type-13)//2
                    cell.append(self.database.read(size).decode("utf-8"))
                elif type >= 12 and type % 2 == 0:  # BLOB
                    size = (type-12)//2
                    cell.append(self.database.read(size))
                elif type == 0:
                    cell.append(row_id)
                    row_id += 1
                elif type <= 6:
                    size = contents_sizes[type]
                    cell.append(int.from_bytes(        
                        self.database.read(size), byteorder="big"))
                # elif type==7: # TODO:float
                else:
                    cell.append(self.database.read(size))
                # print(cell[-1])
            cells.append(cell)
        return cells

@dataclass
class Table:
    type: str
    name: str
    tbl_name: str
    rootpage: int
    sql: str    
    root: Page = None
    columns: dict = field(default_factory=dict)

    def __post_init__(self):
        statement = sqlparse.parse(self.sql)[0]
        for i, token in enumerate(statement.tokens[-1].get_sublists()):
            if type(token) is sqlparse.sql.IdentifierList:
                self.columns[[t.value for t in token.get_sublists()][-1]] = i
            else:
                self.columns[token.value] = i
    
    def get_rows(self):
        return self.root.get_rows()

db = Database(database_path)


def print_token(token):
    print(f"{type(token)}:{token.ttype}:{token.value}")
    [print_token(t) for t in token.get_sublists()] if token.is_group else None


if not command.startswith("."):
    statement = sqlparse.parse(command)[0]
    columns_token = []
    tbl_name = ""
    if statement[0].value.upper() == "SELECT":
        columns_token = statement[2]
        if statement[4].value.upper() == "FROM":
            tbl_name = statement[6].value
    print(db.get_tables(),file=sys.stderr)
    print(db.schema_table,file=sys.stderr)
    table = db.get_table(tbl_name)["table"]
    if columns_token.value == "count(*)":
        #print(table.root,file=sys.stderr)
        print(len(table.get_rows()))
        exit(0)
    columns = []
    if type(columns_token) == sqlparse.sql.Identifier:
        columns.append(columns_token.get_name())
    else:
        columns = [x.get_name() for x in columns_token.get_identifiers()]
    idxs = [table.columns[column] for column in columns]
    rows = []
    filter = []
    ops = {"=": operator.eq}
    if type(statement[-1]) == sqlparse.sql.Where:
        for comparison in statement[-1].get_sublists():
            filter.append(ops[comparison.tokens[2].value])
            if type(comparison.left) == sqlparse.sql.Identifier:
                filter.append(table.columns[comparison.left.value])
            if type(comparison.right) == sqlparse.sql.Token:
                filter.append(comparison.right.value[1:-1])
    #print(db.get_page(page_num).get_rows(),file=sys.stderr)
    for row in table.get_rows():
        if not filter:
            rows.append([row[idx] for idx in idxs])
            continue
        # print(filter)
        # TODO: stack machine base eval
        if filter[0](row[filter[1]], filter[2]):
            rows.append([row[idx] for idx in idxs])
    #print(rows)
    for row in rows:
        print("|".join([str(cell) for cell in row]))
elif command == ".dbinfo":
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    print(f"database page size: {db.page_size}")
    print(f"number of tables: {db.schema_table.num_cells}")
elif command == ".tables":
    for tbl_name in db.get_tables().keys():
        print(tbl_name)
else:
    print(f"Invalid command: {command}")
