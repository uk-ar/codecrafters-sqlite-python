import sys

from dataclasses import dataclass
from struct import unpack

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

def read_varint(file,n):
    var = int.from_bytes(file.read(1), byteorder="big")
    ans = var & 0x7f
    n-=1
    while (var>>7) & 1:
    #ans=0
    #for var in bytes:
        #print(var,(var>>7) & 1,bin(var))
        var = int.from_bytes(file.read(1), byteorder="big")
        ans = (ans<<7)+(var & 0x7f)
        #print(var,(var>>7) & 1,bin(var))
        n-=1
        #if not (var>>7):
        #    break
        #ans |= (var&0x7f) << shift
        #shift +=6
    return ans,n

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        # You can use print statements as follows for debugging, they'll be visible when running tests.
        print("Logs from your program will appear here!")

        # Uncomment this to pass the first stage
        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        print(f"database page size: {page_size}")
        database_file.seek(100)  # Skip the first 100 bytes of the header
        type,freeblock,num_cells,cell_start,num_fragment = unpack("!BHHHB",database_file.read(8))
        #type = int.from_bytes(database_file.read(1), byteorder="big")
        print(f"type: {hex(type)}")
        print(f"number of tables: {num_cells}")
        exit(0)
        print(hex(cell_start))#0xec3
        offsets = [int.from_bytes(database_file.read(2), byteorder="big") for _ in range(num_cells)]
        for offset in offsets:            
            contents_sizes=[0,1,2,3,4,6,8,8,0,0]
            database_file.seek(offset)
            num_payload = int.from_bytes(database_file.read(1), byteorder="big")
            row_id = int.from_bytes(database_file.read(1), byteorder="big")            
            payload = num_payload-2
            num_bytes = int.from_bytes(database_file.read(1), byteorder="big")-1 # -1 for self
            print(hex(offset),num_payload,row_id,num_bytes)
            sizes=[]
            while num_bytes > 0:
            #for _ in range(5):
                type,num_bytes = read_varint(database_file,num_bytes)
                #type = int.from_bytes(database_file.read(1), byteorder="big")
                print(type)
                if type >= 13 and type%2:
                    sizes.append((type-13)//2)
                elif type>=12 and type%2==0:
                    sizes.append((type-12)//2)
                else:
                    sizes.append(contents_sizes[type])
                #content = int.from_bytes(database_file.read(contents_sizes[type]), byteorder="big")
                #print(type,content)
            # 0xf8f,0xf3d,0xec3
            schema = [database_file.read(size) for size in sizes]
            if schema[0]==b"table":
                print(schema[2].decode('utf-8'))#table_name
                pass
            #for size in sizes:
            #    print()
        #type = int.from_bytes(database_file.read(100), byteorder="big")
elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        #print(f"database page size: {page_size}")
        database_file.seek(100)  # Skip the first 100 bytes of the header
        type,freeblock,num_cells,cell_start,num_fragment = unpack("!BHHHB",database_file.read(8))
        #type = int.from_bytes(database_file.read(1), byteorder="big")
        #print(f"type: {hex(type)}")
        #print(f"number of tables: {num_cells}")
        #print(hex(cell_start))#0xec3
        offsets = [int.from_bytes(database_file.read(2), byteorder="big") for _ in range(num_cells)]
        for offset in offsets:     
            contents_sizes=[0,1,2,3,4,6,8,8,0,0]
            database_file.seek(offset)
            num_payload = int.from_bytes(database_file.read(1), byteorder="big")
            row_id = int.from_bytes(database_file.read(1), byteorder="big")            
            payload = num_payload-2
            num_bytes = int.from_bytes(database_file.read(1), byteorder="big")-1 # -1 for self
            #print(hex(offset),num_payload,row_id,num_bytes)
            sizes=[]
            while num_bytes > 0:
            #for _ in range(5):
                type,num_bytes = read_varint(database_file,num_bytes)
                #type = int.from_bytes(database_file.read(1), byteorder="big")
                #print(type)
                if type >= 13 and type%2:
                    sizes.append((type-13)//2)
                elif type>=12 and type%2==0:
                    sizes.append((type-12)//2)
                else:
                    sizes.append(contents_sizes[type])
                #content = int.from_bytes(database_file.read(contents_sizes[type]), byteorder="big")
                #print(type,content)
            # 0xf8f,0xf3d,0xec3
            schema = [database_file.read(size) for size in sizes]
            if schema[0]==b"table":
                print(schema[2].decode('utf-8'))#table_name
else:
    print(f"Invalid command: {command}")