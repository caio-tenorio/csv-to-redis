import csv, redis, json
import sys

REDIS_HOST = 'localhost'

def read_csv_data(csv_file):
    with open(csv_file, encoding='utf-8') as csvf:
        csv_data = csv.reader(csvf)
        return [(r) for r in csv_data]

def store_data(conn, data, key_index):
    if type(key_index) != list:
        for i in data:
            conn.setnx(i[int(key_index)], i)    
    else:
        for line in data:
            key = ""
            for c in key_index:
                key = key + line[int(c)]
            conn.setnx(key, line)
    return data        

def main():
    args_len = len(sys.argv)
    if args_len < 2:
        sys.exit(
            "Usage: %s file.csv key key key..."
            % __file__)
    elif args_len == 2:
        key_index = sys.argv[2]
    else:
        key_index = []
        for i in range (args_len):
            if i != 0 and i != 1:
                key_index.append(sys.argv[i])
    
    data = read_csv_data(sys.argv[1])
    conn = redis.Redis(REDIS_HOST)
    print (json.dumps(store_data(conn, data, key_index)))

if '__main__' == __name__:
    main()