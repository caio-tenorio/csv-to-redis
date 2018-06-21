import csv, redis, json
import sys

REDIS_HOST = 'localhost'
ORGAOS = "orgaos-II"
SUGESTAO = "sugestao-I"
TURMAS_COMPONETES = "turmas-componentes-III"

def read_csv_data(csv_file):
    with open(csv_file, encoding='utf-8') as csvf:
        csv_data = csv.reader(csvf)
        return [(r) for r in csv_data]

def build_key(key_index, db_name, line):
    if type(key_index) != list:
        key = db_name + "_" + line[int(key_index)]
    else:
        key = db_name + "_"
        for c in key_index:
            key = key + line[int(c)]
    return key 

def store_data(conn, data, key_index, db_name):
    dict_to_store = {}
    _id = 1
    header = data[0]
    for line in data:
        key = build_key(key_index, db_name, line)
        count = 0
        dict_to_store["key"] = key
        for attribute in line:
            dict_to_store[header[count]] = attribute
            count = count + 1
        dict_to_redis_hset(conn, ('testando:' + str(_id)), dict_to_store)
        _id = _id + 1
    print (dict_to_store) 
    return data        

def dict_to_redis_hset(r, hkey, dict_to_store):
    return all([r.hset(hkey, k, v) for k, v in dict_to_store.items()])

def which_db(csv_file):
    db = ""
    if csv_file == ORGAOS:
        db = "orgaos"
    elif csv_file == SUGESTAO:
        db = "sugestao"
    elif csv_file == TURMAS_COMPONETES:
        db = "turmas-componentes"
    return db

def main():
    args_len = len(sys.argv)
    if args_len < 2:
        sys.exit("Utilize %s path/to/file.csv key_index key_index key_index..." % __file__)
    elif args_len == 2:
        key_index = sys.argv[2]
    else:
        key_index = []
        for i in range (args_len):
            if i != 0 and i != 1:
                key_index.append(sys.argv[i])
    
    csv_file = sys.argv[1].split("/")[-1][:-4]
    db_name = which_db(csv_file)

    try:
        data = read_csv_data(sys.argv[1])
        conn = redis.StrictRedis(host=REDIS_HOST)
        entry_data = (json.dumps(store_data(conn, data, key_index, db_name)))
        print ("Dados registrados com sucesso!")
        pass
    except Exception as e:
        raise print("Ocorreu um problema...")
    

if '__main__' == __name__:
    main()