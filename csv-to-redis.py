import csv, redis, json
import sys

REDIS_HOST = '172.17.0.2'
ORGAOS = "orgaos-II"
SUGESTAO = "sugestao-I"
TURMAS_COMPONETES = "turmas-componentes-III"

def read_csv_data(csv_file):
    with open(csv_file, encoding='utf-8') as csvf:
        csv_data = csv.reader(csvf)
        return [(r) for r in csv_data]

def store_data(conn, data, key_index, db_name):
    if type(key_index) != list:
        for line in data:
            key = db_name + "_" + line[int(key_index)]
            conn.setnx(key, line)    
    else:
        for line in data:
            key = db_name + "_"
            for c in key_index:
                key = key + line[int(c)]
            conn.setnx(key, line)
    return data        

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
    if args_len < 3:
        sys.exit("Utilize %s path/to/file.csv key_index key_index key_index..." % __file__)
    elif args_len == 3:
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
        conn = redis.Redis(REDIS_HOST)
        entry_data = (json.dumps(store_data(conn, data, key_index, db_name)))
        print ("Dados registrados com sucesso!")
        pass
    except Exception as e:
        raise print("Ocorreu um problema...")
    

if '__main__' == __name__:
    main()