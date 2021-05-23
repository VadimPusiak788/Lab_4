import itertools
from pymongo import MongoClient
import csv 
from conf import DB

def create_collection():
    client = MongoClient(**DB)
    db = client.lab_4  

    if "zno_data" in db.list_collection_names():
        db.zno_data.delete_many({})
    else:
        db.zno_data
    db.counter_col



def insert_data(file):
    year = int(file[-12:-8])
    client = MongoClient(**DB)

    
    db = client.lab_4  
    col_zno = db.zno_data
    col_counter = db.counter_col
    counter  = col_counter.find_one({'year': year})
    if counter is None:
        counter = 0

        col_counter.insert_one({'year': year, 'counter': counter})
    else:
        counter = counter['counter']
    with open(file, encoding="cp1251") as create_file:
        spam = itertools.islice(csv.DictReader(create_file, delimiter=';'), int(counter), None)
        records = []
        if len(list(spam)) > 0:
            for data in spam:
                print(data)
                records.append(data)
                counter += 1
                data['year'] = year
                if counter % 40 == 0:
                    col_zno.insert_many(records)
                    records = []
                    col_counter.update_one({'year': year},
                        {
                            '$set':  {'counter': counter}
                        }
                    )
            
            col_zno.insert_many(records)
            col_counter.update_one({'year': year},
                        {
                            '$set':  {'counter': counter}
                        }
                    )



def write_data():
    client = MongoClient(**DB)
    
    db = client.lab_4  
    col_zno = db.zno_data

    query = col_zno.aggregate(
        [
            {"$match": {"EngTestStatus": "Зараховано"}},
            {"$group": {"_id": {"year": "$year", "regname": "$REGNAME"},
            "min": {"$min": "$engball100"}}
            },
        ]
    )


    with open('data_files/result.csv', mode='w') as file:
        fieldnames = ['Регіон', 'Мінімальний бал', 'Рік']

        result_writer = csv.writer(file, delimiter=',')
        result_writer.writerow(fieldnames)

        for row in query:
            result_writer.writerow(row)

if __name__== "__main__":
    create_collection()

    insert_data('data_files/Odata2019File.csv')
    insert_data('data_files/Odata2020File.csv')

    write_data()