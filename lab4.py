from pymongo import MongoClient
import csv
import datetime

url = input("MongoDB URL (default -- mongodb://localhost:27017): ")
url = url if url else MongoClient()
client = MongoClient(url)

# з'єднання з БД
db = client.db_zno_2019_2020

with open('time.txt', 'w') as logs_file:
    batch_size = 1000  # розмір однієї групи документів
    file_names = ["Odata2019File.csv", "Odata2020File.csv"]
    years = [2019, 2020]

    for j in range(2):
        file_name, year = file_names[j], years[j]
        with open(file_name, "r", encoding="cp1251") as csv_file:

            # записуємо час початку запису файлів
            start_time = datetime.datetime.now()
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            i = 0  # кількість вставлених документів з поточної групи
            batches_num = 0  # кількість вставлених груп
            document_bundle = []

            # знаходимо, скільки документів вже вставлено в колекцію
            num_inserted = db.inserted_docs.find_one({"year": year})
            if num_inserted == None:
                num_inserted = 0
            else:
                num_inserted = num_inserted["num_docs"]


            # читаємо csv-файл; кожен його рядок -- це словник
            for row in csv_reader:

                # якщо n документів вже записано, то перші n документів пропускаємо
                if batches_num * batch_size + i < num_inserted:
                    i += 1
                    if i == batch_size:
                        i = 0
                        batches_num += 1
                    continue

                document = row
                document['year'] = year
                document_bundle.append(document)
                i += 1
                # якщо назбиралось 1000 документів -- записуємо дані
                if i == batch_size:
                    i = 0
                    batches_num += 1
                    db.collection_zno_data.insert_many(document_bundle)
                    document_bundle = []
                    # також записуємо в окремий документ кількість вставлених рядків
                    if batches_num == 1:
                        db.inserted_docs.insert_one({"num_docs": batch_size, "year": year})
                    else:
                        db.inserted_docs.update_one({
                            "year": year, "num_docs": (batches_num - 1) * batch_size},
                            {"$inc": {
                                "num_docs": batch_size
                            }})
            # якщо файл скінчився, а група документів не повна -- записуємо її
            if i != 0 and document_bundle:
                db.inserted_docs.update_one({
                    "year": year, "num_docs": batches_num * batch_size},
                    {"$inc": {
                        "num_docs": i
                    }})
                db.collection_zno_data.insert_many(document_bundle)

            end_time = datetime.datetime.now()
            logs_file.write(str(end_time - start_time) + ", " + file_name + ' -- витрачено часу на завантаження файлу\n')


#статистичний запит і запис в csv-файл
header = ["Область","Максимальний бал в 2019", "Максимальний бал в 2020"]
query_results = db.collection_zno_data.aggregate([
    {
        "$match" : {
            "UkrTestStatus" :  'Зараховано'
        }
    },
    {
        "$group" : {
            "_id" : {
                "region" : "$REGNAME",
                "year" : "$year",
            },
            "UkrMaxResults": { "$max": "$UkrBall100" }
        }
    },
    {
        "$sort" : { "_id.region": 1, "_id.year": 1 }
    }])

data = []
query_results_list = list(query_results)
for i in range(len(query_results_list)):
    if i%2 == 0:
        row = [query_results_list[i]["_id"]["region"],query_results_list[i]["UkrMaxResults"],query_results_list[i+1]["UkrMaxResults"]]
        data.append(row)

with open('result4_ukr_language_and_literature.csv', 'w', encoding='utf-8') as res_file:
    result_writer = csv.writer(res_file, delimiter=';')
    result_writer.writerow(header)
    result_writer.writerows(data)