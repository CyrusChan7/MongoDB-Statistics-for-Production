import pymongo
import mongo_collection_strings
import os


def change_to_current_working_directory():
    dir_path = os.path.dirname(os.path.abspath(__file__))   # Project root
    #print(f"DEBUG: dir_path is {dir_path}")
    os.chdir(dir_path)

change_to_current_working_directory()


def read_file_contents(filename):
    with open(filename, "r") as f:
        file_content = f.read()
    #print(file_content)
    return file_content

collections_of_interest = read_file_contents("collections_to_check.txt")
#print(collections_of_interest)



connection = pymongo.MongoClient(mongo_collection_strings.cadc_server_three)

db_names = connection.list_database_names()
#print(db_names)


for dbname in db_names:
    db = connection[dbname]
    collection_names = db.list_collection_names()
    #print(collection_names)        # Collection names are in lists - for loop required 

    for i in range(len(collection_names)):
        #print(collection_names[i])


        if collection_names[i] in collections_of_interest:
            collection_stats = db.command("collstats", collection_names[i])
            #print(collection_stats)

            for key, value in collection_stats.items():
                if key == "count":
                    #print(key, value)
                    print(f"The count for {collection_names[i]} collection is {value:,}")

        


connection.close()
