import pymongo
from mongo_collection_strings import cadc_server_one, cadc_server_two, cadc_server_three, cadc_server_four, cadc_server_five
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


servers = [cadc_server_one, cadc_server_two, cadc_server_three, cadc_server_four, cadc_server_five]

for server in servers:
    connection = pymongo.MongoClient(server)

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

                print(f"\n{collection_names[i]}:")
                for key, value in collection_stats.items():
                    #print(key, value)
                    if key == "count":
                        print(f"{value:,} document(s)")
                    elif key == "size":
                        print(f"size: {value:,} B")
                    elif key == "storageSize":
                        print(f"storageSize: {value:,} B")


connection.close()
