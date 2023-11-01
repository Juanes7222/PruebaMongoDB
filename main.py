from pymongo import MongoClient, errors  #pip install pymongo
from pprint import pprint

def get_connection_database():
    try:
        # Provide the mongodb atlas url to connect python to mongodb using pymongo
        CONNECTION_STRING = "mongodb+srv://<username>:<password>@<database_name>.bwggemr.mongodb.net/" # Replace the values between <> with your proyect values
        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        client = MongoClient(CONNECTION_STRING)
        return client
  
    # return a friendly error if a URI error is thrown 
    except errors.ConfigurationError:
        raise Exception("An Invalid URI host error was received. Is your Atlas host name correct in your connection string?")
 
 

def create_database(field):
    db = get_connection_database()
    return db[field]

def get_collection_name(database, collection_name):
    return database[collection_name]

def insert_data(collection_name, data):
    try:
        collection_name.insert_many(data)
    except errors.BulkWriteError:
        print("duplicate key error collection")

def get_data_from_collection(collection, field=None):
    result = collection.find(field)
    return result

def update_collection(collection, identifier, new_data):
    # UPDATE A DOCUMENT
    #
    # You can update a single document or multiple documents in a single call.
    # 
    # Here we update the prep_time value on the document we just found.
    #
    # Note the 'new=True' option: if omitted, find_one_and_update returns the
    # original document instead of the updated one.

    my_doc = collection.find_one_and_update(identifier, {"$set": new_data}, new=True)
    if my_doc is not None:
        print("Here's the updated recipe:")
        print(my_doc)
    else:
        print(f"I didn't find any recipes that contain {identifier}.")
        print("\n")

def delete_data(collection, *data):
    # DELETE DOCUMENTS
    #
    # As with other CRUD methods, you can delete a single document 
    # or all documents that match a specified filter. To delete all 
    # of the documents in a collection, pass an empty filter to 
    # the delete_many() method. 
    #
    # The query filter passed to delete_many uses $or to look for documents
    # in which the passed data.

    my_result = collection.delete_many({ "$or": [*data]})
    print(f"I deleted {my_result.deleted_count} records.")
    print("\n")

def print_data(collection, field=None):
    for doc in get_data_from_collection(collection, field):
        pprint(doc)

        # print(f"id: {doc.get('_id')} name: {doc.get('item_name')}")


if __name__ == "__main__":   
  
    # Get the database
    db = create_database("prueba")
    # Get collection name
    collection_name = get_collection_name(db, "user_1_items")
    
    # Create registers
    item_1 = {
    "_id" : "U1IT00001",
    "item_name" : "Blender",
    "max_discount" : "10%",
    "batch_number" : "RR450020FRG",
    "price" : 340,
    "category" : "kitchen appliance"
    }

    item_2 = {
    "_id" : "U1IT00002",
    "item_name" : "Egg",
    "category" : "food",
    "quantity" : 12,
    "price" : 36,
    "item_description" : "brown country eggs"
    }
    
    # Insert registers on the database
    insert_data(collection_name, [item_1, item_2])
    
    # Verify the database content
    print_data(collection_name)
    
    # Update item name where the id is U1IT00002
    update_collection(collection_name, {"_id": "U1IT00002"}, {"item_name": "Butter"})
    
    # Verify the database content
    print_data(collection_name)
    
    # Delete the data where item name is Butter
    delete_data(collection_name, {"item_name": "Butter"})
    
    # Verify the database content
    print_data(collection_name)
    
    
