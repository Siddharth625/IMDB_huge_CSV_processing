import pymongo

client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.tv92yfj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

db = client.get_database('User_Info')
users = db.users
movies_collection = db.movies
movies_mapping_collection = db.movie_mapping
date_added_collection = db.dateAddedSorted
duration_collection = db.durationSorted
release_year_collection = db.releaseYearSorted