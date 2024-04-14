from flask import Flask, render_template, url_for, request, session, redirect, jsonify, make_response
from flask_uploads import UploadSet, configure_uploads, ALL
import bcrypt
import pymongo
import pandas as pd
import requests
from datetime import datetime
import csv
import re

app = Flask(__name__)
app.secret_key = "marrow"
client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.tv92yfj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

db = client.get_database('User_Info')
users = db.users
movies_collection = db.movies
movies_mapping_collection = db.movie_mapping
date_added_collection = db.dateAddedSorted
duration_collection = db.durationSorted
release_year_collection = db.releaseYearSorted

csv_files = UploadSet('csv', ALL)
app.config['UPLOADED_CSV_DEST'] = 'uploads'
configure_uploads(app, csv_files)

def DurationProcessing(final_document):
    data = final_document['Duration'].keys()
    pattern = r'\d+'
    numbers_dict = {}
    for string in data:
        matches = re.findall(pattern, string)
        numbers = [int(num) for num in matches]
        transformed_string = ' '.join(str(num) for num in numbers)
        numbers_dict[string] = [int(transformed_string), len(final_document["Duration"][string])]
    sorted_dict = dict(sorted(numbers_dict.items(), key=lambda x: x[1][0]))
    duration_collection.insert_many([sorted_dict])
    return "Successfully processed duration."

def DateAddedProcessing(final_document):
    data = final_document['Date_Added'].keys()
    datetime_dict = {}
    for string in data:
        datetime_obj = datetime.strptime(string, '%B %d, %Y')
        transformed_string = datetime_obj.strftime('%Y-%m-%d')
        datetime_dict[string] = [transformed_string, len(final_document["Date_Added"][string])]
    sorted_dict = dict(sorted(datetime_dict.items(), key=lambda x: x[1][0]))
    date_added_collection.insert_many([sorted_dict])
    return "Successfully processed date_added."

def ReleaseYearProcessing(final_document):
    data = final_document['Release_Year'].keys()
    int_dict = {}
    for string in data:
        transformed_string = int(string)
        int_dict[string] = [transformed_string, len(final_document["Release_Year"][string])]
    sorted_dict = dict(sorted(int_dict.items(), key=lambda x: x[1][0]))
    release_year_collection.insert_many([sorted_dict])
    return "Successfully processed release_year."

def Processor(reader):
    total_records = 0
    final_document = {}
    for i, chunk in enumerate(reader):
        chunk['release_year'] = chunk['release_year'].astype(str)
        total_records += len(chunk)
        # print(f'Processing chunk {i+1}, uploaded records: ', total_records)
        documents = chunk.to_dict(orient='records')
        Duration_grp = chunk.groupby('duration')['show_id'].apply(list).to_dict()
        final_document['Duration'] = Duration_grp
        Date_Added_grp = chunk.groupby('date_added')['show_id'].apply(list).to_dict()
        final_document['Date_Added'] = Date_Added_grp
        Release_year_grp = chunk.groupby('release_year')['show_id'].apply(list).to_dict()
        final_document['Release_Year'] = Release_year_grp
        movies_collection.insert_many(documents)
        requests.post('http://127.0.0.1:5000/process', json={'progress': i + 1})
    DurationProcessing(final_document)
    DateAddedProcessing(final_document)
    ReleaseYearProcessing(final_document)
    movies_mapping_collection.insert_many([final_document])
    return total_records


@app.route('/', methods=['POST','GET'])
def home():
    return redirect(url_for("login"))

@app.route('/login', methods=['POST','GET'])
def login():
    message = ''
    if request.method == "POST":
        username = request.form.get("fullname")
        email = request.form.get("email")
        password = request.form.get("password")
        email_found = users.find_one({"email": email})
        if email_found:
            db_password = users.find_one({"email": email},{"_id": 0, "password": 1})['password']
            if  bcrypt.checkpw(password.encode('utf-8'), db_password):
                session["email"] =  email
                return redirect(url_for("uploadCSV"))
            else:
                message = 'Password does not match'
            return render_template('logged_in.html', message=message)
        else:
            hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
            user_input = {'name': username, 'email': email, 'password': hashed}
            users.insert_one(user_input)
            message = 'Thanks for  registering!'
            return render_template('logged_in.html', message=message)
    else:
        return render_template("logged_in.html")
    

@app.route('/uploadCSV', methods=['GET', 'POST'])
def uploadCSV():
    if request.method == 'POST':
        if 'file' not in request.files:
            return render_template('upload_csv.html', message='No file part')
        file = request.files['file']
        if file.filename == '':
            return render_template('upload_csv.html', message='No selected file')
        chunk_size = 800  
        reader = pd.read_csv(file, chunksize=chunk_size)
        records = Processor(reader)
        return render_template('upload_csv.html', message=f'Processing finished, {records} uploaded successfully!')
    return render_template('upload_csv.html')

@app.route('/process', methods=['GET', 'POST'])
def process():
    if request.method == 'POST':
        progress = request.json.get('progress')
        print(f'Processing chunk {progress+1}')
    return render_template('upload_csv.html'  , progress = f'Processing chunk {progress+1}' )

def sortedFilterDF(collection):
    d = collection.find_one({},{'_id': 0})
    df = pd.DataFrame.from_dict(d, orient='index', columns=['Value', 'Count'])
    df.index.name = 'Index'
    return df

def create_index_ranges(df, start_index, end_index):
    result_dict = {}
    cumulative_count = 0
    for index, row in df.iterrows():
        value = row['Value']
        count = row['Count']
        if cumulative_count >= end_index:
            break
        if cumulative_count + count <= start_index:
            cumulative_count += count
            continue
        start_value = max(start_index - cumulative_count, 0)
        end_value = min(end_index - cumulative_count, count)
        result_dict[index] = [start_value, start_value + end_value]
        cumulative_count += count
    return result_dict


@app.route("/search", methods=['POST', "GET"])
def search():
    if request.method == "POST":
        colSort = request.form.get("Sort")
        orderSort = request.form.get("Order")
        PG_no = request.form.get("PageNumber")
        RecordsPerPage = request.form.get("Records")
        PG_no = int(PG_no)
        RecordsPerPage = int(RecordsPerPage)
        print(colSort, orderSort)
        print(type(PG_no), type(RecordsPerPage))
        if colSort == "duration":
            mapping_key = "Duration"
            if orderSort == "1":
                resdf = sortedFilterDF(duration_collection)
            elif orderSort == "-1":
                resdf = sortedFilterDF(duration_collection)
                resdf =  resdf[::-1]
        elif colSort == "date_added":
            mapping_key = "Date_Added"
            if orderSort == "1":
                resdf = sortedFilterDF(date_added_collection)
            elif orderSort == "-1":
                mapping_key = "Date_Added"
                resdf = sortedFilterDF(date_added_collection)
                resdf =  resdf[::-1]
        if colSort == "release_year":
            mapping_key = "Release_Year"
            if orderSort == "1":
                resdf = sortedFilterDF(release_year_collection)
            elif orderSort == "-1":
                resdf = sortedFilterDF(release_year_collection)
                resdf =  resdf[::-1]
        total_records = resdf['Count'].sum()
        print(total_records)
        start_index = (PG_no - 1) * RecordsPerPage
        end_index = min(start_index + RecordsPerPage, total_records)
        res_dict = create_index_ranges(resdf, start_index, end_index)
        print(res_dict)

        sliced_doc = []
        document = movies_mapping_collection.find_one()
        for key, idx in (res_dict.items()):
            print(key, idx)
            nested_doc = document[mapping_key][key] 
            sliced_doc.append(nested_doc[idx[0]:idx[1]])
        id_list = []
        for lst in sliced_doc:
            id_list += lst
        print(id_list)

        movie_query = {"show_id" : {"$in" : id_list }}
        page = movies_collection.find(movie_query)
        page = list(page)
        print(len(page))
    return render_template('upload_csv.html', pagination = page)


if __name__ == "__main__":
    app.run(debug=True)
