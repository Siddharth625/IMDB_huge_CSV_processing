from flask import Flask, render_template, url_for, request, session, redirect, jsonify, make_response
import bcrypt
import pandas as pd
from db_config import *

from backend.processor import Processor
processor = Processor()

from backend.pagination import Paginate
paginate = Paginate()

from app import app

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
        records = processor.ProcessUpload(reader, 
                      movies_collection, 
                      movies_mapping_collection,
                      date_added_collection,
                      duration_collection,
                      release_year_collection)
        return render_template('upload_csv.html', message=f'Processing finished, {records} uploaded successfully!')
    return render_template('upload_csv.html')

@app.route('/process', methods=['GET', 'POST'])
def process():
    if request.method == 'POST':
        progress = request.json.get('progress')
        print(f'Processing chunk {progress+1}')
    return render_template('upload_csv.html' , progress = f'Processing chunk {progress+1}' )


@app.route("/search", methods=['POST', "GET"])
def search():
    if request.method == "POST":
        colSort = request.form.get("Sort")
        orderSort = request.form.get("Order")
        PG_no = request.form.get("PageNumber")
        RecordsPerPage = request.form.get("Records")
        PG_no = int(PG_no)
        RecordsPerPage = int(RecordsPerPage)

        if colSort == "duration":
            mapping_key = "Duration"
            if orderSort == "1":
                resdf = paginate.sortedFilterDF(duration_collection)
            elif orderSort == "-1":
                resdf = paginate.sortedFilterDF(duration_collection)
                resdf =  resdf[::-1]
        elif colSort == "date_added":
            mapping_key = "Date_Added"
            if orderSort == "1":
                resdf = paginate.sortedFilterDF(date_added_collection)
            elif orderSort == "-1":
                mapping_key = "Date_Added"
                resdf = paginate.sortedFilterDF(date_added_collection)
                resdf =  resdf[::-1]
        if colSort == "release_year":
            mapping_key = "Release_Year"
            if orderSort == "1":
                resdf = paginate.sortedFilterDF(release_year_collection)
            elif orderSort == "-1":
                resdf = paginate.sortedFilterDF(release_year_collection)
                resdf =  resdf[::-1]

        total_records = resdf['Count'].sum()
        start_index = (PG_no - 1) * RecordsPerPage
        end_index = min(start_index + RecordsPerPage, total_records)
        res_dict = paginate.create_index_ranges(resdf, start_index, end_index)
        sliced_doc = []
        document = movies_mapping_collection.find_one()
        for key, idx in (res_dict.items()):
            nested_doc = document[mapping_key][key] 
            sliced_doc.append(nested_doc[idx[0]:idx[1]])
        id_list = []
        for lst in sliced_doc:
            id_list += lst
        movie_query = {"show_id" : {"$in" : id_list }}
        page = movies_collection.find(movie_query)
        page = list(page)
    return render_template('upload_csv.html', pagination = page)
