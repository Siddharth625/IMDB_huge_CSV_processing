from flask import Flask, render_template, url_for, request, session, redirect, jsonify, make_response
import bcrypt
import pandas as pd
from db_config import *
from constants import *

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
    """
    Handle user login functionality.

    Returns:
        str: HTML template for rendering the login page.
    """
    message = ''  # Initialize an empty message string
    # Check if the request method is POST
    if request.method == "POST":
        username = request.form.get("fullname")  # Get the username from the form
        email = request.form.get("email")  # Get the email from the form
        password = request.form.get("password")  # Get the password from the form
        
        # Check if the email exists in the database
        email_found = users.find_one({"email": email})
        if email_found:  # If the email exists
            db_password = users.find_one({"email": email},{"_id": 0, "password": 1})['password']
            # Check if the entered password matches the hashed password in the database
            if  bcrypt.checkpw(password.encode('utf-8'), db_password):
                session["email"] =  email  # Store the email in the session
                return redirect(url_for("uploadCSV"))  # Redirect to the uploadCSV route
            else:
                message = 'Password does not match'  # Set a message indicating password mismatch
            return render_template('logged_in.html', message=message)  # Render the login page with the message
        else:  # If the email is not found in the database
            hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())  # Hash the password
            user_input = {'name': username, 'email': email, 'password': hashed}  # Create user input dictionary
            users.insert_one(user_input)  # Insert the user input into the database
            message = 'Thanks for  registering!'  # Set a message indicating successful registration
            return render_template('logged_in.html', message=message)  # Render the login page with the message
    else:  # If the request method is not POST
        return render_template("logged_in.html")  # Render the login page

    

@app.route('/uploadCSV', methods=['GET', 'POST'])
def uploadCSV():
    """
    Handle CSV file upload and processing.

    Returns:
        str: HTML template for rendering the upload CSV page.
    """
    # Check if the request method is POST
    if request.method == 'POST':
        # Check if 'file' exists in request files
        if 'file' not in request.files:
            return render_template('upload_csv.html', message='No file part')  # Render template with error message
        file = request.files['file']  # Get the file from request files
        # Check if filename is empty
        if file.filename == '':
            return render_template('upload_csv.html', message='No selected file')  # Render template with error message
        chunk_size = CHUNKSIZE  # Set chunk size for reading CSV file
        reader = pd.read_csv(file, chunksize=chunk_size)  # Create CSV reader object with specified chunk size
        records = processor.ProcessUpload(reader, 
                      movies_collection, 
                      movies_mapping_collection,
                      date_added_collection,
                      duration_collection,
                      release_year_collection)  # Process the uploaded CSV file
        return render_template('upload_csv.html', message=f'Processing finished, {records} uploaded successfully!')  # Render template with success message
    return render_template('upload_csv.html')  # Render the upload CSV page


@app.route('/process', methods=['GET', 'POST'])
def process():
    """
    Update processing progress and render the upload CSV page.

    Returns:
        str: HTML template for rendering the upload CSV page.
    """
    # Check if the request method is POST
    if request.method == 'POST':
        progress = request.json.get('progress')  # Get progress from JSON request
        print(f'Number of processed records: {(progress+1)*CHUNKSIZE}')  # Print processing progress
    return render_template('upload_csv.html', progress=f'Number of processed records: {(progress+1)*CHUNKSIZE}')  # Render the upload CSV page with progress message



@app.route("/search", methods=['POST', "GET"])
def search():
    """
    Search for records based on user input parameters.

    Returns:
        str: HTML template for rendering the upload CSV page with search results.
    """
    if request.method == "POST":
        # Get sorting and pagination parameters from form
        colSort = request.form.get("Sort")
        orderSort = request.form.get("Order")
        PG_no = request.form.get("PageNumber")
        RecordsPerPage = request.form.get("Records")
        PG_no = int(PG_no)
        RecordsPerPage = int(RecordsPerPage)
        # Determine the mapping key based on sorting parameter
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
        # Calculate start and end index for pagination
        start_index = (PG_no - 1) * RecordsPerPage
        end_index = min(start_index + RecordsPerPage, total_records)
        # Create index ranges for slicing the DataFrame
        res_dict = paginate.create_index_ranges(resdf, start_index, end_index)
        sliced_doc = []
        # Get movie documents based on index ranges
        document = movies_mapping_collection.find_one()
        for key, idx in (res_dict.items()):
            nested_doc = document[mapping_key][key] 
            sliced_doc.append(nested_doc[idx[0]:idx[1]])
        id_list = []
        # Flatten the list of lists to get the final list of IDs
        for lst in sliced_doc:
            id_list += lst
        # Query movies collection for documents with matching IDs
        movie_query = {"show_id" : {"$in" : id_list }}
        page = movies_collection.find(movie_query)
        page = list(page)
    return render_template('upload_csv.html', pagination = page)
