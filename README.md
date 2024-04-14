# IMDB_huge_CSV_processing

## Steps to run the repo:
#### 1. Clone https://github.com/Siddharth625/IMDB_huge_CSV_processing (main)
#### 2. Run the command - "pip install -r requirements.txt"
#### 3. Run the command - "python app.py"
#### 4. Click the localhost url generated

A few things to note:
1) Login/Register - The login API does the functionality of both logging in and registration. If the user is not on the DB, it will register the user first, then you can use the same login credentials to log in.

2) The process API does not render the processed records on the UI; however, you can see the results and the working on the server logs. The frontend rendering can be made efficient using React hooks like useState to change (out of scope).

3) show_id - The show_id is not consistent, therefore I have changed it to natural numbers like 1, 2, and 3 for the sake of simplicity.

4) The logged-in user can use the app, however the file uploading schemas do not take into account which user, need to change the DB schema and config but haven't done yet due to time constraints

5) Test cases are not yet completed due to time constraints
   
6) The DB is cleared for your testing purposes.

