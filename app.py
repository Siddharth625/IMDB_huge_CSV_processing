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

from backend.routes import *

if __name__ == "__main__":
    app.run(debug=True)
