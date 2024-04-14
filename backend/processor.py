import re
from datetime import datetime
import json
import requests
from constants import PROCESS_API


class Processor:
    def DurationProcessing(self, final_document, duration_collection):
        """
        Process the duration data in the final document and insert it into the duration collection.

        Args:
            final_document (dict): Final document containing duration data.
            duration_collection (pymongo.collection.Collection): Collection to store duration documents.

        Returns:
            str: Message indicating successful processing of duration data.
        """
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

    def DateAddedProcessing(self, final_document, date_added_collection):
        """
        Process the date added data in the final document and insert it into the date added collection.

        Args:
            final_document (dict): Final document containing date added data.
            date_added_collection (pymongo.collection.Collection): Collection to store date added documents.

        Returns:
            str: Message indicating successful processing of date added data.
        """
        data = final_document['Date_Added'].keys()
        datetime_dict = {}
        for string in data:
            datetime_obj = datetime.strptime(string, '%B %d, %Y')
            transformed_string = datetime_obj.strftime('%Y-%m-%d')
            datetime_dict[string] = [transformed_string, len(final_document["Date_Added"][string])]
        sorted_dict = dict(sorted(datetime_dict.items(), key=lambda x: x[1][0]))
        date_added_collection.insert_many([sorted_dict])
        return "Successfully processed date_added."

    def ReleaseYearProcessing(self, final_document, release_year_collection):
        """
        Process the release year data in the final document and insert it into the release year collection.

        Args:
            final_document (dict): Final document containing release year data.
            release_year_collection (pymongo.collection.Collection): Collection to store release year documents.

        Returns:
            str: Message indicating successful processing of release year data.
        """
        data = final_document['Release_Year'].keys()
        int_dict = {}
        for string in data:
            transformed_string = int(string)
            int_dict[string] = [transformed_string, len(final_document["Release_Year"][string])]
        sorted_dict = dict(sorted(int_dict.items(), key=lambda x: x[1][0]))
        release_year_collection.insert_many([sorted_dict])
        return "Successfully processed release_year."

    def ProcessUpload(self, 
                      reader, 
                      movies_collection, 
                      movies_mapping_collection,
                      date_added_collection,
                      duration_collection,
                      release_year_collection
                      ):
        """
        Process the uploaded CSV file in chunks.

        Args:
            reader (pandas.io.parsers.TextFileReader): Reader object for reading the CSV file in chunks.
            movies_collection (pymongo.collection.Collection): Collection to store movie documents.
            movies_mapping_collection (pymongo.collection.Collection): Collection to store movie mapping documents.
            date_added_collection (pymongo.collection.Collection): Collection to store date added documents.
            duration_collection (pymongo.collection.Collection): Collection to store duration documents.
            release_year_collection (pymongo.collection.Collection): Collection to store release year documents.

        Returns:
            int: Total number of records processed.
        """
        total_records = 0
        final_document = {}
        for i, chunk in enumerate(reader):
            chunk['release_year'] = chunk['release_year'].astype(str)
            total_records += len(chunk)
            documents = chunk.to_dict(orient='records')
            Duration_grp = chunk.groupby('duration')['show_id'].apply(list).to_dict()
            final_document['Duration'] = Duration_grp
            Date_Added_grp = chunk.groupby('date_added')['show_id'].apply(list).to_dict()
            final_document['Date_Added'] = Date_Added_grp
            Release_year_grp = chunk.groupby('release_year')['show_id'].apply(list).to_dict()
            final_document['Release_Year'] = Release_year_grp
            movies_collection.insert_many(documents)
            requests.post(PROCESS_API, json={'progress': i + 1})
        self.DurationProcessing(final_document, duration_collection)
        self.DateAddedProcessing(final_document, date_added_collection)
        self.ReleaseYearProcessing(final_document, release_year_collection)
        movies_mapping_collection.insert_many([final_document])
        return total_records