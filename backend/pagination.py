import pandas as pd

class Paginate:
    def sortedFilterDF(self, 
                       collection):
        """
        Retrieve data from the MongoDB collection and create a sorted DataFrame.

        Args:
            collection (pymongo.collection.Collection): MongoDB collection to retrieve data from.

        Returns:
            pandas.DataFrame: Sorted DataFrame containing the retrieved data.
        """
        d = collection.find_one({},{'_id': 0})
        df = pd.DataFrame.from_dict(d, orient='index', columns=['Value', 'Count'])
        df.index.name = 'Index'
        return df

    def create_index_ranges(self, 
                            df, 
                            start_index, 
                            end_index):
        """
        Create index ranges based on the given DataFrame and start/end index.

        Args:
            df (pandas.DataFrame): DataFrame containing data to create index ranges from.
            start_index (int): Start index for creating the range.
            end_index (int): End index for creating the range.

        Returns:
            dict: Dictionary containing index ranges.
        """
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