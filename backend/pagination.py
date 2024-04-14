import pandas as pd

class Paginate:
    def sortedFilterDF(self, 
                       collection):
        d = collection.find_one({},{'_id': 0})
        df = pd.DataFrame.from_dict(d, orient='index', columns=['Value', 'Count'])
        df.index.name = 'Index'
        return df

    def create_index_ranges(self, 
                            df, 
                            start_index, 
                            end_index):
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