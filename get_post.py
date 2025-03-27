import pandas as pd
from collections import Counter

def read_data(file_path):
    data = pd.read_csv(file_path)
    return data




if __name__ == "__main__":
    data_path = "ai_posts.csv"
    data = read_data(data_path)
    print(Counter(data['protocol']))
    