import pandas as pd
import mysql.connector
import gzip
from dotenv import load_dotenv
import os

load_dotenv()

def get_db_connection():
    return mysql.connector.connect(
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        host = os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        database = os.getenv("DB_NAME")
    )

def extract_data(file_path, column_names):
    print(f"Extracting data from {file_path}...")
    try:
        if "title.basics" in file_path:
            column_names = ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres']
        elif "title.ratings" in file_path:
            column_names = ['tconst', 'averageRating', 'numVotes']
        elif "name.basics" in file_path:
            column_names = ['nconst', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession', 'knownForTitles']
        else:
            print("Unknown file type.")
            return None
        
        df = pd.read_csv(
            gzip.open(file_path, 'rt', encoding='utf-8'),
            sep='\t',
            low_memory=False,
            na_values='\\N',
            chunksize=30000,
            header=None,
            names=column_names
        )
        return df
    except Exception as e:
        print(f"Error during extraction from {file_path}: {e}")
        return None
    
def transform_data(df, table_name):
    print(f"Transforming data for {table_name}...")
    if df is None:
        return None
    
    is_first_chunk = True
    for chunk_df in df:
        if is_first_chunk:
            chunk_df = chunk_df.iloc[1:].copy()
            is_first_chunk = False
            if chunk_df.empty:
                continue

        chunk_df = chunk_df.astype(object).where(pd.notnull(chunk_df), None)
        if table_name == 'titles':
            chunk_df['isAdult'] = pd.to_numeric(chunk_df['isAdult'], errors='coerce')
            chunk_df['isAdult'] = chunk_df['isAdult'].apply(lambda x: 1 if x == 1 else 0)
            chunk_df['isAdult'] = chunk_df['isAdult'].astype(int)
            cols_to_load = ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres']
            chunk_df = chunk_df[cols_to_load]

        elif table_name == 'ratings':
            cols_to_load = ['tconst', 'averageRating', 'numVotes']
            chunk_df = chunk_df[cols_to_load]

        elif table_name == 'people':
            cols_to_load = ['nconst', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession', 'knownForTitles']
            chunk_df = chunk_df[cols_to_load]
        
        yield chunk_df
    
def load_data(transformed_iterator, table_name, connection):
    if transformed_iterator is None:
        print(f"No data to load into {table_name}.")
        return

    cursor = connection.cursor()
    total_rows = 0

    try:
        for chunk_df in transformed_iterator:
            if chunk_df is None or chunk_df.empty:
                continue

            columns = ', '.join(chunk_df.columns)
            placeholders = ', '.join(['%s'] * len(chunk_df.columns))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            records = [tuple(x) for x in chunk_df.values]
            cursor.executemany(sql, records)
            connection.commit()
            total_rows += len(records)
            print(f"Successfully loaded {len(records)} rows into {table_name}.")

    except mysql.connector.Error as err:
        print(f"Error loading data into {table_name}: {err}")
        connection.rollback()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        connection.rollback()
    finally:
        cursor.close()
    
    print(f"Finished loading a total of {total_rows} rows into {table_name}.")


if __name__ == "__main__":
    db_connection = get_db_connection()

    titles_cols = ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres']
    titles_iterator = extract_data("data/title.basics.tsv.gz", titles_cols)
    transformed_titles = transform_data(titles_iterator, "titles")
    load_data(transformed_titles, "titles", db_connection)

    ratings_cols = ['tconst', 'averageRating', 'numVotes']
    ratings_iterator = extract_data("data/title.ratings.tsv.gz", ratings_cols)
    transformed_ratings = transform_data(ratings_iterator, "ratings")
    load_data(transformed_ratings, "ratings", db_connection)

    people_cols = ['nconst', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession', 'knownForTitles']
    people_iterator = extract_data("data/name.basics.tsv.gz", people_cols)
    transformed_people = transform_data(people_iterator, "people")
    load_data(transformed_people, "people", db_connection)

    db_connection.close()