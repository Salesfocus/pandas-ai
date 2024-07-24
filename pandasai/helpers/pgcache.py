import glob
import os
from typing import Any

import psycopg2

from ..constants import POSTGRES_CONNECTION
# from .path import find_project_root


class Cache:
    """Cache class for caching queries. It is used to cache queries
    to save time and money.

    Args:
        filename (str): filename to store the cache.
    """

    def __init__(self, filename="", abs_path=None):
        # # Define cache directory and create directory if it does not exist
        # if abs_path:
        #     cache_dir = abs_path
        # else:
        #     try:
        #         cache_dir = os.path.join(find_project_root(), "cache")
        #     except ValueError:
        #         cache_dir = os.path.join(os.getcwd(), "cache")

        # os.makedirs(cache_dir, mode=DEFAULT_FILE_PERMISSIONS, exist_ok=True)

        # self.filepath = os.path.join(cache_dir, f"{filename}.db")
        # self.connection = duckdb.connect(self.filepath)
        # self.connection.execute(
        #     "CREATE TABLE IF NOT EXISTS cache (key STRING, value STRING)"
        # )
        pass

    def versioned_key(self, key: str) -> str:
        return f"{key}"

    def set(self, key: str, value: str) -> None:
        # """Set a key value pair in the cache.

        # Args:
        #     key (str): key to store the value.
        #     value (str): value to store in the cache.
        # """
        # self.connection.execute(
        #     "INSERT INTO cache VALUES (?, ?)", [self.versioned_key(key), value]
        # )
        pass

    def get(self, key: str) -> str:
        """Get a value from the cache.

        Args:
            key (str): key to get the value from the cache.

        Returns:
            str: value from the cache.
        """
        parts = key.split("~")

        conn = psycopg2.connect(
            host=POSTGRES_CONNECTION["host"],
                database=POSTGRES_CONNECTION["database"],
                user=POSTGRES_CONNECTION["user"],
                password=POSTGRES_CONNECTION["password"]
        )

        cursor = conn.cursor()

        check_query = '''
            SELECT code_executed FROM promptmaster
            WHERE data_source = %s AND dataframe_hash = %s AND normalized_question = %s AND bad_code is null 
        '''
         # question = parts[2][parts[2].index("'") + 1:parts[2].rindex("'")]
        question = parts[2]
        question = question.replace("### QUERY\n ", "")
        cursor.execute(check_query, (parts[0], parts[1], question))
        question_exists = cursor.fetchone()

        
        if question_exists == None:
          return None
        else:
          return question_exists[0]

    def delete(self, key: str) -> None:
        # """Delete a key value pair from the cache.

        # Args:
        #     key (str): key to delete the value from the cache.
        # """
        # self.connection.execute(
        #     "DELETE FROM cache WHERE key=?", [self.versioned_key(key)]
        # )
        pass

    def close(self) -> None:
        # """Close the cache."""
        # self.connection.close()
        pass

    def clear(self) -> None:
        # """Clean the cache."""
        # self.connection.execute("DELETE FROM cache")
        pass

    def destroy(self) -> None:
        # """Destroy the cache."""
        # self.connection.close()
        # for cache_file in glob.glob(f"{self.filepath}.*"):
        #     os.remove(cache_file)
        pass

    def get_cache_key(self, context: Any) -> str:
        """
        Return the cache key for the current conversation.

        Returns:
            str: The cache key for the current conversation
        """
        # cache_key = context.memory.get_conversation()

        cache_key = context.data_source  + "~" + self.get_column_hash(context) + "~" + context.normalized_memory.get_last_message()

        # # make the cache key unique for each combination of dfs
        # for df in context.dfs:
        #     cache_key += str(df.column_hash)

        return cache_key
    
    def get_column_hash(self, context: Any) -> str:
        """
        Return the cache key for the current conversation.
        """
        column_hash = ""
        # make the cache key unique for each combination of dfs
        for df in context.dfs:
            column_hash += str(df.column_hash)

        return column_hash
