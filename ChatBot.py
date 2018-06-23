import sqlite3
import json
from datetime import datetime
from Constants import DATA_DIR

timeframe = '2005-12'
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()


def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id "
              "TEXT PRIMARY KEY, "
              "comment_id TEXT UNIQUE, "
              "parent TEXT, "
              "comment TEXT, "
              "subreddit TEXT, "
              "unix INT, "
              "score INT)")


def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data


def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result is not None:
            return result[0]
        else:
            return False
    except Exception as e:
        print("find parent ", str(e))
        return False


def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return  False
    elif len(data) >1000:
        return False
    elif data == '[deleted]' or data = '[removed]':
        return False

def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result is not None:
            return result[0]
        else:
            return False
    except Exception as e:
        print("find parent ", str(e))
        return False


def main():
    create_table()
    row_counter = 0  # How much rows were iterated
    paired_rows = 0  # How many <parent,child> are there

    with open(DATA_DIR + "RC_{}".format(timeframe), buffering=1000) as f:
        print("Reading file...")
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['name']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)

            if score >= 2:
                existing_comment_score = find_existing_score(parent_id)
                if score > existing_comment_score:


if __name__ == '__main__':
    main()
