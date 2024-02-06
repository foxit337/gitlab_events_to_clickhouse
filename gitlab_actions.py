import requests
from threading import Thread, Semaphore
from clickhouse_driver import Client
import configparser

config = configparser.ConfigParser()
config.read('/usr/local/bin/config.ini')

gitlab_token = config.get('GitLab', 'gitlab_token')
gitlab_url = config.get('GitLab', 'gitlab_url')
csv_file_path = config.get('Paths', 'csv_file_path')
clickhouse_table_name = config.get('ClickHouse', 'clickhouse_table_name')
clickhouse_host = config.get('ClickHouse', 'clickhouse_host')
clickhouse_port = config.getint('ClickHouse', 'clickhouse_port')
clickhouse_user = config.get('ClickHouse', 'clickhouse_user')
clickhouse_password = config.get('ClickHouse', 'clickhouse_password')
clickhouse_database = config.get('ClickHouse', 'clickhouse_database')

users_url = f'{gitlab_url}/users?per_page=500'
headers = {'PRIVATE-TOKEN': gitlab_token}
user_ids = []
events = []
threads = []
semaphore = Semaphore(100)

clickhouse_connection_params = {
    'host': clickhouse_host,
    'port': clickhouse_port,
    'user': clickhouse_user,
    'password': clickhouse_password,
    'database': clickhouse_database,
}

def truncate_clickhouse_table():
    client = Client(**clickhouse_connection_params)

    # Truncate the table
    client.execute(f'TRUNCATE TABLE {clickhouse_table_name}')

def write_to_clickhouse():
    client = Client(**clickhouse_connection_params)

    # Truncate the ClickHouse table before inserting data
    truncate_clickhouse_table()

    # Read data from CSV and insert into ClickHouse
    with open(csv_file_path, "r") as csv_file:
        lines = csv_file.readlines()
        data = [line.strip().split(',') for line in lines]

        # Insert data into ClickHouse
        client.execute(f'INSERT INTO {clickhouse_table_name} VALUES', data)

def get_users(page):
    try:
        users_url_page = f'{users_url}&page={page}'
        r = requests.get(users_url_page, headers=headers)
        r.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        users = r.json()
        for user in users:
            user_ids.append(user["id"])
    except requests.RequestException as e:
        print(e)

def get_data(sem, user_id):
    page = 0
    while True:
        with sem:
            events_url = f'{gitlab_url}/users/{user_id}/events/?per_page=500&page={page}'
            r = requests.get(events_url, headers=headers)
            r.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            try:
                data = r.json()
                if not data:  # No more data to fetch
                    break

                for value in data:
                    entry = f'{value["author"]["username"]},{value["action_name"]},{value["created_at"]}'
                    events.append(entry)

                page += 1
            except requests.RequestException as e:
                print(e)


if __name__ == '__main__':
    with requests.Session() as session:
        for n in range(10):
            thread = Thread(target=get_users, args=(n,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for user_id in user_ids:
            thread = Thread(target=get_data, args=(semaphore, user_id))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    with open(csv_file_path, "w") as csv_file:
        for line in events:
            csv_file.write(line + "\n")
    write_to_clickhouse()

    print("Data collection and CSV/Clickhouse writing completed.")

