import json
import random
import sqlite3
import threading
import queue
from statistics import fmean


def read_config():
    """
    Функция для чтения конфигурационного файла
    """
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config

def create_dict_of_arrays(n):
    """
    Функция для создания словаря с массивами
    """
    arrays = {}
    for i in range(n):
        array_name = f'array_{i+1}'
        arrays[array_name] = [random.randint(0, 100) for _ in range(random.randint(50, 100))]
    return arrays


def create_db(db_path):
    """
    Функция для удаления старой таблицы, создания новой базы данных (если не существует) и таблицы
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''DROP TABLE IF EXISTS calculations''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS calculations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        array_name TEXT,
                        current_element INTEGER,
                        avg_last_m REAL,
                        avg_all REAL)''')
    conn.commit()
    conn.close()

def save_to_db(db_path, array_name, current_element, avg_last_m, avg_all):
    """
    Функция для сохранения данных в базу данных
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO calculations (array_name, current_element, avg_last_m, avg_all) VALUES (?, ?, ?, ?)',
                   (array_name, current_element, avg_last_m, avg_all))
    conn.commit()
    conn.close()

def process_array(array_name, array, m, result_queue):
    """
    Функция для обработки массива
    """
    read_values = []
    for i in range(len(array)):
        current_value = array[i]
        read_values.append(current_value)

        avg_last_m = fmean(read_values[-m:]) if len(read_values) >= m else fmean(read_values)
        avg_all = fmean(read_values)

        result_queue.put((array_name, current_value, avg_last_m, avg_all))

def worker(db_path, result_queue):
    """
    Рабочий поток для записи данных в таблицу
    """
    while True:
        item = result_queue.get()
        if item is None:
            break
        array_name, current_element, avg_last_m, avg_all = item
        save_to_db(db_path, array_name, current_element, avg_last_m, avg_all)

def main():
    config = read_config()
    n = config['n']
    m = config['m']
    db_path = config['db_path']

    arrays = create_dict_of_arrays(n)
    create_db(db_path)

    result_queue = queue.Queue()

    worker_thread = threading.Thread(target=worker, args=(db_path, result_queue))
    worker_thread.start()

    threads = []

    for array_name, array in arrays.items():
        thread = threading.Thread(target=process_array, args=(array_name, array, m, result_queue))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    result_queue.put(None)
    worker_thread.join()

if __name__ == '__main__':
    main()
    print('all done!')