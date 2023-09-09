import random
import time

import psycopg2
from redis import Redis

in_redis_time = 0
in_postgres_time = 0
out_redis_time = 0
out_postgres_time = 0
redis_list = []
postgres_list = []
cars = ["Audi", "BMW", "Chevrolet", "Kia", "Hyundai", "Lada", "Renault", "Nissan", "Toyota", "Honda",
        "Volkswagen", "Ford", "Chevrolet", "Mazda", "Subaru"]
auto_nums = ["A", "B", "C", "E", "H", "K", "M", "O", "P", "T", "X", "Y"]
districts = ['Центральный', 'Северный', 'Северо-Восточный', 'Восточный', 'Юго-Восточный', 'Южный',
             'Юго-Западный', 'Западный', 'Северо-Западный']


def create_taxi_redis(taxi_amount: int):
    r = Redis(host='localhost', port='6379', decode_responses=True)
    start_time = time.time()
    global redis_time

    with r.pipeline() as pipe:
        pipe.multi()

        for i in range(taxi_amount):
            district = random.choice(districts)
            car = random.choice(cars) + '--' + str(random.choice(auto_nums)) + str(random.randint(100, 999)) \
                  + str(random.choice(auto_nums)) + str(random.choice(auto_nums)) + str(random.randint(33, 190))

            coordinate_x = 55 + (random.randint(1, 10) / 10) + (random.randint(1, 100) / 100)
            coordinate_y = 37 + (random.randint(1, 10) / 10) + (random.randint(1, 100) / 100)

            pipe.geoadd(district, [coordinate_x, coordinate_y, car], nx=False)

        pipe.execute()

    end_time = time.time()
    redis_time = end_time - start_time
    print("Затраченное время на ввод данных для REDIS: ", redis_time)
    r.close()


def create_taxi_postgres(taxi_amount: int):
    conn = psycopg2.connect(host='localhost', port=5432, dbname='fastapi', user='postgres',
                            password='postgres')
    cursor = conn.cursor()
    start_time = time.time()
    global postgres_time

    for i in range(taxi_amount):
        district = random.choice(districts)
        car = random.choice(cars) + str(random.choice(auto_nums)) + str(random.randint(100, 999)) \
              + str(random.choice(auto_nums)) + str(random.choice(auto_nums)) + str(random.randint(33, 190))

        coordinate_x = 55 + (random.randint(1, 10) / 10) + (random.randint(1, 100) / 100)
        coordinate_y = 37 + (random.randint(1, 10) / 10) + (random.randint(1, 100) / 100)
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS taxi (id SERIAL PRIMARY KEY, district VARCHAR, car VARCHAR, coordinate_x FLOAT,"
            " coordinate_y FLOAT)")
        cursor.execute("INSERT INTO taxi (district, car, coordinate_x, coordinate_y) VALUES (%s, %s, %s, %s)",
                       (district, car, coordinate_x, coordinate_y))
        conn.commit()

    cursor.close()
    conn.close()
    end_time = time.time()
    postgres_time = end_time - start_time
    print("Затраченное время на ввод данных для Postgres: ", postgres_time)


def check_select_postgres():
    global out_postgres_time
    start_time = time.time()

    conn = psycopg2.connect(host='localhost', port=5432, dbname='fastapi', user='postgres',
                            password='postgres')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM taxi")
    rows = cursor.fetchall()
    count = 0
    for row in rows:
        count += 1
        string = "\n".join(map(str, row))
        with open('postgres.txt', 'a', encoding='utf-8') as file:
            file.write(string)

    cursor.close()
    conn.close()
    end_time = time.time()
    out_postgres_time = end_time - start_time
    print("Затраченное время на вывод данных из Postgres: ", out_postgres_time, "Количество строк :", count)


def check_select_redis():
    global out_redis_time
    start_time = time.time()
    r = Redis(host='localhost', port='6379', decode_responses=True)
    keys = r.keys('*')
    count = 0
    for key in keys:
        count += 1
        string = "\n".join(map(str, r.zrange(key, 0, -1)))

        with open('redis.txt', 'a', encoding='utf-8') as file:
            file.write(string)

    r.close()
    end_time = time.time()
    out_redis_time = end_time - start_time
    print("Затраченное время на вывод данных из Redis: ", out_redis_time, "Количество строк :", count)

if __name__ == '__main__':

    create_taxi_redis(100)
    create_taxi_postgres(100)
    check_select_postgres()
    check_select_redis()
    total_in = postgres_time / redis_time
    total_out = out_postgres_time / out_redis_time
    print(f"Ввод данных в REDIS быстрее чем в POSTGRES в {round((total_in), 4)} раз")
    print(f"Вывод данных из REDIS быстрее чем из POSTGRES в {round((total_out), 4)} раз")
