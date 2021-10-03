from pathlib import Path
from sys import argv, exit
import pandas as pd
from sqlalchemy import create_engine

# Database configuration
postgres_user = "postgres"
postgres_password = "postgres"
postgres_host = "localhost"
postgres_port = "5432"
db_name = "enkost"

if len(argv) > 1 and argv[1].endswith('.xlsm'):
    path = Path(argv[1])
    try:
        print(f"Открываю {argv[1]}...")
        excel_table_data = pd.read_excel(path)    # Reading xlsm
    except FileNotFoundError:
        print("Файл не найден! Проверьте путь.")
        print("Используйте upload.exe путь_к_файлу.xlsm")
        input("Нажмите enter для выхода")
        exit()
    except:
        print("Произошла ошибка. Возможно, файл неверный или доступ к нему ограничен")
        print("Используйте upload.exe путь_к_файлу.xlsm")
        input("Нажмите enter для выхода...")
        exit()

else:
    print("Отсутствует путь к файлу. Используйте upload.exe путь_к_файлу.xlsm")
    print("Для выхода нажмите enter...")
    input("")
    exit()

print("Успешно. Создаю подключение к БД...")

# Using SqlAlchemy to connect to db
try:
    engine = \
        create_engine(f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{db_name}")
except:
    print("Произошла ошибка при подключении к БД. Проверьте данные доступа к БД.")
    exit()
print("Подключено. Создаю/обновляю таблицу данных...")

try:
    excel_table_data.to_sql("endpoint_names", engine, if_exists="replace")    # Uploading data from .xlsm to db
except:
    print("Произошла ошибка при добавлении данных в БД. Возможно, неверный формат данных в файле excel.")
    exit()
print('Данные успешно внесены в БД.')
input('Нажмите enter для выхода...')


