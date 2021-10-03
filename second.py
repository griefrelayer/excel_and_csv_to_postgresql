import os
from sys import argv, exit
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# Database configuration
postgres_user = "postgres"
postgres_password = "postgres"
postgres_host = "localhost"
postgres_port = "5432"
db_name = "enkost"

csv_dir = Path(r"C:\Users\Илья\python\enkost_test\2. create_view_task")   # Folder with .csv files


def dt_inplace(df):
    """Automatically detect and convert (in place!) each
    dataframe column of datatype 'object' to a datetime just
    when ALL of its non-NaN values can be successfully parsed
    by pd.to_datetime().  Also returns a ref. to df for
    convenient use in an expression.
    """
    from pandas.errors import ParserError
    for c in df.columns[df.dtypes=='object']: #don't cnvt num
        try:
            df[c]=pd.to_datetime(df[c])
        except (ParserError,ValueError): #Can't cnvrt some
            pass # ...so leave whole column as-is unconverted
    return df


def read_csv(*args, **kwargs):
    """Drop-in replacement for Pandas pd.read_csv. It invokes
    pd.read_csv() (passing its arguments) and then auto-
    matically detects and converts each column whose datatype
    is 'object' to a datetime just when ALL of the column's
    non-NaN values can be successfully parsed by
    pd.to_datetime(), and returns the resulting dataframe.
    """
    return dt_inplace(pd.read_csv(*args, **kwargs))


dir_files = os.listdir(csv_dir)    # Getting csv folder files list
found = False
csv_tables_data = {}
for file in dir_files:    # Searching for the .csv files
    if file.endswith(".csv") and file != "example_result.csv":
        found = True
        # Adding .csv as pandas DataFrame to csv table list with filename without extention as a table name
        csv_tables_data[file.split(".")[0]] = read_csv(Path(csv_dir, file), sep=";")

if not found:
    print("Не нашел файлы... Для выхода нажмите enter...")
    input("")
    exit()

print("Создаю подключение к БД...")

# Using SqlAlchemy to connect to db
try:
    engine = \
        create_engine(f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{db_name}")
except:
    print("Произошла ошибка при подключении к БД. Проверьте данные доступа к БД.")
    exit()

print("Подключено. Создаю таблицы данных...")

for table_name, table_data in csv_tables_data.items():    # Inserting data from .csv files to db
    try:
        table_data.to_sql(table_name, engine, if_exists="replace")
    except:
        print("Произошла ошибка при добавлении данных в БД. Возможно, представление period_view уже создано и"
              "не позволяет подменить таблицы, от которых оно зависит, либо неверный формат файла csv.")
        exit()
print('Данные успешно внесены в БД.')
print('Отправляем запрос на создание postgresql view...')

query = """CREATE OR REPLACE VIEW public.period_view
             AS
             SELECT a.endpoint_id,
                a.mode_start,
                a.mode_start + a.mode_duration::double precision * '00:01:00'::interval AS mode_end,
                a.mode_duration,
                ''::text AS label,
                c.reason,
                b.operator_name,
                sum(d.kwh / 60::double precision) AS energy_sum
               FROM periods a,
                operators b,
                reasons c,
                energy d
              WHERE a.endpoint_id = b.endpoint_id AND a.endpoint_id = c.endpoint_id AND tstzrange(b.login_time, b.logout_time) @> a.mode_start AND c.event_time <@ tstzrange(a.mode_start, a.mode_start + a.mode_duration::double precision * '00:01:00'::interval) AND d.event_time <@ tstzrange(a.mode_start, a.mode_start + a.mode_duration::double precision * '00:01:00'::interval)
              GROUP BY a.endpoint_id, a.mode_start, a.mode_duration, c.reason, b.operator_name;
            """

try:
    engine.execute(text(query))
except:
    print("Произошла ошибка создания представления.")
    input('Нажмите enter для выхода...')
    exit()

print("Представление period_view успешно создано!")
input('Нажмите enter для выхода...')
