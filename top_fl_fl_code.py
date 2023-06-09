import pandas as pd

# Загрузка данных из файла flights.parquet
df_flights = pd.read_parquet('flights.parquet')

# Фильтрация строк без указания кода рейса
df_filtered = df_flights[df_flights['TAIL_NUMBER'].notna()]

# Группировка данных по коду рейса и подсчет числа вылетов
df_grouped = df_filtered.groupby('TAIL_NUMBER').size().reset_index(name='Вылеты')

# Сортировка данных по числу вылетов в порядке убывания
df_sorted = df_grouped.sort_values('Вылеты', ascending=False)

# Получение топ 10 рейсов
top_10_flights = df_sorted.head(10)

# Сохранение сводной таблицы в формате Parquet
top_10_flights.to_parquet('top_flights.parquet', index=False)
