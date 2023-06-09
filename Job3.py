import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    # Загрузка данных из файла flights.parquet
    df_flights = spark.read.parquet(flights_path)

    # Фильтрация данных для аэропортов с максимальной задержкой 1000 секунд и больше
    df_filtered = df_flights.filter(df_flights['DEPARTURE_DELAY'] >= 1000)

    # Группировка данных по аэропорту отправления (ORIGIN_AIRPORT)
    # и вычисление среднего, минимального и максимального времени задержки
    df_grouped = df_filtered.groupby('ORIGIN_AIRPORT') \
        .agg(F.avg('DEPARTURE_DELAY').alias('avg_delay'),
             F.min('DEPARTURE_DELAY').alias('min_delay'),
             F.max('DEPARTURE_DELAY').alias('max_delay'))

    # Вычисление корреляции между временем задержки и днем недели
    correlation = df_flights.stat.corr('DEPARTURE_DELAY', 'DAY_OF_WEEK')

    # Создание DataFrame для результата
    result_df = spark.createDataFrame([(correlation,)], ['corr_delay2day_of_week'])

    # Соединение данных с корреляцией с данными по аэропортам
    result_df = result_df.crossJoin(df_grouped)

    # Сохранение результатов преобразований
    result_df.write.parquet(result_path, mode='overwrite')


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob3').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
