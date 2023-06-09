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

    # Группировка данных по авиамаршрутам (ORIGIN_AIRPORT, DESTINATION_AIRPORT)
    # и подсчет числа рейсов и среднего времени в полете
    df_grouped = df_flights.groupby('ORIGIN_AIRPORT', 'DESTINATION_AIRPORT') \
        .agg(F.count('TAIL_NUMBER').alias('tail_count'), F.avg('AIR_TIME').alias('avg_air_time'))

    # Сортировка данных по числу рейсов в порядке убывания
    df_sorted = df_grouped.orderBy(F.desc('tail_count'))

    # Получение топ 10 авиамаршрутов
    top_10_routes = df_sorted.limit(10)

    # Сохранение результатов преобразований
    top_10_routes.write.parquet(result_path, mode='overwrite')


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob2').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
