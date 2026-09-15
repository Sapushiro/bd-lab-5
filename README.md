# Кластеризация Open Food Facts на PySpark

## Результат

Модель разделяет продукты Open Food Facts на группы по пищевой ценности. Для обучения используются пять числовых признаков:

- энергетическая ценность;
- жиры;
- углеводы;
- белки;
- сахар.

В ходе экспериментов проверялись значения `k` от 2 до 8. Лучший результат:

| Параметр | Значение |
| --- | ---: |
| Алгоритм | KMeans |
| Объём выборки | 1 000 000 продуктов |
| Лучшее число кластеров | 4 |
| Silhouette score | 0.59 |

Из первоначального набора признаков были исключены `saturated-fat`, `fiber` и `salt`. Насыщенные жиры частично дублировали общий показатель жиров, клетчатка содержала больше пропусков, а экстремальные значения соли формировали отдельный кластер выбросов. После отбора признаков silhouette score вырос с `0.489919` до `0.59`.

## Структура проекта

```text
bd-lab-5/
├── conf/
│   └── spark-defaults.conf     # параметры запуска Spark
├── src/
│   ├── preprocess.py           # чтение и предобработка исходных данных
│   ├── train.py                # подбор k и обучение KMeans
│   ├── evaluate.py             # анализ и интерпретация кластеров
│   ├── logger.py               # настройка логирования
│   └── spark_session.py        # создание SparkSession
├── test/
│   ├── text.txt                # данные для проверки WordCount
│   └── word_count.py           # тестовый пример Spark
├── config.json                 # пути, признаки и параметры модели
├── requirements.txt
└── README.md
```

Следующие каталоги создаются локально и не сохраняются в Git:

```text
data/                           # исходные и обработанные данные
models/                         # сохранённая PipelineModel
artifacts/                      # метрики, профили и предсказания
spark-events/                   # журналы событий Spark
logs/                           # журналы приложения
```

## Используемые технологии

- Python 3.12;
- OpenJDK 17;
- Apache Spark / PySpark 4.2;
- PySpark SQL;
- PySpark ML;
- Parquet;
- Spark History Server.

## Подготовка окружения на macOS

Установить Java 17 и Python 3.12:

```bash
brew install openjdk@17 python@3.12
```

Добавить Java 17 в окружение. Для Mac с Apple Silicon:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

Проверить версии:

```bash
java --version
python3.12 --version
```

Создать и активировать виртуальное окружение:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Проверить PySpark:

```bash
pyspark --version
```

## Конфигурация Spark

Параметры Spark находятся в `conf/spark-defaults.conf`. Для запуска на локальной машине использовались четыре рабочих потока, 8 ГБ памяти драйвера, восемь shuffle-партиций и Adaptive Query Execution.

Пример конфигурации:

```properties
spark.master                       local[4]
spark.driver.memory                8g
spark.sql.shuffle.partitions       8
spark.sql.adaptive.enabled         true
spark.eventLog.enabled             true
spark.eventLog.dir                 file:./spark-events
```

Конфигурация вынесена из Python-кода, поэтому параметры ресурсов можно менять без изменения приложения.

Перед первым запуском необходимо создать каталог журналов событий:

```bash
mkdir -p spark-events
```

## Проверка Spark с помощью WordCount

Перед основной частью лабораторной выполняется тестовая задача подсчёта слов:

```bash
spark-submit \
  --properties-file conf/spark-defaults.conf \
  test/word_count.py
```

Успешное выполнение подтверждает, что Python, Java и Spark настроены корректно и Spark способен читать данные, распределять вычисления и возвращать результат.

## Получение данных

Используется набор данных [Open Food Facts](https://world.openfoodfacts.org/data). Для проекта выбран официальный экспорт в формате Parquet, поскольку он имеет колоночную структуру, поддерживает выборочное чтение столбцов и эффективно обрабатывается Spark.

Создать каталог и скачать датасет:

```bash
mkdir -p data/raw

curl -L \
  "https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet?download=true" \
  -o data/raw/food.parquet
```

Исходный файл имеет большой размер и не должен добавляться в Git.

## Настройки приложения

Файл `config.json` содержит:

- пути к исходным и обработанным данным;
- максимальный размер выборки;
- набор признаков для предобработки;
- набор признаков для модели;
- диапазон значений `k`;
- параметры KMeans;
- пути сохранения модели, метрик и результатов анализа.

Для модели используется следующий набор:

```json
"model_features": [
  "energy-kcal",
  "fat",
  "carbohydrates",
  "proteins",
  "sugars"
]
```

Разделение `preprocessing_features` и `model_features` позволяет один раз сохранить полный набор подготовленных нутриентов, а затем проводить эксперименты с разными признаками без повторной обработки исходного файла.

## Предобработка данных

Запуск:

```bash
spark-submit \
  --properties-file conf/spark-defaults.conf \
  src/preprocess.py
```

Во время предобработки выполняются:

1. чтение исходного Parquet-файла;
2. извлечение кода, названия, бренда и категорий продукта;
3. разворачивание массива `nutriments` с помощью `explode_outer`;
4. получение значений нутриентов на 100 г;
5. преобразование нутриентов из строк в столбцы с помощью `pivot`;
6. объединение пищевых характеристик с информацией о продукте;
7. удаление дубликатов, пропусков и некорректных значений;
8. формирование воспроизводимой выборки до 1 000 000 строк;
9. сохранение результата в Parquet.

Обработанный набор сохраняется в:

```text
data/processed/food_small.parquet
```

## Обучение модели

Запуск:

```bash
spark-submit \
  --properties-file conf/spark-defaults.conf \
  src/train.py
```

Этапы обучения:

1. `VectorAssembler` объединяет числовые признаки в вектор `raw_features`;
2. `StandardScaler` стандартизирует признаки, чтобы их масштабы одинаково влияли на евклидово расстояние;
3. KMeans обучается для каждого `k` из заданного диапазона;
4. каждая модель оценивается по silhouette score;
5. выбирается модель с максимальным silhouette score;
6. сохраняются метрики, PipelineModel и предсказанный кластер каждого продукта.

Сохранённая `PipelineModel` включает `VectorAssembler`, обученный `StandardScalerModel` и `KMeansModel`. Поэтому при обработке новых данных применяются те же преобразования, что и во время обучения.

Результаты сохраняются в:

```text
models/openfoodfacts_kmeans/
artifacts/kmeans_metrics.json
artifacts/food_clusters/
```

## Оценка и интерпретация кластеров

Запуск:

```bash
spark-submit \
  --properties-file conf/spark-defaults.conf \
  src/evaluate.py
```

Скрипт не обучает модель повторно. Он читает сохранённые предсказания и формирует:

- количество продуктов в каждом кластере;
- средние значения используемых признаков;
- пять наиболее частых категорий каждого кластера;
- примеры продуктов из каждого кластера.

Результаты сохраняются в каталоги:

```text
artifacts/cluster_profiles/
artifacts/cluster_categories/
artifacts/cluster_examples/
```

Spark сохраняет DataFrame как каталог с одним или несколькими файлами `part-*.csv`. Использование `coalesce(1)` создаёт один CSV-файл данных, но сам путь всё равно остаётся каталогом.

## Интерпретация результата

Для лучшей модели с `k = 4` были получены следующие обобщённые группы:

| Кластер | Основная характеристика |
| ---: | --- |
| 0 | калорийные сладкие продукты |
| 1 | низкокалорийные продукты |
| 2 | жирные продукты |
| 3 | продукты с высоким содержанием белка |

Номер кластера не имеет самостоятельного смысла и может измениться при другом seed или повторном обучении. Интерпретация выполняется по средним значениям признаков, категориям и примерам товаров.

## Spark UI и History Server

Во время выполнения приложения Spark UI доступен по адресу:

```text
http://localhost:4040
```

После завершения приложения интерфейс на порту 4040 закрывается. Для просмотра сохранённой истории необходимо запустить History Server:

History Server читает настройки из каталога `SPARK_CONF_DIR`, поэтому при локальном запуске ему нужно указать проектный каталог `conf`:

```bash
SPARK_CONF_DIR="$(pwd)/conf" start-history-server.sh
```

Интерфейс истории будет доступен по адресу:

```text
http://localhost:18080
```

Остановка History Server:

```bash
stop-history-server.sh
```

В Spark UI можно исследовать jobs, stages, tasks, SQL-запросы, число партиций, shuffle-операции, кэширование и физические планы выполнения.

## Полный порядок запуска

```bash
source .venv/bin/activate

spark-submit --properties-file conf/spark-defaults.conf test/word_count.py
spark-submit --properties-file conf/spark-defaults.conf src/preprocess.py
spark-submit --properties-file conf/spark-defaults.conf src/train.py
spark-submit --properties-file conf/spark-defaults.conf src/evaluate.py
```

При изменении только логики оценки достаточно повторно запустить `evaluate.py`. При изменении `model_features` необходимо повторить обучение и оценку. Предобработка требуется заново только при изменении исходных данных, правил очистки, размера выборки или `features`.
