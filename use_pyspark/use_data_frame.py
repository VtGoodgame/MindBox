from uuid import uuid1 as uuid
from pathlib import Path
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when

# Инициализация сессии
spark = SparkSession.builder.appName("Products and Categories").getOrCreate()

# Датафрейм продуктов (products)
products = spark.createDataFrame([
    Row(product_id=1, product_name="iPhone 15"),
    Row(product_id=2, product_name="MacBook Pro"),
    Row(product_id=3, product_name="iPad Air"),
    Row(product_id=4, product_name="Apple Watch"),
    Row(product_id=5, product_name="Samsung Galaxy S23"),
    Row(product_id=6, product_name="Dell XPS 13") 
], schema='product_id int , product_name string')

# Датафрейм категорий (categories)
categories = spark.createDataFrame([
    Row(category_id=1, category_name="Смартфон"),
    Row(category_id=2, category_name="Ноутбук"),
    Row(category_id=3, category_name="Планшет"),
    Row(category_id=4, category_name="Умные часы")
], schema='category_id int, category_name string')


products_and_categories = spark.createDataFrame([
    Row(id = str(uuid),product_id=products.collect()[0].product_name, category_id=categories.collect()[0].category_name),
    Row(id = str(uuid),product_id=products.collect()[1].product_name, category_id=categories.collect()[1].category_name),
    Row(id = str(uuid),product_id=products.collect()[2].product_name, category_id=categories.collect()[2].category_name),
    Row(id = str(uuid),product_id=products.collect()[3].product_name, category_id=categories.collect()[3].category_name),
], schema='product_name string, category_name string')

print("Продукты")
products.show()
products.printSchema()

print("\nКатегории")
categories.show()
categories.printSchema()

print("\n Связь между dataframe Продукты и категории")
products_and_categories.show()
products_and_categories.printSchema()

# Соединение датафреймов для получения пар "Имя продукта – Имя категории"
result_df=products.join(products, products.product_id == categories.category_id, "outer").select(products.product_name, categories.category_name)
result_not_category = result_df.filter(col('category_name').isNull()).select('product_name')

print("Все пары 'Имя продукта – Имя категории':")
result_df.show()
result_df.printSchema()

print("Продукты без категории:")
result_not_category.show()
result_not_category.printSchema()
# Остановка сессии Spark
spark.stop()


class ReadFile_by_spark:
    """
    Класс для чтения файлов различных форматов и получения их столбцов.
    Поддерживаемые форматы: .csv, .json, .parquet
    """
    allowed_columns = {'product_id', 'category_id', 'product_name', 'category_name'}

    def __init__(self, path: str):
        self.path = Path(path)
        self.file = self._read_file()
        self.columns = self.file.columns
        self.validate_columns()

    def _read_file(self):
        if not self.path.exists():
            raise FileNotFoundError(f"Файл {self.path} не найден.")
        
        if self.path.suffix == '.csv':
            return spark.read.csv(str(self.path), header=True, inferSchema=True)
        elif self.path.suffix == '.json':
            return spark.read.json(str(self.path), multiLine=True)
        elif self.path.suffix == '.parquet':
            return spark.read.parquet(str(self.path))
        else:
            raise ValueError("Формат файла не поддерживается. Поддерживаемые форматы: .csv, .json, .parquet")

    def validate_columns(self):
        if not set(self.columns).issubset(self.allowed_columns):
            raise ValueError(f"Недопустимые столбцы: {self.columns}. Допустимые: {self.allowed_columns}")

    def get_dataframe(self):
        return self.file

    @staticmethod
    def get_product_category_pairs(products_df, categories_df, product_category_df):
        """
        Возвращает:
        1. Пары «Имя продукта – Имя категории»
        2. Продукты без категории
        """
        df = products_df.join(product_category_df, on="product_id", how="left").join(categories_df, on="category_id", how="left")\
        .select("product_name", "category_name")

        products_without_categories = df.filter(col("category_name").isNull()).select("product_name")

        return df, products_without_categories
    
    @staticmethod
    def get_product_category_pairs(products_df, categories_df, product_category_df):
        """
        Возвращает датафрейм с парами «Имя продукта – Имя категории» и булевым столбцом,
        указывающим, есть ли у продукта категория.
        """
        df = products_df.join(product_category_df, on="product_id", how="left").join(categories_df, on="category_id", how="left") \
        .select("product_name", "category_name").withColumn("has_category", when(col("category_name").isNotNull(), True).otherwise(False))
        return df
