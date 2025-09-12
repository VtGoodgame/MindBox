import pytest
from pathlib import Path
from pyspark.sql import SparkSession, Row
from use_pyspark.use_data_frame import ReadFile_by_spark  



# Фикстура для pyspark
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("TestApp").getOrCreate()
    yield spark
    spark.stop()

#Фикстура для  файлов 
@pytest.fixture
def tmp_csv_file(tmp_path):
    file_path = tmp_path / "products.csv"
    file_path.write_text("product_id,product_name\n1,iPhone 15\n2,MacBook Pro")
    return file_path

@pytest.fixture
def tmp_invalid_csv_file(tmp_path):
    file_path = tmp_path / "bad.csv"
    file_path.write_text("bad_column,something\n1,abc\n2,def")
    return file_path

@pytest.fixture
def product_dataframes(spark):
    # Продукты
    products = spark.createDataFrame([
        Row(product_id=1, product_name="iPhone 15"),
        Row(product_id=2, product_name="MacBook Pro"),
        Row(product_id=3, product_name="iPad"),
        Row(product_id=4, product_name="Apple Watch")
    ])
    # Категории
    categories = spark.createDataFrame([
        Row(category_id=1, category_name="Смартфон"),
        Row(category_id=2, category_name="Ноутбук"),
    ])
    # Связи
    product_category = spark.createDataFrame([
        Row(product_id=1, category_id=1),
        Row(product_id=2, category_id=2),
    ])
    return products, categories, product_category

#Тесты

def test_read_csv_valid(spark, tmp_csv_file):
    reader = ReadFile_by_spark(str(tmp_csv_file))
    df = reader.get_dataframe()
    assert df.count() == 2
    assert "product_name" in df.columns

def test_read_csv_invalid_columns(spark, tmp_invalid_csv_file):
    with pytest.raises(ValueError, match="Недопустимые столбцы"):
        ReadFile_by_spark(str(tmp_invalid_csv_file))

def test_file_not_found():
    with pytest.raises(FileNotFoundError):
        ReadFile_by_spark("non_existent.csv")

def test_invalid_extension(tmp_path):
    file_path = tmp_path / "data.txt"
    file_path.write_text("some,data")
    with pytest.raises(ValueError, match="Формат файла не поддерживается"):
        ReadFile_by_spark(str(file_path))

def test_get_product_category_pairs(spark, product_dataframes):
    products_df, categories_df, product_category_df = product_dataframes
    result_df = ReadFile_by_spark.get_product_category_pairs(products_df, categories_df, product_category_df)

    result = result_df.select("product_name", "category_name", "has_category").collect()

    assert len(result) == 4  # все продукты
    names = [r['product_name'] for r in result]
    assert "Apple Watch" in names
    for row in result:
        if row['product_name'] == "Apple Watch":
            assert row['has_category'] is False
        else:
            assert row['has_category'] is True
