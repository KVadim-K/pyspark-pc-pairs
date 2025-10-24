import pytest
from pyspark.sql import SparkSession
from pc_pairs import product_category_pairs

# .env (если используется python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- страховка для PySpark: всегда использовать активный Python ---
import os, sys
def _ensure_pyspark_python():
    cur = sys.executable
    for k in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        v = os.environ.get(k)
        if not v or not os.path.isabs(v) or not os.path.exists(v):
            os.environ[k] = cur
_ensure_pyspark_python()
# -----------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pc_pairs_tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def as_set(df):
    return {tuple(x if x is not None else None for x in row) for row in df.collect()}

def test_basic_pairs_and_nulls(spark):
    products = spark.createDataFrame([
        (1, "Тостер"),
        (2, "Чайник"),
        (3, "Блендер"),
        (4, "Гриль"),  # без категорий
    ], ["id", "name"])

    categories = spark.createDataFrame([
        (10, "Кухня"),
        (20, "Электроника"),
        (30, "Премиум"),  # без продуктов — допустимо
    ], ["id", "name"])

    links = spark.createDataFrame([
        (1, 10),
        (1, 20),
        (2, 10),
        (3, 20),
        (1, 20),  # дубль
    ], ["product_id", "category_id"])

    res = product_category_pairs(products, categories, links)

    expected = spark.createDataFrame([
        ("Блендер", "Электроника"),
        ("Гриль",   None),
        ("Тостер",  "Кухня"),
        ("Тостер",  "Электроника"),
        ("Чайник",  "Кухня"),
    ], ["product_name", "category_name"])

    assert as_set(res) == as_set(expected)
    assert res.columns == ["product_name", "category_name"]

def test_custom_column_names(spark):
    products = spark.createDataFrame([
        (1, "P1"),
        (2, "P2"),
    ], ["product_key", "title"])

    categories = spark.createDataFrame([
        (100, "C1"),
    ], ["cat_key", "label"])

    links = spark.createDataFrame([
        (1, 100),
    ], ["p_key", "c_key"])

    res = product_category_pairs(
        products, categories, links,
        product_id_col="product_key",
        product_name_col="title",
        category_id_col="cat_key",
        category_name_col="label",
        link_product_id_col="p_key",
        link_category_id_col="c_key",
    )

    expected = spark.createDataFrame([
        ("P1", "C1"),
        ("P2", None),
    ], ["product_name", "category_name"])

    assert as_set(res) == as_set(expected)

def test_empty_links_returns_all_products_with_nulls(spark):
    products = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])
    categories = spark.createDataFrame([], schema="id INT, name STRING")
    links = spark.createDataFrame([], schema="product_id INT, category_id INT")

    res = product_category_pairs(products, categories, links)
    expected = spark.createDataFrame(
        [("A", None), ("B", None)],
        schema="product_name STRING, category_name STRING"
    )

    assert as_set(res) == as_set(expected)
