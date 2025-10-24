# demo/demo.py — самодостаточный импорт из src
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

# .env (если используется python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- страховка для PySpark: всегда использовать активный Python ---
def _ensure_pyspark_python():
    cur = sys.executable
    for k in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        v = os.environ.get(k)
        if not v or not os.path.isabs(v) or not os.path.exists(v):
            os.environ[k] = cur
_ensure_pyspark_python()
# -----------------------------------------------------------------

from pyspark.sql import SparkSession
from pc_pairs import product_category_pairs


def main():
    # локальная сессия для примера
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pc_pairs_demo")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    products = spark.createDataFrame(
        [(1, "Тостер"), (2, "Чайник"), (3, "Блендер"), (4, "Гриль")],
        ["id", "name"],
    )
    categories = spark.createDataFrame(
        [(10, "Кухня"), (20, "Электроника")],
        ["id", "name"],
    )
    links = spark.createDataFrame(
        [(1, 10), (1, 20), (2, 10), (3, 20), (1, 20)],  # последний дубль намеренно
        ["product_id", "category_id"],
    )

    res = product_category_pairs(products, categories, links)
    res.orderBy("product_name", "category_name").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
