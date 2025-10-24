from pyspark.sql import DataFrame, functions as F

def product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_categories_df: DataFrame,
    *,
    product_id_col: str = "id",
    product_name_col: str = "name",
    category_id_col: str = "id",
    category_name_col: str = "name",
    link_product_id_col: str = "product_id",
    link_category_id_col: str = "category_id",
) -> DataFrame:
    """
    Возвращает один DataFrame с колонками:
      - product_name (строка)
      - category_name (строка или NULL, если у продукта нет категорий)

    Идея:
    - LEFT JOIN от products к связям, затем к categories,
    - не теряем продукты без категорий (NULL),
    - убираем дубликаты пар.

    Параметры *_col позволяют подстроиться под произвольные имена колонок.
    """
    p = products_df.alias("p")
    pc = product_categories_df.alias("pc")
    c = categories_df.alias("c")

    joined = (
        p.join(pc, F.col(f"p.{product_id_col}") == F.col(f"pc.{link_product_id_col}"), "left")
         .join(c, F.col(f"pc.{link_category_id_col}") == F.col(f"c.{category_id_col}"), "left")
         .select(
             F.col(f"p.{product_name_col}").alias("product_name"),
             F.col(f"c.{category_name_col}").alias("category_name"),
         )
    )

    return joined.dropDuplicates(["product_name", "category_name"])
