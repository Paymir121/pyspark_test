from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession


class SparkSessionSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.session =  SparkSession.builder.appName("ExampleApp").getOrCreate()

def get_products_with_category_and_products_without_categories(
        products_df: DataFrame,
        categories_df: DataFrame,
        product_category_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    product_with_categories: DataFrame = products_df.join(
        product_category_df, 'product_id', 'left'
    ).join(categories_df, 'category_id', 'left'
           ).select(products_df.product_name, categories_df.category_name)
    products_without_categories: DataFrame = products_df.join(
        product_category_df, 'product_id', 'left_anti'
    ).select(products_df.product_name)

    return (product_with_categories, products_without_categories)


if __name__ == "__main__":
    spark: SparkSessionSingleton = SparkSessionSingleton()
    products: list[tuple[int, str]] = [
        (1, "Чаппи"),
        (2, "Педдигри"),
        (3, "Бог Император"),
        (4, "Морковка"),
        (5, "Тесла")
    ]
    categories: list[tuple[int, str]] = [
        (1, "Бог"),
        (2, "Еда")
    ]
    product_category: list[tuple[int, int]] = [
        (1, 2),
        (2, 2),
        (3, 1),
        (4, 2)
    ]

    products_df = spark.session.createDataFrame(
        data=products,
        schema=["product_id", "product_name"]
    )
    categories_df = spark.session.createDataFrame(
        data=categories,
        schema=["category_id", "category_name"]
    )
    product_category_df = spark.session.createDataFrame(
        data=product_category,
        schema=["product_id", "category_id"]
    )

    pairs, no_category_products = get_products_with_category_and_products_without_categories(products_df, categories_df, product_category_df)
    pairs.show()
    no_category_products.show()