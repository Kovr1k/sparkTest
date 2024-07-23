import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("Product Category Example") \
    .getOrCreate()

# Пример данных
products_data = [
    (1, "Product A"),
    (2, "Product B"),
    (3, "Product C"),
    (4, "Product D")
]

categories_data = [
    (1, "Category X"),
    (2, "Category Y"),
    (3, "Category Z")
]

product_categories_data = [
    (1, 1),
    (1, 2),
    (2, 2),
    (3, 3)
]

# Создание датафреймов
products = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_categories = spark.createDataFrame(product_categories_data, ["product_id", "category_id"])

def get_product_category_pairs_and_orphans(products, categories, product_categories):
    # Join для получения пар "Имя продукта – Имя категории"
    product_category_pairs = product_categories.join(products, "product_id") \
                                               .join(categories, "category_id") \
                                               .select("product_name", "category_name")
    
    # Left anti join для получения продуктов без категорий
    products_with_categories = product_categories.select("product_id").distinct()
    products_without_categories = products.join(products_with_categories, 
                                                products.product_id == products_with_categories.product_id, 
                                                "leftanti") \
                                          .select("product_name")
    
    return product_category_pairs, products_without_categories

# Получение результатов
product_category_pairs, products_without_categories = get_product_category_pairs_and_orphans(products, categories, product_categories)

# Показать результаты
product_category_pairs.show()
products_without_categories.show()