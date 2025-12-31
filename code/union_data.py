import pandas as pd

path_rows = '../data/rows/'
path_to_save = '../data/merge_data/result.csv'

orders = pd.read_csv(path_rows + 'olist_orders_dataset.csv')
order_items = pd.read_csv(path_rows + 'olist_order_items_dataset.csv')
customers = pd.read_csv(path_rows + 'olist_customers_dataset.csv')
products = pd.read_csv(path_rows + 'olist_products_dataset.csv')
payments = pd.read_csv(path_rows + 'olist_order_payments_dataset.csv')
reviews = pd.read_csv(path_rows + 'olist_order_reviews_dataset.csv')
sellers = pd.read_csv(path_rows + 'olist_sellers_dataset.csv')
category_translation = pd.read_csv(path_rows + 'product_category_name_translation.csv')

orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])
orders["order_approved_at"] = pd.to_datetime(orders["order_approved_at"])
orders["order_delivered_carrier_date"] = pd.to_datetime(orders["order_delivered_carrier_date"])
orders["order_delivered_customer_date"] = pd.to_datetime(orders["order_delivered_customer_date"])
orders["order_estimated_delivery_date"] = pd.to_datetime(orders["order_estimated_delivery_date"])

products = products.merge(category_translation, on="product_category_name", how="left")


order_items_agg = order_items.groupby("order_id").agg(
    total_items=("order_item_id", "count"),
    total_price=("price", "sum"),
    total_freight=("freight_value", "sum")
).reset_index()


payments_agg = payments.groupby("order_id").agg(
    total_payment=("payment_value", "sum"),
    num_payments=("payment_installments", "sum")
).reset_index()

reviews = reviews.drop_duplicates(subset=["review_id"])
reviews_agg = reviews.groupby("order_id").agg(
    review_score=("review_score", "first")
).reset_index()


df = orders \
    .merge(order_items_agg, on="order_id", how="left") \
    .merge(payments_agg, on="order_id", how="left") \
    .merge(reviews_agg, on="order_id", how="left") \
    .merge(customers, on="customer_id", how="left")

df = df.merge(order_items[['order_id', 'product_id', 'price', 'freight_value']], on='order_id', how='left')
df = df.merge(products, on='product_id', how='left')


df['revenue'] = df['total_price'] + df['total_freight']
df['order_month'] = df["order_purchase_timestamp"].dt.to_period("M")


df.to_csv(path_to_save, sep=',', index=False)

