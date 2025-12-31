import pandas as pd
import numpy as np
import os
import logging
from pathlib import Path


logger = logging.getLogger(__name__)



def load_csv(path:str | Path) ->pd.DataFrame:
    path = Path(path)
    if not path.exists():
        logger.warning(f"Файл не найден: {path}")
        raise FileNotFoundError(f"Файл не найден: {path}")
    data_frame = pd.read_csv(path, parse_dates= [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ])

    logger.info(f"Размер датафрейма: {data_frame.shape[0]} строк × {data_frame.shape[1]} колонок")
    logger.debug(f"Колонки: {data_frame.columns}")
    return data_frame


def check_data(data: pd.DataFrame) -> pd.DataFrame:
    copy_data = data.copy()
    if copy_data.duplicated().sum() > 1:
        logger.info(f"Количество дубликатов: {copy_data.duplicated().sum()}")
        copy_data.drop_duplicates(inplace = True)

    english_name_columns = 'product_category_name_english'
    copy_data['product_category_name_english'] = copy_data['product_category_name_english'].replace(['', 'nan', 'None'],
                                                                                                    np.nan)
    copy_data = copy_data.dropna(subset = [english_name_columns])

    reviews_columns = ["review_comment_title", "review_comment_message", "review_answer_timestamp"]
    for col in reviews_columns:
        if col in copy_data.columns:
            copy_data.drop(columns = [col], inplace = True)

    id_cols = ["order_id", "customer_id", "product_id"]

    for col in id_cols:
        if col in copy_data.columns:
            copy_data[col] = copy_data[col].astype(str)

    if all(x in copy_data.columns for x in ["order_purchase_timestamp", "order_approved_at"]):
        copy_data = copy_data[copy_data["order_purchase_timestamp"] <= copy_data["order_approved_at"]]

    if all(x in copy_data.columns for x in ["order_approved_at", "order_delivered_carrier_date"]):
        copy_data = copy_data[
            (copy_data["order_delivered_carrier_date"].isna()) |
            (copy_data["order_delivered_carrier_date"] >= copy_data["order_approved_at"])
        ]

    if all(x in copy_data.columns for x in ["order_delivered_carrier_date", "order_delivered_customer_date"]):
        copy_data = copy_data[
            (copy_data["order_delivered_customer_date"].isna()) |
            (copy_data["order_delivered_customer_date"] >= copy_data["order_delivered_carrier_date"])
            ]

    for col in ["price", "freight_value", "payment_value"]:
        if col in copy_data.columns:
            copy_data = copy_data[copy_data[col] > 0]

    crit_cols = ["order_id", "customer_id", "product_id"]
    for col in crit_cols:
        if col in copy_data.columns:
            copy_data = copy_data[copy_data[col].notna()]


    return copy_data

def save_csv_file(pd: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok = True )
    df.to_csv(path, index = False)



if __name__ == "__main__":
    logging.basicConfig(
        level = logging.DEBUG,
        format = '%(asctime)s %(levelname)s %(name)s:%(lineno)d - %(message)s',
        handlers= [
            logging.FileHandler('py_log.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    path_to_data = '../data/merge_data/result.csv'
    path_to_save_cleaned_df = '../data/processed_clean/processed_data.csv'
    df = load_csv(path_to_data)
    clean_df = check_data(df)


    print(clean_df["order_month"])
    save_csv_file(clean_df, path_to_save_cleaned_df)
