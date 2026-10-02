"""Предобработка транзакций для модели.

Этот же модуль используется и в ноутбуке обучения (notebooks/train_model.ipynb),
и в сервисе. Поэтому признаки при обучении и при скоринге гарантированно одинаковые.

Предобработка не зависит от тренировочных данных: каждую транзакцию можно
обработать отдельно, сразу как она пришла из Kafka.
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Категориальные признаки: CatBoost кодирует их сам
CAT_FEATURES = ['merch', 'cat_id', 'gender', 'one_city', 'us_state', 'jobs']

# Числовые признаки
NUM_FEATURES = [
    'amount_log',
    'population_city_log',
    'lat', 'lon',
    'merchant_lat', 'merchant_lon',
    'distance_km',
    'hour',
    'day_of_week',
]

# Итоговый список признаков в том порядке, в котором их ждёт модель
FEATURES = CAT_FEATURES + NUM_FEATURES

EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1, lon1, lat2, lon2):
    """Расстояние по поверхности Земли между двумя точками, в километрах."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = (np.sin((lat2 - lat1) / 2) ** 2
         + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2)
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def run_preproc(input_df: pd.DataFrame) -> pd.DataFrame:
    """Превращает сырые транзакции (формат test.csv) в признаки для модели.

    Работает как для одной транзакции, так и для целой таблицы.
    """
    df = input_df.copy()

    # Категориальные признаки: приводим к строке, пропуски заменяем на 'NA'
    for col in CAT_FEATURES:
        df[col] = df[col].fillna('NA').astype(str)

    # Числовые колонки из исходных данных
    for col in ['amount', 'population_city', 'lat', 'lon', 'merchant_lat', 'merchant_lon']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Временные признаки
    time = pd.to_datetime(df['transaction_time'], errors='coerce')
    df['hour'] = time.dt.hour
    df['day_of_week'] = time.dt.dayofweek

    # Логарифмы у признаков с большим разбросом значений
    df['amount_log'] = np.log1p(df['amount'].clip(lower=0))
    df['population_city_log'] = np.log1p(df['population_city'].clip(lower=0))

    # Расстояние от клиента до продавца
    df['distance_km'] = haversine_km(
        df['lat'], df['lon'], df['merchant_lat'], df['merchant_lon']
    )

    logger.debug('Preprocessing completed. Output shape: %s', df[FEATURES].shape)
    return df[FEATURES]
