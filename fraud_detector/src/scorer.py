"""Загрузка модели и скоринг транзакций."""
import json
import logging

import pandas as pd
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

MODEL_PATH = './models/model.cbm'
CONFIG_PATH = './models/model_config.json'

logger.info('Importing pretrained model...')

model = CatBoostClassifier()
model.load_model(MODEL_PATH)

# Порог подобран в ноутбуке обучения и сохранён вместе с моделью
with open(CONFIG_PATH) as f:
    model_config = json.load(f)
model_th = model_config['threshold']

logger.info('Pretrained model imported successfully. Threshold: %.4f', model_th)


def make_pred(features: pd.DataFrame, source_info: str = 'kafka') -> pd.DataFrame:
    """Возвращает таблицу с колонками score и fraud_flag."""
    scores = model.predict_proba(features[model_config['features']])[:, 1]

    result = pd.DataFrame({
        'score': scores,
        'fraud_flag': (scores > model_th).astype(int),
    })
    logger.info('Prediction complete for data from %s', source_info)
    return result
