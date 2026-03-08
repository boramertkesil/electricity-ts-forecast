import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from typing import Mapping

class WeightedAverage(BaseEstimator, TransformerMixin):
    def __init__(self, weight_map: Mapping[str, float], output_name="weighted_average", drop_original=False):
        self.weight_map = weight_map
        self.output_name = output_name
        self.drop_original = drop_original

    def fit(self, X: pd.DataFrame, y=None):
        if not isinstance(X, pd.DataFrame):
            raise TypeError("WeightedAverage expects a pandas DataFrame as input.")
        self.cols_ = list(self.weight_map.keys())

        missing = [c for c in self.cols_ if c not in X.columns]
        if missing:
            raise ValueError(f"Missing columns in X: {missing}")

        self.w_ = np.array([self.weight_map[c] for c in self.cols_], dtype=float)
        return self

    def transform(self, X: pd.DataFrame):
        X_sub = X.loc[:, self.cols_].astype(float)
        X[self.output_name] = X_sub.dot(self.w_) / np.sum(self.w_)

        if self.drop_original:
            X = X.drop(columns=self.cols_)
        return X
    
    def get_feature_names_out(self, input_features=None):
        return np.array([self.output_name])