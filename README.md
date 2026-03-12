# Electricity Load Forecast 
This project performs time series forecasting to predict daily electricity load demand in Turkey. Historical and real-time electricity load data is sourced from [**EPİAŞ (Transparency Platform)**](https://seffaflik.epias.com.tr/home). Forecasts are generated on a daily basis using a trained machine learning model, with the workflow orchestrated through GitHub Actions.

[![Workflow](https://github.com/boramertkesil/electricity-ts-forecast/actions/workflows/etl_predict_report.yaml/badge.svg)](https://github.com/boramertkesil/electricity-ts-forecast/actions)


## ⚡ Live Forecast
The figures below show the day-ahead electricity load forecast for Turkey and a comparison of actual vs forecasted load over the past seven days.


### Day‑Ahead Forecast
![Day‑Ahead Forecast](https://ts-forecast-boramkesil.s3.eu-north-1.amazonaws.com/reports/forecast_day_ahead.png)

---

### Actual vs Forecast — Last 7 Days
![Actual vs Forecast Last 7 Days](https://ts-forecast-boramkesil.s3.eu-north-1.amazonaws.com/reports/actual_vs_forecast_last_7_days.png)


## 📖 Readings

The repository includes two Jupyter notebooks, the first notebook explains how data is gathered from various sources and stored in AWS S3, along with some exploratory analysis. The second notebook demonstrates how the forecasting model and features are built and trained for day-ahead load prediction.

1. [Data Sources & Storage Notebook](data-sources-and-storage.ipynb)  
2. [Model Training Notebook](model-training.ipynb)

