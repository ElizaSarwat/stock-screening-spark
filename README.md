# Big Data Stock Screening Pipeline
**MSc Big Data | University of Glasgow | March 2026**

A large-scale stock analysis pipeline built with Apache Spark and Java, designed to screen and rank investment candidates based on financial indicators computed across historical price data.

---

## Project Overview

This pipeline processes large volumes of historical stock price data to identify the best-performing, low-volatility investment candidates. It filters stocks based on financial thresholds (volatility, P/E ratio) and ranks them by 5-day return.

**Business question answered:** *Which stocks have the best short-term returns while staying within acceptable risk and valuation limits?*

---

## What It Does

1. **Loads** stock price data (CSV) and asset metadata (JSON) using Apache Spark
2. **Cleans** data by filtering out null prices and incomplete records
3. **Computes** two financial indicators per stock:
   - **5-day Return** — short-term price performance
   - **Annualised Volatility** — risk measure based on daily price changes
4. **Filters** stocks using configurable thresholds:
   - Volatility below 4% ceiling
   - P/E ratio below 25 (value filter)
5. **Joins** price features with asset metadata (name, industry, sector)
6. **Ranks** remaining candidates by return (descending)
7. **Outputs** a ranked list of investment candidates to CSV

---

## Tech Stack

| Tool | Use |
|------|-----|
| Java | Core application logic |
| Apache Spark | Distributed data processing |
| Spark SQL / RDD API | Data transformation and filtering |
| JavaPairRDD | Key-value pair operations |
| JSON + CSV | Input data formats |

---

## Project Structure

```
src/
├── bigdata/
│   ├── app/
│   │   └── AssessedExercise.java        # Main pipeline entry point
│   ├── objects/
│   │   ├── Asset.java                   # Asset data model
│   │   ├── AssetFeatures.java           # Feature container
│   │   ├── AssetMetadata.java           # Metadata model (name, sector, P/E)
│   │   ├── AssetRanking.java            # Ranking output model
│   │   └── StockPrice.java              # Stock price data model
│   ├── technicalindicators/
│   │   ├── Returns.java                 # 5-day return calculation
│   │   └── Volitility.java              # Annualised volatility calculation
│   ├── transformations/
│   │   ├── filters/
│   │   │   └── NullPriceFilter.java     # Removes null price rows
│   │   ├── maps/
│   │   │   └── PriceReaderMap.java      # Parses CSV rows to StockPrice objects
│   │   └── pairing/
│   │       └── AssetMetadataPairing.java # Pairs asset metadata by ticker
│   └── util/
│       ├── MathUtils.java               # Statistical utility functions
│       └── TimeUtil.java                # Date/time parsing utilities
```

---

## Key Configuration

Set via environment variables or defaults in code:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BIGDATA_PRICES` | `resources/all_prices-noHead.csv` | Stock price CSV file |
| `BIGDATA_ASSETS` | `resources/stock_data.json` | Asset metadata JSON file |
| `SPARK_MASTER` | `local[4]` | Spark master (local or cluster) |
| `volatilityCeiling` | `4.0` | Maximum allowed annualised volatility |
| `peRatioThreshold` | `25.0` | Maximum allowed P/E ratio |
| `datasetEndDate` | `2020-04-01` | End date for price data |

---

## How to Run

**Requirements:** Java 8+, Apache Spark

```bash
# Compile
javac -cp spark-jars/* src/bigdata/**/*.java

# Run locally
spark-submit --class bigdata.app.AssessedExercise target/bigdata.jar
```

Or run directly in an IDE with Spark dependencies configured.

---

