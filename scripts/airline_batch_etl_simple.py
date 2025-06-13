import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum as spark_sum, count, lit, round as spark_round, concat
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime

def main():
    print("Starting Airline Batch ETL Process...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("AirlineBatchETL") \
        .getOrCreate()
    
    try:
        print("Reading customer summary data...")
        customer_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("/opt/airflow/spark_analysis_results/csv/customer_summary.csv")
        
        print(f"Customer data loaded: {customer_df.count()} records")
        
        # Simple churn analysis
        churn_analysis = customer_df.select(
            col("loyalty_number").alias("customer_id"),
            col("total_flights_lifetime").alias("total_flights").cast(IntegerType()),
            col("total_distance_km").alias("total_distance").cast(DoubleType()),
            col("total_points_earned").alias("total_points").cast(DoubleType()),
            col("loyalty_card")
        ).filter(
            col("loyalty_number").isNotNull() &
            col("total_flights_lifetime").isNotNull() &
            col("total_flights_lifetime") > 0
        )
        
        # Calculate simple metrics
        churn_analysis = churn_analysis.withColumn(
            "avg_points_per_flight",
            spark_round(col("total_points") / col("total_flights"), 2)
        ).withColumn(
            "churn_risk_score",
            when(col("total_flights") >= 20, 90.0)
            .when(col("total_flights") >= 10, 70.0)
            .when(col("total_flights") >= 5, 50.0)
            .otherwise(30.0)
        ).withColumn(
            "churn_category",
            when(col("churn_risk_score") >= 80, "LOW_RISK")
            .when(col("churn_risk_score") >= 60, "MEDIUM_RISK")
            .otherwise("HIGH_RISK")
        )
        
        print(f"Churn analysis completed: {churn_analysis.count()} customers")
        
        # Show sample results
        print("Sample churn analysis results:")
        churn_analysis.show(5)
        
        # Save to CSV
        print("Saving results to CSV...")
        churn_analysis.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv("/opt/airflow/data/output/churn_analysis_simple")
        
        print("ETL Process completed successfully!")
        
    except Exception as e:
        print(f"ETL Process failed: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 