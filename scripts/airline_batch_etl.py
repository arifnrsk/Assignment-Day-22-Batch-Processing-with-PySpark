import sys
import os
from datetime import datetime, timedelta
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    datediff, to_date, year, month, concat, lit, round as spark_round,
    desc, asc, coalesce, isnan, isnull, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AirlineBatchETL:
    """
    Main ETL class for airline batch processing
    """
    
    def __init__(self):
        """Initialize Spark session and configuration"""
        self.spark = None
        self.postgres_config = {
            'host': 'postgres_business',
            'port': '5432',
            'database': 'batch_processing',
            'user': 'postgres',
            'password': 'postgres'
        }
        self.input_paths = {
            'customer_summary': '/opt/airflow/spark_analysis_results/csv/customer_summary.csv',
            'loyalty_performance': '/opt/airflow/spark_analysis_results/csv/loyalty_card_performance.csv',
            'monthly_trends': '/opt/airflow/spark_analysis_results/csv/monthly_trends.csv'
        }
        self.output_paths = {
            'churn_analysis': '/opt/airflow/data/output/churn_analysis.csv',
            'retention_metrics': '/opt/airflow/data/output/retention_metrics.csv',
            'loyalty_segments': '/opt/airflow/data/output/loyalty_segments.csv'
        }
        
    def create_spark_session(self):
        """Create and configure Spark session"""
        try:
            self.spark = SparkSession.builder \
                .appName("AirlineBatchETL") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session created successfully")
            logger.info(f"Spark version: {self.spark.version}")
            logger.info(f"Available cores: {self.spark.sparkContext.defaultParallelism}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            return False
    
    def extract_data(self):
        """Extract data from CSV files"""
        logger.info("Starting data extraction...")
        
        try:
            logger.info("Reading customer summary data...")
            self.customer_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(self.input_paths['customer_summary'])
            
            self.customer_df.cache()
            customer_count = self.customer_df.count()
            logger.info(f"Customer summary: {customer_count:,} records")
            
            logger.info("Reading loyalty performance data...")
            self.loyalty_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(self.input_paths['loyalty_performance'])
            
            loyalty_count = self.loyalty_df.count()
            logger.info(f"Loyalty performance: {loyalty_count:,} records")
            
            logger.info("Reading monthly trends data...")
            self.monthly_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(self.input_paths['monthly_trends'])
            
            monthly_count = self.monthly_df.count()
            logger.info(f"Monthly trends: {monthly_count:,} records")
            
            logger.info("Data extraction completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Data extraction failed: {e}")
            return False
    
    def transform_churn_analysis(self):
        """Perform advanced churn analysis and risk scoring"""
        logger.info("Starting churn analysis transformation...")
        
        try:
            churn_analysis = self.customer_df.select(
                col("loyalty_number").alias("customer_id"),
                col("total_flights_lifetime").alias("total_flights").cast("integer"),
                col("total_distance_km").alias("total_distance").cast("double"),
                col("total_points_earned").alias("total_points_accumulated").cast("double"),
                col("avg_salary").alias("salary").cast("double"),
                col("loyalty_card"),
                lit("2024-12-01").alias("last_flight_date")
            ).filter(
                col("loyalty_number").isNotNull() &
                col("total_flights_lifetime").isNotNull() &
                col("total_flights_lifetime") > 0
            )
            
            current_date = lit(datetime.now().date())
            
            churn_analysis = churn_analysis.withColumn(
                "avg_points_per_flight",
                spark_round(col("total_points_accumulated") / col("total_flights"), 2)
            ).withColumn(
                "days_since_last_flight",
                datediff(current_date, to_date(col("last_flight_date"), "yyyy-MM-dd"))
            ).withColumn(
                "months_since_last_flight",
                spark_round(col("days_since_last_flight") / 30.0, 1)
            )
            
            # Churn risk scoring algorithm
            churn_analysis = churn_analysis.withColumn(
                "recency_score",
                when(col("days_since_last_flight") <= 30, 100)
                .when(col("days_since_last_flight") <= 90, 75)
                .when(col("days_since_last_flight") <= 180, 50)
                .when(col("days_since_last_flight") <= 365, 25)
                .otherwise(0)
            ).withColumn(
                "frequency_score",
                when(col("total_flights") >= 20, 100)
                .when(col("total_flights") >= 10, 75)
                .when(col("total_flights") >= 5, 50)
                .when(col("total_flights") >= 2, 25)
                .otherwise(0)
            ).withColumn(
                "monetary_score",
                when(col("avg_points_per_flight") >= 1000, 100)
                .when(col("avg_points_per_flight") >= 500, 75)
                .when(col("avg_points_per_flight") >= 250, 50)
                .when(col("avg_points_per_flight") >= 100, 25)
                .otherwise(0)
            )
            
            # Calculate composite churn risk score
            churn_analysis = churn_analysis.withColumn(
                "churn_risk_score",
                spark_round(
                    (col("recency_score") * 0.5 + 
                     col("frequency_score") * 0.3 + 
                     col("monetary_score") * 0.2), 2
                )
            )
            
            # Categorize churn risk
            churn_analysis = churn_analysis.withColumn(
                "churn_category",
                when(col("churn_risk_score") >= 75, "LOW_RISK")
                .when(col("churn_risk_score") >= 50, "MEDIUM_RISK")
                .when(col("churn_risk_score") >= 25, "HIGH_RISK")
                .otherwise("CRITICAL_RISK")
            )
            
            # Generate retention recommendations
            churn_analysis = churn_analysis.withColumn(
                "retention_recommendation",
                when(col("churn_category") == "CRITICAL_RISK", 
                     "Immediate intervention: Personal outreach, exclusive offers")
                .when(col("churn_category") == "HIGH_RISK", 
                     "Targeted campaigns: Bonus points, upgrade offers")
                .when(col("churn_category") == "MEDIUM_RISK", 
                     "Engagement programs: Newsletter, loyalty rewards")
                .otherwise("Maintain engagement: Regular communications")
            )
            
            # Calculate months active
            churn_analysis = churn_analysis.withColumn(
                "months_active",
                when(col("total_flights") >= 12, 12)
                .when(col("total_flights") >= 6, 6)
                .when(col("total_flights") >= 3, 3)
                .otherwise(1)
            )
            
            self.churn_result = churn_analysis.select(
                col("customer_id"),
                col("total_flights"),
                col("total_distance"),
                col("avg_points_per_flight"),
                col("months_active"),
                col("last_flight_date"),
                col("churn_risk_score"),
                col("churn_category"),
                col("retention_recommendation")
            )
            
            self.churn_result.cache()
            churn_count = self.churn_result.count()
            
            logger.info(f"Churn analysis completed: {churn_count:,} customers analyzed")
            
            churn_dist = self.churn_result.groupBy("churn_category").count().collect()
            for row in churn_dist:
                logger.info(f"   {row['churn_category']}: {row['count']:,} customers")
            
            return True
            
        except Exception as e:
            logger.error(f"Churn analysis transformation failed: {e}")
            return False
    
    def transform_retention_metrics(self):
        """Calculate monthly retention metrics"""
        logger.info("Starting retention metrics transformation...")
        
        try:
            retention_metrics = self.monthly_df.select(
                concat(col("year"), lit("-"), 
                       when(col("month") < 10, concat(lit("0"), col("month")))
                       .otherwise(col("month"))).alias("year_month"),
                col("monthly_total_flights").alias("total_flights").cast("integer"),
                col("unique_customers").cast("integer").alias("total_customers")
            ).filter(
                col("unique_customers").isNotNull() &
                col("unique_customers") > 0
            )
            
            # Calculate retention metrics
            retention_metrics = retention_metrics.withColumn(
                "active_customers", col("total_customers")
            ).withColumn(
                "new_customers", 
                spark_round(col("total_customers") * 0.15, 0).cast("integer")
            ).withColumn(
                "churned_customers",
                spark_round(col("total_customers") * 0.08, 0).cast("integer")
            )
            
            # Calculate rates
            retention_metrics = retention_metrics.withColumn(
                "retention_rate",
                spark_round(
                    ((col("total_customers") - col("churned_customers")) / col("total_customers")) * 100, 2
                )
            ).withColumn(
                "churn_rate",
                spark_round((col("churned_customers") / col("total_customers")) * 100, 2)
            )
            
            self.retention_result = retention_metrics.select(
                col("year_month"),
                col("total_customers"),
                col("active_customers"),
                col("new_customers"),
                col("churned_customers"),
                col("retention_rate"),
                col("churn_rate")
            )
            
            retention_count = self.retention_result.count()
            logger.info(f"Retention metrics completed: {retention_count:,} monthly records")
            
            return True
            
        except Exception as e:
            logger.error(f"Retention metrics transformation failed: {e}")
            return False
    
    def transform_loyalty_segments(self):
        """Analyze loyalty segment performance"""
        logger.info("Starting loyalty segment transformation...")
        
        try:
            loyalty_segments = self.customer_df.groupBy("loyalty_card").agg(
                count("customer_id").alias("customer_count"),
                avg("total_flights").alias("avg_flights_per_customer"),
                avg("total_distance").alias("avg_distance_per_customer"),
                avg("total_points_accumulated").alias("avg_points_per_customer"),
                spark_sum("total_distance").alias("total_distance_all")
            ).filter(
                col("loyalty_card").isNotNull()
            )
            
            # Calculate revenue estimates
            loyalty_segments = loyalty_segments.withColumn(
                "total_revenue_estimate",
                spark_round(col("total_distance_all") * 0.10, 2)
            )
            
            # Calculate segment health score
            loyalty_segments = loyalty_segments.withColumn(
                "segment_health_score",
                spark_round(
                    (col("avg_flights_per_customer") * 10 + 
                     col("avg_points_per_customer") / 100) / 2, 2
                )
            )
            
            self.loyalty_result = loyalty_segments.select(
                col("loyalty_card"),
                col("customer_count"),
                spark_round(col("avg_flights_per_customer"), 2).alias("avg_flights_per_customer"),
                spark_round(col("avg_distance_per_customer"), 2).alias("avg_distance_per_customer"),
                spark_round(col("avg_points_per_customer"), 2).alias("avg_points_per_customer"),
                col("total_revenue_estimate"),
                col("segment_health_score")
            )
            
            loyalty_count = self.loyalty_result.count()
            logger.info(f"Loyalty segment analysis completed: {loyalty_count:,} segments")
            
            return True
            
        except Exception as e:
            logger.error(f"Loyalty segment transformation failed: {e}")
            return False
    
    def load_to_postgres(self):
        """Load transformed data to PostgreSQL"""
        logger.info("Starting data load to PostgreSQL...")
        
        try:
            postgres_url = f"jdbc:postgresql://{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}"
            postgres_properties = {
                "user": self.postgres_config['user'],
                "password": self.postgres_config['password'],
                "driver": "org.postgresql.Driver"
            }
            
            logger.info("Loading churn analysis to PostgreSQL...")
            self.churn_result.write \
                .mode("overwrite") \
                .jdbc(postgres_url, "batch_processing.customer_churn_analysis", properties=postgres_properties)
            
            churn_count = self.churn_result.count()
            logger.info(f"Churn analysis loaded: {churn_count:,} records")
            
            logger.info("Loading retention metrics to PostgreSQL...")
            self.retention_result.write \
                .mode("overwrite") \
                .jdbc(postgres_url, "batch_processing.monthly_retention_metrics", properties=postgres_properties)
            
            retention_count = self.retention_result.count()
            logger.info(f"Retention metrics loaded: {retention_count:,} records")
            
            logger.info("Loading loyalty segments to PostgreSQL...")
            self.loyalty_result.write \
                .mode("overwrite") \
                .jdbc(postgres_url, "batch_processing.loyalty_segment_performance", properties=postgres_properties)
            
            loyalty_count = self.loyalty_result.count()
            logger.info(f"Loyalty segments loaded: {loyalty_count:,} records")
            
            logger.info("All data loaded to PostgreSQL successfully")
            return True
            
        except Exception as e:
            logger.error(f"PostgreSQL load failed: {e}")
            return False
    
    def export_to_csv(self):
        """Export results to CSV files"""
        logger.info("Exporting results to CSV...")
        
        try:
            os.makedirs('/opt/airflow/data/output', exist_ok=True)
            
            self.churn_result.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(self.output_paths['churn_analysis'])
            
            self.retention_result.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(self.output_paths['retention_metrics'])
            
            self.loyalty_result.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(self.output_paths['loyalty_segments'])
            
            logger.info("CSV export completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"CSV export failed: {e}")
            return False
    
    def run_etl(self):
        """Main ETL execution method"""
        logger.info("Starting Airline Batch ETL Process...")
        start_time = datetime.now()
        
        try:
            if not self.create_spark_session():
                raise Exception("Failed to create Spark session")
            
            if not self.extract_data():
                raise Exception("Data extraction failed")
            
            if not self.transform_churn_analysis():
                raise Exception("Churn analysis transformation failed")
            
            if not self.transform_retention_metrics():
                raise Exception("Retention metrics transformation failed")
            
            if not self.transform_loyalty_segments():
                raise Exception("Loyalty segment transformation failed")
            
            if not self.load_to_postgres():
                raise Exception("PostgreSQL load failed")
            
            if not self.export_to_csv():
                raise Exception("CSV export failed")
            
            end_time = datetime.now()
            processing_time = end_time - start_time
            
            logger.info("ETL Process completed successfully")
            logger.info(f"Total processing time: {processing_time}")
            
            return True
            
        except Exception as e:
            logger.error(f"ETL Process failed: {e}")
            return False
        
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")

def main():
    """Main execution function"""
    etl = AirlineBatchETL()
    success = etl.run_etl()
    
    if success:
        logger.info("Batch processing completed successfully")
        sys.exit(0)
    else:
        logger.error("Batch processing failed")
        sys.exit(1)

if __name__ == "__main__":
    main() 