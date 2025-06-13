# Batch Processing with Apache Airflow & PySpark

**Assignment Day 22 - Data Engineer Bootcamp**  

## Project Overview

This project implements a comprehensive **Batch Processing Pipeline** using **Apache Airflow** and **PySpark** to process airline customer data. The pipeline performs advanced ETL operations, conducts churn-retention analysis, and stores results in multiple formats (PostgreSQL, CSV, Console).

### Assignment Requirements Completed

1. **Infrastructure Setup**: Apache Airflow + PySpark containerized environment
2. **Airflow DAG Creation**: 5-task automated pipeline workflow
3. **ETL Implementation**: Extract, Transform, Load with advanced analytics
4. **PostgreSQL Integration**: Read/write operations with data persistence
5. **Output Generation**: Console print, CSV files, and PostgreSQL tables

### Key Features

- **Dockerized Environment**: Complete containerized setup with 6 services
- **Apache Airflow**: Workflow orchestration with 5-task DAG
- **Advanced Analytics**: Churn analysis, retention metrics, loyalty segmentation
- **PostgreSQL Integration**: Dual database setup for metadata and business data
- **Multi-format Output**: CSV, PostgreSQL tables, and console reporting
- **Production Ready**: Comprehensive error handling and monitoring

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Airflow       │    │   PySpark       │    │   PostgreSQL    │
│   Scheduler     │───▶│   Processing    │───▶│   Database      │
│   & Webserver   │    │   Engine        │    │   Storage       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Input Data    │
                    │   (Day 21       │
                    │   Analysis)     │
                    └─────────────────┘
```

## Quick Start with Docker

### Prerequisites

- Docker Desktop installed
- Docker Compose v2.0+
- At least 4GB RAM available
- At least 2 CPU cores

### 1. Clone Repository

```bash
# Clone the repository
git clone https://github.com/arifnrsk/Assignment-Day-22-Batch-Processing-with-PySpark.git
cd Assignment-Day-22-Batch-Processing-with-PySpark
```

### 2. Environment Setup

The project includes a pre-configured `.env` file with all necessary environment variables. No additional setup required.

### 2. Start Services

```bash
# Start all services (no additional setup required)
docker-compose up -d

# Check services status
docker-compose ps
```

### 3. Access Applications

- **Airflow Web UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`
- **Spark Master UI**: http://localhost:8081
- **PostgreSQL**: localhost:5432 (Airflow) / localhost:5433 (Business)

## Services Overview

| Service | Port | Description |
|---------|------|-------------|
| airflow-webserver | 8080 | Airflow Web Interface |
| airflow-scheduler | - | DAG Scheduler |
| postgres | 5432 | Airflow Metadata DB |
| postgres_business | 5433 | Business Data DB |
| spark-master | 8081 | Spark Master UI |
| spark-worker | - | Spark Worker Node |

## Batch Processing Pipeline

### ETL Workflow

1. **Extract**: Load processed data from Day 21 analysis
2. **Transform**: 
   - Customer segmentation
   - Churn risk analysis
   - Retention cohort analysis
   - Monthly trend aggregation
3. **Load**: Store results in PostgreSQL tables

### Database Schema

```sql
-- Main Tables
├── customer_summary          # Customer lifetime metrics
├── monthly_trends           # Time-series analysis
├── loyalty_card_performance # Loyalty program analysis
├── churn_analysis          # Customer churn predictions
├── retention_analysis      # Cohort retention rates
└── batch_processing_log    # Pipeline execution logs
```

## Project Structure

```
Assignment Day 22 - Batch Processing with PySpark/
├── docker-compose.yml                    # Main orchestration
├── Dockerfile                            # Custom Airflow image
├── requirements.txt                      # Python dependencies
├── README.md                             # Project documentation
├── BATCH_PROCESSING_DOCUMENTATION.md    # Comprehensive assignment docs
│
├── dags/                                 # Airflow DAGs
│   └── airline_batch_processing_dag.py     # Main 5-task pipeline DAG
│
├── scripts/                              # Setup Scripts
│   └── setup_airflow_connections.py        # Airflow connections setup
│
├── sql/                                  # Database Scripts
│   └── create_tables.sql                   # PostgreSQL table creation
│
├── config/                               # Configuration
│   └── airflow.cfg                         # Airflow settings
│
├── data/                                 # Data Directory
│   ├── input/                              # Input CSV files (Day 21 results)
│   └── output/                             # Generated CSV outputs
│
├── logs/                                 # Airflow execution logs
├── plugins/                              # Airflow custom plugins
└── artifacts/                            # Assignment artifacts
```

## Assignment Deliverables

### Required Files Included:

1. **Airflow DAGs**: `dags/airline_batch_processing_dag.py`
2. **PySpark Scripts**: ETL logic integrated in DAG
3. **PostgreSQL Queries**: SQL operations within pipeline
4. **Documentation**: `BATCH_PROCESSING_DOCUMENTATION.md`

### Output Results:

- **Console Print**: Comprehensive summary reports in Airflow logs
- **CSV Files**: 3 analysis files (churn, retention, loyalty)
- **PostgreSQL Tables**: 3 tables with processed analytics data

## Running the Pipeline

### 1. Start the Environment

```bash
# Start all services
docker-compose up -d

# Wait for initialization (2-3 minutes)
docker-compose logs -f airflow-init
```

### 2. Access Airflow UI

1. Open http://localhost:8080
2. Login with `admin` / `admin`
3. Navigate to DAGs page
4. Find `batch_processing_pipeline` DAG

### 3. Execute Pipeline

```bash
# Trigger DAG manually
docker-compose exec airflow-webserver airflow dags trigger batch_processing_pipeline

# Or use the UI to trigger
```

### 4. Monitor Execution

- **Airflow UI**: Monitor task status and logs
- **Spark UI**: View Spark job execution
- **Database**: Check results in PostgreSQL

## Analysis Results

After running the pipeline, you can analyze the results using the following methods:

### 1. Check CSV Output Files

```bash
# List generated CSV files
docker exec assignmentday22-batchprocessingwithpyspark-airflow-webserver-1 \
  ls -la /opt/airflow/data/output/

# View sample churn analysis data
docker exec assignmentday22-batchprocessingwithpyspark-airflow-webserver-1 \
  head -10 /opt/airflow/data/output/churn_analysis.csv

# View loyalty segments data
docker exec assignmentday22-batchprocessingwithpyspark-airflow-webserver-1 \
  cat /opt/airflow/data/output/loyalty_segments.csv
```

### 2. Query PostgreSQL Database

**Connect to database:**
```bash
# Access PostgreSQL container
docker exec -it assignmentday22-batchprocessingwithpyspark-postgres_business-1 \
  psql -U postgres -d batch_processing
```

**Ready-to-use SQL queries:**

#### Customer Churn Analysis
```sql
-- View churn distribution
SELECT churn_category, COUNT(*) as customer_count, 
       ROUND(AVG(churn_risk_score), 2) as avg_risk_score
FROM batch_processing.customer_churn_analysis 
GROUP BY churn_category 
ORDER BY avg_risk_score DESC;

-- High-risk customers (copy-paste ready)
SELECT customer_id, total_flights, churn_risk_score, retention_recommendation
FROM batch_processing.customer_churn_analysis 
WHERE churn_category = 'HIGH_RISK'
ORDER BY churn_risk_score DESC
LIMIT 10;

-- Customer analysis by flight frequency
SELECT 
  CASE 
    WHEN total_flights >= 20 THEN '20+ flights'
    WHEN total_flights >= 10 THEN '10-19 flights'
    WHEN total_flights >= 5 THEN '5-9 flights'
    ELSE 'Under 5 flights'
  END as flight_category,
  COUNT(*) as customers,
  ROUND(AVG(churn_risk_score), 2) as avg_risk
FROM batch_processing.customer_churn_analysis
GROUP BY flight_category
ORDER BY avg_risk DESC;
```

#### Monthly Retention Metrics
```sql
-- View retention trends (copy-paste ready)
SELECT year_month, retention_rate, churn_rate, total_customers
FROM batch_processing.monthly_retention_metrics 
ORDER BY year_month;

-- Best and worst performing months
(SELECT 'Best Month' as type, year_month, retention_rate 
 FROM batch_processing.monthly_retention_metrics 
 ORDER BY retention_rate DESC LIMIT 1)
UNION ALL
(SELECT 'Worst Month' as type, year_month, retention_rate 
 FROM batch_processing.monthly_retention_metrics 
 ORDER BY retention_rate ASC LIMIT 1);
```

#### Loyalty Segment Performance
```sql
-- Loyalty tier comparison (copy-paste ready)
SELECT loyalty_card, customer_count, 
       ROUND(avg_flights_per_customer, 2) as avg_flights,
       ROUND(total_revenue_estimate, 2) as revenue_estimate,
       ROUND(segment_health_score, 2) as health_score
FROM batch_processing.loyalty_segment_performance 
ORDER BY segment_health_score DESC;

-- Revenue per customer by tier
SELECT loyalty_card,
       ROUND(total_revenue_estimate / customer_count, 2) as revenue_per_customer
FROM batch_processing.loyalty_segment_performance 
ORDER BY revenue_per_customer DESC;
```

### 3. Business Intelligence Queries

#### Cross-Analysis Queries
```sql
-- Customer value segmentation
SELECT 
  CASE 
    WHEN total_flights >= 30 THEN 'VIP'
    WHEN total_flights >= 15 THEN 'Premium'
    WHEN total_flights >= 5 THEN 'Regular'
    ELSE 'Occasional'
  END as customer_segment,
  COUNT(*) as customers,
  ROUND(AVG(total_distance), 0) as avg_distance,
  ROUND(AVG(avg_points_per_flight), 0) as avg_points_per_flight
FROM batch_processing.customer_churn_analysis
GROUP BY customer_segment
ORDER BY avg_distance DESC;

-- Risk vs Activity Analysis
SELECT churn_category,
       ROUND(AVG(total_flights), 1) as avg_flights,
       ROUND(AVG(total_distance), 0) as avg_distance,
       COUNT(*) as customer_count
FROM batch_processing.customer_churn_analysis
GROUP BY churn_category
ORDER BY avg_flights DESC;
```

### 4. Export Results

```bash
# Export churn analysis to local CSV
docker exec assignmentday22-batchprocessingwithpyspark-postgres_business-1 \
  psql -U postgres -d batch_processing -c \
  "COPY (SELECT * FROM batch_processing.customer_churn_analysis) TO STDOUT WITH CSV HEADER" \
  > local_churn_analysis.csv

# Export retention metrics
docker exec assignmentday22-batchprocessingwithpyspark-postgres_business-1 \
  psql -U postgres -d batch_processing -c \
  "COPY (SELECT * FROM batch_processing.monthly_retention_metrics) TO STDOUT WITH CSV HEADER" \
  > local_retention_metrics.csv
```

### 5. Quick Data Validation

```bash
# Check record counts (copy-paste ready)
docker exec assignmentday22-batchprocessingwithpyspark-postgres_business-1 \
  psql -U postgres -d batch_processing -c "
SELECT 
  'customer_churn_analysis' as table_name, COUNT(*) as records 
FROM batch_processing.customer_churn_analysis
UNION ALL
SELECT 
  'monthly_retention_metrics' as table_name, COUNT(*) as records 
FROM batch_processing.monthly_retention_metrics
UNION ALL
SELECT 
  'loyalty_segment_performance' as table_name, COUNT(*) as records 
FROM batch_processing.loyalty_segment_performance;"
```

## Development

### Adding New Analysis

1. Create PySpark script in `scripts/`
2. Add task to DAG in `dags/batch_processing_dag.py`
3. Update database schema if needed
4. Test with `docker-compose restart`

### Custom Configuration

- **Airflow**: Modify `config/airflow.cfg`
- **Spark**: Update environment variables in `docker-compose.yml`
- **Database**: Add tables in `sql/init_database.sql`

## Troubleshooting

### Common Issues

**1. Services not starting**
```bash
# Check logs
docker-compose logs airflow-webserver
docker-compose logs postgres

# Restart services
docker-compose restart
```

**2. Permission issues**
```bash
# Fix permissions
sudo chown -R 50000:0 logs/ plugins/
```

**3. Database connection errors**
```bash
# Check PostgreSQL status
docker-compose exec postgres pg_isready -U airflow

# Reset database
docker-compose down -v
docker-compose up -d
```

**4. Spark connection issues**
```bash
# Check Spark master
docker-compose logs spark-master

# Restart Spark cluster
docker-compose restart spark-master spark-worker
```

## Performance Tuning

### Spark Configuration

```yaml
# In docker-compose.yml
environment:
  - SPARK_WORKER_MEMORY=4G      # Increase for large datasets
  - SPARK_WORKER_CORES=4        # Match your CPU cores
```

### Airflow Configuration

```ini
# In config/airflow.cfg
parallelism = 32                # Increase for more concurrent tasks
dag_concurrency = 16           # Tasks per DAG
```

## Security Notes

- Default passwords are for development only
- Change credentials in production
- Use environment variables for sensitive data
- Enable SSL for production deployments

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## License

This project is for educational purposes as part of the Data Engineer Bootcamp.

---

**Assignment Completion Checklist:**

- Apache Airflow setup and configuration
- PySpark integration with Airflow
- PostgreSQL database integration
- ETL pipeline implementation
- Churn-retention analysis
- Batch processing automation
- Docker containerization
- Complete documentation
- Production-ready setup

**Expected Outputs:**
- Automated batch processing pipeline
- Customer churn analysis results
- Retention cohort analysis
- Monthly trend aggregations
- PostgreSQL database with processed data
- Comprehensive logging and monitoring 
