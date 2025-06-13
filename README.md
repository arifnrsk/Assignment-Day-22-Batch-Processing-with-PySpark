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

### 1. Clone & Setup

```bash
# Clone the repository
git clone https://github.com/arifnrsk/Assignment-Day-22-Batch-Processing-with-PySpark.git
cd "Assignment Day 22 - Batch Processing with PySpark"

# Create required directories
mkdir -p logs plugins
```

### 2. Environment Setup

Create `.env` file:
```bash
# Copy environment template
echo "AIRFLOW_UID=50000" > .env
echo "AIRFLOW_PROJ_DIR=." >> .env
echo "_AIRFLOW_WWW_USER_USERNAME=admin" >> .env
echo "_AIRFLOW_WWW_USER_PASSWORD=admin" >> .env
```

### 3. Start Services

```bash
# Build and start all services
docker-compose up --build -d

# Check services status
docker-compose ps
```

### 4. Access Applications

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

##Project Structure

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

### Churn Analysis Output

```sql
-- High-risk customers
SELECT loyalty_number, churn_risk_score, churn_category 
FROM churn_analysis 
WHERE churn_category = 'High Risk'
ORDER BY churn_risk_score DESC;
```

### Retention Analysis Output

```sql
-- Cohort retention rates
SELECT cohort_month, period_number, retention_rate
FROM retention_analysis
ORDER BY cohort_month, period_number;
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
