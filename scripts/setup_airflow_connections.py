from airflow import settings
from airflow.models import Connection
from sqlalchemy.orm import sessionmaker
import logging

logger = logging.getLogger(__name__)

def create_postgres_connection():
    """Create PostgreSQL connection for business database"""
    try:
        session = settings.Session()
        
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'postgres_default'
        ).first()
        
        if existing_conn:
            logger.info("PostgreSQL connection already exists, updating...")
            session.delete(existing_conn)
        
        postgres_conn = Connection(
            conn_id='postgres_default',
            conn_type='postgres',
            host='postgres_business',
            schema='batch_processing',
            login='postgres',
            password='postgres',
            port=5432,
            extra='{"sslmode": "prefer"}'
        )
        
        session.add(postgres_conn)
        session.commit()
        session.close()
        
        logger.info("PostgreSQL connection created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL connection: {e}")
        return False

def create_spark_connection():
    """Create Spark connection"""
    try:
        session = settings.Session()
        
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'spark_default'
        ).first()
        
        if existing_conn:
            logger.info("Spark connection already exists, updating...")
            session.delete(existing_conn)
        
        spark_conn = Connection(
            conn_id='spark_default',
            conn_type='spark',
            host='spark-master',
            port=7077,
            extra='{"queue": "default", "deploy-mode": "client"}'
        )
        
        session.add(spark_conn)
        session.commit()
        session.close()
        
        logger.info("Spark connection created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create Spark connection: {e}")
        return False

def setup_all_connections():
    """Setup all required connections"""
    logger.info("Setting up Airflow connections...")
    
    postgres_success = create_postgres_connection()
    spark_success = create_spark_connection()
    
    if postgres_success and spark_success:
        logger.info("All connections setup successfully")
        return True
    else:
        logger.error("Some connections failed to setup")
        return False

if __name__ == "__main__":
    setup_all_connections() 