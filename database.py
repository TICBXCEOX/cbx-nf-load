import psycopg2
from sqlalchemy import create_engine

# AWS DEV
#SQLALCHEMY_DATABASE_URI = "postgresql://edmar:_ed55_w2eijkh7@plataforma.cfjbmj8sxs2z.sa-east-1.rds.amazonaws.com:5432/cbx_dev"

# AWS PRD
#SQLALCHEMY_DATABASE_URI = "postgresql://postgres:84iuPbpQnCF5vze@plataforma.cfjbmj8sxs2z.sa-east-1.rds.amazonaws.com:5432/cbx_prd"

# LOCAL
#SQLALCHEMY_DATABASE_URI = "postgresql://postgres:local123@localhost:5432/cbx_dev"

#engine = create_engine(SQLALCHEMY_DATABASE_URI)
#session_sync = sessionmaker(bind=engine)

def connect_to_db(prod):
    if prod:
        conn = psycopg2.connect(
            host="plataforma.cfjbmj8sxs2z.sa-east-1.rds.amazonaws.com",
            database="cbx_prd",
            user="postgres",
            password="84iuPbpQnCF5vze"
        )
        return conn
    else:
        conn = psycopg2.connect(
            host="localhost",
            database="cbx_dev",
            user="postgres",
            password="local123"
        )
        return conn

def get_engine(prod):
    conn = connect_to_db(prod=prod)
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)    
    return engine
