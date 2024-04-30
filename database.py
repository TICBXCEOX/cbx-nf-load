from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# AWS DEV
#SQLALCHEMY_DATABASE_URI = "postgresql://edmar:_ed55_w2eijkh7@plataforma.cfjbmj8sxs2z.sa-east-1.rds.amazonaws.com:5432/cbx_dev"

# AWS PRD
#SQLALCHEMY_DATABASE_URI = "postgresql://postgres:84iuPbpQnCF5vze@plataforma.cfjbmj8sxs2z.sa-east-1.rds.amazonaws.com:5432/cbx_prd"

# LOCAL
SQLALCHEMY_DATABASE_URI = "postgresql://postgres:local123@localhost:5432/cbx_dev"

engine = create_engine(SQLALCHEMY_DATABASE_URI)
session_sync = sessionmaker(bind=engine)
