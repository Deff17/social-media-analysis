import psycopg2
from sqlalchemy import create_engine

connection_HP = psycopg2.connect(
    host='localhost',
    port=54320,
    dbname='my_database',
    password='pass',
    user='user',
)

# connection_SALON = psycopg2.connect(
#     host='localhost',
#     port=54340,
#     dbname='salon24',
#     password='pass',
#     user='postgres',
# )

engine_HP = create_engine('postgresql+psycopg2://user:pass@localhost:54320/my_database')