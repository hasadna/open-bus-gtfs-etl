import os
os.environ['SQLALCHEMY_URL'] = os.getenv('SQLALCHEMY_URL', 'postgresql://postgres:123456@localhost')
