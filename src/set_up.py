try:
    import unzip_requirements
except ImportError as e:
    print(e)

from database.data_manager.data_access import connect_as_aws_root, connect_as_aws_user, setup_db, setup_schema
from database.data_manager.init_queries import database_name
conn = connect_as_aws_root()
conn.autocommit = True
cursor = conn.cursor()

def main(event, context):
    setup_db(drop_old_info=True, db_cursor=cursor)
    cursor.execute(f'USE {database_name}')
    setup_schema(cursor=cursor)
    print('Database is set up and ready for use.')

if __name__ == '__main__':
    main(None, None)