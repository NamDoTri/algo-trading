from database.data_manager.data_access import setup_db, setup_schema

if __name__ == '__main__':
    setup_db(drop_old_info=True)
    setup_schema()