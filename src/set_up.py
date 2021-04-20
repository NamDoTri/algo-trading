from database.data_manager.data_access import connect_as_aws_root, connect_as_aws_user, setup_db, setup_schema

def main(event, context):
    cursor = connect_as_aws_root().cursor()
    setup_db(drop_old_info=True, db_cursor=cursor)
    user_cursor = connect_as_aws_user().cursor()
    setup_schema(cursor=user_cursor)

if __name__ == '__main__':
    main(None, None)