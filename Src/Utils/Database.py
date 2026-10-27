from contextlib import contextmanager


@contextmanager
def postgres_cursor(postgres_hook):
    connection = postgres_hook.get_conn()
    try:
        with connection:
            with connection.cursor() as cursor:
                yield cursor
    finally:
        connection.close()
