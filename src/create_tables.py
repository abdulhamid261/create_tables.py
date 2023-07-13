import mysql.connector
from sql_queries import create_tbl_qur, dp_tbl_qur


def create_database():
    """
    Function creates a database.

    Using mysql connection to the database is established, kikkify database is established
    with UTF8 encoding.

    Returns
    -------
    cur
        Database cursor.
    conn
        Database connection

    """
    # connect to default database
    conn = mysql.connector.connect(host="127.0.0.1", database="kikkify", user="root", passwd="zzzxxxZX1")
    cur = conn.cursor()
    # create kikkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS kikkify")
    cur.execute("CREATE DATABASE kikkify")
    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = mysql.connector.connect(host='127.0.0.1', database="kikkify", user='root', passwd='zzzxxxZX1')
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Existing tables, created by create_tables() functions, are dropped from the database.

    Parameters
    ----------
    cur
        Database cursor.
    conn
        Database connection.
    """

    for query in dp_tbl_qur:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, db):
    """
    Tables defined in the sql_queries.py are created in the database.

    Parameters
    ----------
    cur
        Database cursor.
    conn
        Database connection.
    """

    for query in create_tbl_qur:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that creates the database, drops existing tables and creates new tables.
    """
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn, "kikkify")
    conn.close()


if __name__ == "__main__":
    main()