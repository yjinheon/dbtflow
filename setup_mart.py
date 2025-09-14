import duckdb


def setup_duckdb():
    """
    init database for datamart
    """
    conn = duckdb.connect("./dm/dvdmart.duckdb")

    # create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS intermediate;")

    print("created duckdb dm")
    conn.close()


if __name__ == "__main__":
    setup_duckdb()
