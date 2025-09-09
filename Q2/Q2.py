########################### DO NOT MODIFY THIS SECTION ##########################
#################################################################################
import sqlite3
from sqlite3 import Error, Connection
import csv
from typing import Any
#################################################################################

## Change to False to disable Sample
SHOW = False

############### SAMPLE CLASS AND SQL QUERY ###########################
######################################################################
class Sample():
    def sample(self):
        try:
            connection = sqlite3.connect("sample")
            connection.text_factory = str
        except Error as e:
            print("Error occurred: " + str(e))
        print('\033[32m' + "Sample: " + '\033[m')
        
        # Sample Drop table
        connection.execute("DROP TABLE IF EXISTS sample;")
        # Sample Create
        connection.execute("CREATE TABLE sample(id integer, name text);")
        # Sample Insert
        connection.execute("INSERT INTO sample VALUES (?,?)",("1","test_name"))
        connection.commit()
        # Sample Select
        cursor = connection.execute("SELECT * FROM sample;")
        print(cursor.fetchall())

######################################################################

############### DO NOT MODIFY THIS SECTION ###########################
######################################################################
def create_connection(path: str) -> Connection:
    connection = None
    try:
        connection = sqlite3.connect(path)
        connection.text_factory = str
    except Error as e:
        print("Error occurred: " + str(e))

    return connection


def execute_query(connection: Connection, query: str) -> str:
    cursor = connection.cursor()
    try:
        if query == "":
            return "Query Blank"
        else:
            cursor.execute(query)
            connection.commit()
            return "Query executed successfully"
    except Error as e:
        return "Error occurred: " + str(e)


def execute_query_and_get_result(connection: Connection, query: str) -> Any:
    cursor = connection.execute(query)
    return cursor.fetchall()
######################################################################
######################################################################


def GTusername() -> str:
    gt_username = ""
    return gt_username


def part_1_a_i() -> str:
    ############### EDIT SQL STATEMENT ###################################
    query = """
    CREATE TABLE IF NOT EXISTS incidents (
        report_id TEXT PRIMARY KEY,
        category  TEXT NOT NULL,
        date      TEXT
    );
    """
    ######################################################################
    return query


def part_1_a_ii() -> str:
    ############### EDIT SQL STATEMENT ###################################
    query = """
    CREATE TABLE IF NOT EXISTS details (
        report_id      TEXT PRIMARY KEY,
        subject        TEXT,
        transport_mode TEXT,
        detection      TEXT,
        FOREIGN KEY (report_id) REFERENCES incidents(report_id)
    );
    """
    ######################################################################
    return query


def part_1_a_iii() -> str:
    ############### EDIT SQL STATEMENT ###################################
    query = """
    CREATE TABLE IF NOT EXISTS outcomes (
        report_id         TEXT PRIMARY KEY,
        outcome           TEXT,
        num_ppl_fined     INTEGER,
        fine              REAL,
        num_ppl_arrested  INTEGER,
        prison_time       REAL,
        prison_time_unit  TEXT,
        FOREIGN KEY (report_id) REFERENCES incidents(report_id)
    );
    """
    ######################################################################
    return query


def part_1_b_i(connection: Connection, path: str) -> None:
    with open(path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        rows = [(row["report_id"], row["category"], row["date"]) for row in r]
    connection.executemany(
        "INSERT INTO incidents(report_id, category, date) VALUES (?,?,?)",
        rows
    )
    connection.commit()


def part_1_b_ii(connection: Connection, path: str) -> None:
    with open(path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        rows = [(row["report_id"], row["subject"], row["transport_mode"], row["detection"]) for row in r]
    connection.executemany(
        "INSERT INTO details(report_id, subject, transport_mode, detection) VALUES (?,?,?,?)",
        rows
    )
    connection.commit()


def part_1_b_iii(connection: Connection, path: str) -> None:
        def to_int(x):
            x = x.strip() if isinstance(x, str) else x
            return int(x) if x not in (None, "",) else None
        def to_float(x):
            x = x.strip() if isinstance(x, str) else x
            return float(x) if x not in (None, "",) else None

        with open(path, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            rows = []
            for row in r:
                rows.append((
                    row["report_id"],
                    row["outcome"],
                    to_int(row["num_ppl_fined"]),
                    to_float(row["fine"]),
                    to_int(row["num_ppl_arrested"]),
                    to_float(row["prison_time"]),
                    row["prison_time_unit"]
                ))
        connection.executemany(
            """INSERT INTO outcomes
            (report_id, outcome, num_ppl_fined, fine, num_ppl_arrested, prison_time, prison_time_unit)
            VALUES (?,?,?,?,?,?,?)""",
            rows
        )
        connection.commit()

def part_2_a() -> str:
    query = "CREATE INDEX IF NOT EXISTS idx_incidents_category_date ON incidents(category, date);"
    return query


def part_2_b() -> str:
    query = "CREATE INDEX IF NOT EXISTS idx_details_detection_mode ON details(detection, transport_mode);"
    return query


def part_2_c() -> str:
    query = "CREATE INDEX IF NOT EXISTS idx_outcomes_arrest_fine ON outcomes(num_ppl_arrested, num_ppl_fined);"
    return query


def part_3() -> str:
    query = """
    SELECT ROUND(
        100.0 * SUM(CASE WHEN date BETWEEN '2018-01-01' AND '2020-12-31' THEN 1 ELSE 0 END)
              / COUNT(*)
    , 2) AS pct
    FROM incidents;
    """
    return query


def part_4() -> str:
    query = """
    SELECT transport_mode,
           COUNT(*) AS count
    FROM details
    WHERE detection = 'Intelligence'
      AND transport_mode IS NOT NULL
      AND TRIM(transport_mode) <> ''
    GROUP BY transport_mode
    ORDER BY count DESC, transport_mode ASC
    LIMIT 3;
    """
    return query


def part_5() -> str:
    query = """
    SELECT d.detection,
           COUNT(*) AS count,
           ROUND(AVG(o.num_ppl_arrested * 1.0), 2) AS avg_ppl_arrested
    FROM details d
    INNER JOIN outcomes o USING (report_id)
    WHERE o.num_ppl_arrested > 0
    GROUP BY d.detection
    HAVING COUNT(*) >= 100
    ORDER BY avg_ppl_arrested DESC, d.detection ASC
    LIMIT 3;
    """
    return query


def part_6() -> str:
    query = """
        SELECT i.category,
            COUNT(*) AS count,
            ROUND(AVG(
                CASE
                WHEN LOWER(o.prison_time_unit) LIKE 'year%'  THEN COALESCE(o.prison_time,0) * 365.0
                WHEN LOWER(o.prison_time_unit) LIKE 'month%' THEN COALESCE(o.prison_time,0) * 30.0
                WHEN LOWER(o.prison_time_unit) LIKE 'week%'  THEN COALESCE(o.prison_time,0) * 7.0
                ELSE COALESCE(o.prison_time,0)
                END
            ), 2) AS avg_prison_time_days
        FROM incidents i
        INNER JOIN outcomes o USING (report_id)
        GROUP BY i.category
        HAVING COUNT(*) > 50
        ORDER BY avg_prison_time_days DESC, i.category ASC;
        """
    return query


def part_7_a() -> str:
    query = """
    CREATE VIEW IF NOT EXISTS fines AS
    SELECT i.report_id,
           i.date,
           o.num_ppl_fined,
           o.fine
    FROM incidents i
    INNER JOIN outcomes o USING (report_id)
    WHERE o.num_ppl_fined >= 1;
    """

    return query


def part_7_b() -> str:
    query = """
    SELECT strftime('%Y', date) AS year,
           SUM(num_ppl_fined)   AS total_ppl_fined,
           ROUND(SUM(fine), 2)  AS total_fine_amount
    FROM fines
    GROUP BY year
    ORDER BY total_fine_amount DESC, year ASC
    LIMIT 3;
    """
    return query


def part_8_a() -> str:
    query = """
    CREATE VIRTUAL TABLE IF NOT EXISTS incident_overviews
    USING fts5(report_id, subject);
    """
    return query


def part_8_b() -> str:
    query = """
    INSERT INTO incident_overviews (report_id, subject)
    SELECT report_id, subject
    FROM details;
    """
    return query

    
def part_8_c():
    return """
    SELECT COUNT(*)
    FROM incident_overviews
    WHERE incident_overviews MATCH 'NEAR("dead" "pangolin", 2)';
    """



if __name__ == "__main__":
    
    ########################### DO NOT MODIFY THIS SECTION ##########################
    #################################################################################
    if SHOW:
        sample = Sample()
        sample.sample()

    print('\033[32m' + "Q2 Output: " + '\033[m')
    try:
        conn = create_connection("Q2")
    except Exception as e:
        print("Database Creation Error:", e)

    try:
        conn.execute("DROP TABLE IF EXISTS incidents;")
        conn.execute("DROP TABLE IF EXISTS details;")
        conn.execute("DROP TABLE IF EXISTS outcomes;")
        conn.execute("DROP VIEW IF EXISTS fines;")
        conn.execute("DROP TABLE IF EXISTS incident_overviews;")
    except Exception as e:
        print("Error in Table Drops:", e)

    try:
        print('\033[32m' + "part 1.a.i: " + '\033[m' + execute_query(conn, part_1_a_i()))
        print('\033[32m' + "part 1.a.ii: " + '\033[m' + execute_query(conn, part_1_a_ii()))
        print('\033[32m' + "part 1.a.iii: " + '\033[m' + execute_query(conn, part_1_a_iii()))
    except Exception as e:
         print("Error in part 1.a:", e)

    try:
        part_1_b_i(conn,"data/incidents.csv")
        print('\033[32m' + "Row count for Incidents Table: " + '\033[m' + str(execute_query_and_get_result(conn, "select count(*) from incidents")[0][0]))
        part_1_b_ii(conn, "data/details.csv")
        print('\033[32m' + "Row count for Details Table: " + '\033[m' + str(execute_query_and_get_result(conn,"select count(*) from details")[0][0]))
        part_1_b_iii(conn, "data/outcomes.csv")
        print('\033[32m' + "Row count for Outcomes Table: " + '\033[m' + str(execute_query_and_get_result(conn,"select count(*) from outcomes")[0][0]))
    except Exception as e:
        print("Error in part 1.b:", e)

    try:
        print('\033[32m' + "part 2.a: " + '\033[m' + execute_query(conn, part_2_a()))
        print('\033[32m' + "part 2.b: " + '\033[m' + execute_query(conn, part_2_b()))
        print('\033[32m' + "part 2.c: " + '\033[m' + execute_query(conn, part_2_c()))
    except Exception as e:
        print("Error in part 2:", e)

    try:
        print('\033[32m' + "part 3: " + '\033[m' + str(execute_query_and_get_result(conn, part_3())[0][0]))
    except Exception as e:
        print("Error in part 3:", e)

    try:
        print('\033[32m' + "part 4: " + '\033[m')
        for line in execute_query_and_get_result(conn, part_4()):
            print(line[0],line[1])
    except Exception as e:
        print("Error in part 4:", e)

    try:
        print('\033[32m' + "part 5: " + '\033[m')
        for line in execute_query_and_get_result(conn, part_5()):
            print(line[0],line[1],line[2])
    except Exception as e:
        print("Error in part 5:", e)

    try:
        print('\033[32m' + "part 6: " + '\033[m')
        for line in execute_query_and_get_result(conn, part_6()):
            print(line[0],line[1],line[2])
    except Exception as e:
        print("Error in part 6:", e)
    
    try:
        execute_query(conn, part_7_a())
        print('\033[32m' + "part 7.a: " + '\033[m' + str(execute_query_and_get_result(conn,"select count(*) from fines")[0][0]))
        print('\033[32m' + "part 7.b: " + '\033[m')
        for line in execute_query_and_get_result(conn, part_7_b()):
            print(line[0],line[1], line[2])
    except Exception as e:
        print("Error in part 7:", e)

    try:   
        print('\033[32m' + "part 8.a: " + '\033[m'+ execute_query(conn, part_8_a()))
        execute_query(conn, part_8_b())
        print('\033[32m' + "part 8.b: " + '\033[m' + str(execute_query_and_get_result(conn, "select count(*) from incident_overviews")[0][0]))
        print('\033[32m' + "part 8.c: " + '\033[m' + str(execute_query_and_get_result(conn, part_8_c())[0][0]))
    except Exception as e:
        print("Error in part 8:", e)

    conn.close()
    #################################################################################
    #################################################################################
  