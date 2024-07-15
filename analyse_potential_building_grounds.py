import os
import json
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Load database connection parameters from config file
loc_config = os.path.join('..', '..', 'data', 'config.json')
with open(loc_config, 'r') as f:
    db_params = json.load(f)

# Database connection parameters
db_name = db_params['dbname']
db_user = db_params['user']
db_password = db_params['password']
db_host = db_params['host']
db_port = db_params['port']

def get_project_id(project_name):
    """Fetch the project id for a given project name from the database."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()

        # SQL query to fetch the project id
        query = """
        SELECT id 
        FROM editeren.riolering_screening_projectzone_24001
        WHERE naam = %s;
        """
        cur.execute(query, (project_name,))
        result = cur.fetchone()

        if result:
            project_id = result[0]
            print(f"Project ID for '{project_name}' is {project_id}.")
            return project_id
        else:
            print(f"No project found with the name '{project_name}'.")
            return None

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

def get_all_project_ids():
    """Fetch all project ids from the database."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()

        # SQL query to fetch all project ids
        query = """
        SELECT id 
        FROM editeren.riolering_screening_projectzone_24001;
        """
        cur.execute(query)
        result = cur.fetchall()
        project_ids = [row[0] for row in result]
        return project_ids

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

def format_user_name(first_name, last_name):
    """Format the user name according to the specific pattern."""
    return f"STADAARSCHOT_{first_name.lower()}.{last_name.lower()}"

def pass_user_id(user_name):
    """Pass user id from user name."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()

        # SQL query to fetch user id
        query = """
        SELECT id
        FROM identity_server.asp_net_users
        WHERE user_name = %s;
        """    
        cur.execute(query, (user_name,))
        
        # Fetch the result
        result = cur.fetchone()
        
        if result:
            user_id = result[0]
            print("User id successfully returned")
            return user_id
        else:
            print("User not found")
            return None

    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def create_combined_subset_tables(project_id):
    """Create combined subset tables for all project zones and restrict geometries to within the project zones."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()

        # Create combined tables if they do not exist
        combined_tables = {
            "projects.gbg_combined": """
                CREATE TABLE IF NOT EXISTS projects.gbg_combined (
                    id SERIAL PRIMARY KEY,
                    project_id INTEGER,
                    geometry geometry,
                    CONSTRAINT fk_project FOREIGN KEY(project_id) 
                        REFERENCES editeren.riolering_screening_projectzone_24001(id)
                );
            """,
            "projects.gewestplan_combined": """
                CREATE TABLE IF NOT EXISTS projects.gewestplan_combined (
                    id SERIAL PRIMARY KEY,
                    project_id INTEGER,
                    hoofdcode CHAR(4),
                    voorschriften TEXT,
                    geometry geometry,
                    CONSTRAINT fk_project FOREIGN KEY(project_id) 
                        REFERENCES editeren.riolering_screening_projectzone_24001(id)
                );
            """,
            "projects.percelen_combined": """
                CREATE TABLE IF NOT EXISTS projects.percelen_combined (
                    id SERIAL PRIMARY KEY,
                    project_id INTEGER,
                    capakey TEXT,
                    geometry geometry,
                    CONSTRAINT fk_project FOREIGN KEY(project_id) 
                        REFERENCES editeren.riolering_screening_projectzone_24001(id)
                );
            """
        }

        for table_name, create_query in combined_tables.items():
            cur.execute(create_query)

        # Queries to insert data into combined tables
        insert_queries = [
            sql.SQL("""
                INSERT INTO projects.gbg_combined (project_id, geometry)
                SELECT %s, ST_Intersection(ST_SetSRID(b.shape, 31370), ST_SetSRID(r.geometry, 31370)) AS geometry
                FROM wfs.grb_gbg AS b
                JOIN editeren.riolering_screening_projectzone_24001 AS r
                ON ST_Intersects(ST_SetSRID(b.shape, 31370), ST_SetSRID(r.geometry, 31370))
                WHERE r.id = %s AND ST_IsValid(ST_Intersection(ST_SetSRID(b.shape, 31370), ST_SetSRID(r.geometry, 31370)));
            """),
            sql.SQL("""
                INSERT INTO projects.gewestplan_combined (project_id, hoofdcode, voorschriften, geometry)
                SELECT %s, g.hoofdcode, g.voorschriften, ST_Intersection(ST_SetSRID(g.geometry, 31370), ST_SetSRID(r.geometry, 31370)) AS geometry
                FROM bestemmingen.vw_gewestplan AS g
                JOIN editeren.riolering_screening_projectzone_24001 AS r
                ON ST_Intersects(ST_SetSRID(g.geometry, 31370), ST_SetSRID(r.geometry, 31370))
                WHERE r.id = %s AND ST_IsValid(ST_Intersection(ST_SetSRID(g.geometry, 31370), ST_SetSRID(r.geometry, 31370)));
                """),
            sql.SQL("""
                INSERT INTO projects.percelen_combined (project_id, capakey, geometry)
                SELECT %s, p.capakey, ST_Intersection(ST_SetSRID(p.geometry, 31370), ST_SetSRID(r.geometry, 31370)) AS geometry
                FROM public.percelen AS p
                JOIN editeren.riolering_screening_projectzone_24001 AS r
                ON ST_Intersects(ST_SetSRID(p.geometry, 31370), ST_SetSRID(r.geometry, 31370))
                WHERE r.id = %s AND ST_IsValid(ST_Intersection(ST_SetSRID(p.geometry, 31370), ST_SetSRID(r.geometry, 31370)));
            """)
        ]

        # Execute insert queries
        for query in insert_queries:
            cur.execute(query, (project_id, project_id))

        conn.commit()
        print(f"Data for project ID '{project_id}' inserted successfully into combined tables.")

    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def store_potential_building_grounds(user_id):
    """Store potential building grounds for all project zones."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()

        # Current timestamp
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # SQL query to insert potential building grounds
        query = """
        WITH valid_geometries AS (
            SELECT 
                p.capakey,
                g.voorschriften,
                ST_Multi(ST_MakeValid(ST_SetSRID(ST_Intersection(p.geometry, g.geometry), 31370))) AS geometry
            FROM projects.percelen_combined AS p
            JOIN projects.gewestplan_combined AS g 
                ON ST_Intersects(ST_SetSRID(p.geometry, 31370), ST_SetSRID(g.geometry, 31370))
            WHERE 
                g.hoofdcode IN ('0100', '0101', '0102', '0103', '0104', '0105', '0110')
                AND ST_IsValid(p.geometry) 
                AND NOT ST_IsEmpty(p.geometry)
        )
        INSERT INTO editeren.riolering_screening_potentiele_bouwgronden_24001 (capakey, voorschriften, geometry, created_by, created_at)
        SELECT 
            w.capakey, 
            w.voorschriften, 
            ST_SetSRID(w.geometry, 0) AS geometry,
            %s,
            %s
        FROM 
            valid_geometries AS w
        LEFT JOIN  
            projects.gbg_combined AS b 
        ON ST_Intersects(ST_SetSRID(w.geometry, 31370), ST_SetSRID(b.geometry, 31370)) 
        WHERE b.id IS NULL  
            AND ST_Area(ST_SetSRID(w.geometry, 31370)) > 300
        GROUP BY w.capakey, w.geometry, w.voorschriften;
        """

        # Execute the query
        cur.execute(query, (user_id, created_at))
        conn.commit()
        print("Potential building grounds stored successfully.")

    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def store_gewestplan_woonzones(user_id):
    """Store selected gewestplan woonzones for a given project zone."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()

        # Current timestamp
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # SQL query to insert gewestplan woonzones
        query = """
        INSERT INTO editeren.riolering_screening_woonzones_gwp_24001 (naam, hoofdcode, voorschriften, geometry, created_by, created_at)
        SELECT 
            pz.naam,
            g.hoofdcode,
            g.voorschriften,
            ST_SetSRID(ST_Multi(g.geometry), 0) as geometry,
            %s,
            %s
        FROM 
            projects.gewestplan_combined AS g
        JOIN editeren.riolering_screening_projectzone_24001 AS pz
        ON g.project_id = pz.id
        WHERE 
            g.hoofdcode IN ('0100', '0101', '0102', '0103', '0104', '0105', '0110');
        """
        cur.execute(query, (user_id, created_at))
        conn.commit()
        print("Gewestplan woonzones stored successfully.")

    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

# Main execution
if __name__ == "__main__":
    mode = input("Enter mode (input/analyze): ").strip().lower()
    first_name = input("Enter the first name: ").strip().lower()
    last_name = input("Enter the last name: ").strip().lower()
    user_name = format_user_name(first_name, last_name)
    user_id = pass_user_id(user_name)

    if user_id is None:
        print("User ID could not be retrieved. Exiting.")
    else:
        if mode == "input":
            print(f"User ID for '{user_name}' is {user_id}.")
            # Add your input mode code here, if any
        elif mode == "analyze":
            store_potential_building_grounds(user_id)
            store_gewestplan_woonzones(user_id)
        else:
            print("Invalid mode entered. Please enter either 'input' or 'analyze'.")