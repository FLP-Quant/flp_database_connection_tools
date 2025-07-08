import pandas as pd
import pyodbc
from typing import List, Tuple
# import getpass

class flp_database_connector:
    def __init__(self, username: str) -> None:
        self.quant_db_name = "DataQuant01"
        self.quant_server_name = "azrsql002.database.windows.net"
        self.burapp_server_name = "BURAPP007"
        self.username = username

    # --- Functions to connect to databses ---
    def connect_to_quant_db(self) -> pyodbc.Connection:
        """
        Connect to Quant SQL Server using a full connection string with ActiveDirectoryIntegrated auth.
        """
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER=tcp:{self.quant_server_name},1433;"
            f"DATABASE={self.quant_db_name};"
            f"Uid={self.username};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
            f"Authentication=ActiveDirectoryIntegrated"
        )
        return pyodbc.connect(conn_str)
    
    def connect_to_burapp_db(self, database: str) -> pyodbc.Connection:
        """
        Connect to BURAPP SQL Server using a Windows authentication.
        """
        # password = getpass.getpass("Enter your Windows password: ")
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER=tcp:{self.burapp_server_name},1433;"
            f"DATABASE={database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
            f"Authentication=ActiveDirectoryIntegrated"
        )
        return pyodbc.connect(conn_str)

    # --- Database read functions ---
    def read_from_db(self, server: str, database: str, query: str) -> pd.DataFrame:
        if server == self.quant_db_name:
            conn = self.connect_to_quant_db()
        elif server == self.burapp_server_name:
            conn = self.connect_to_burapp_db(database)
        else:
            raise ValueError(f"Unknown server: {server}. Expected value is either BURAPP007 or DataQuant01")

        df = pd.read_sql(query, conn)
        conn.close()
        return df

    # --- Database write functions ---
    def upload_excel_to_quant_db(self,
        table_name: str,
        excel_file: str,
        mode: str = "append",
        skip_prompt: bool = False
    ) -> None:
        """
        Upload Excel data to the Quant SQL server.
        If the table already exists, will either append or overwrite data in the table.
        If the table doesn't exist, will create it.

        Parameters:
            table_name: str           -- e.g., "dbo.MyTable"
            excel_file: str           -- path to Excel file
            mode: str = "append"      -- 'append' or 'overwrite'
            skip_prompt: bool = False -- if true, skip user prompt asking for confirmation of overwriting or creating a new table
        """
        # Step 1: Load Excel
        df = pd.read_excel(excel_file)

        # Step 2: Upload data
        self.upload_data_to_quant_db(table_name, df, mode, skip_prompt)

    def upload_data_to_quant_db(self,
        table_name: str,
        df: pd.DataFrame,
        mode: str = "append",
        skip_prompt: bool = False
    ) -> None:
        """
        Upload data from a dataframe to the Quant SQL server.
        If the table already exists, will either append or overwrite data in the table.
        If the table doesn't exist, will create it.

        Parameters:
            table_name: str           -- e.g., "dbo.MyTable"
            df: dataframe             -- Pandas dataframe containing the data to be uploaded
            mode: str = "append"      -- 'append', 'overwrite', or 'create
            skip_prompt: bool = False -- if true, skip user prompt asking for confirmation of creating or overwriting a table
        """
        # Step 1: Connect to SQL
        conn = self.connect_to_quant_db()
        cursor = conn.cursor()

        # Step 2: Get schema & validate column names if table exists. If it doesn't, create a new table
        sql_columns = self.get_sql_columns(cursor, table_name)
        if len(sql_columns) < 1:
            if mode=="create" or skip_prompt or input(f"Table '{table_name}' does not exist. Create it? (y/n): ").lower() == 'y':
                    self.create_table_from_dataframe(cursor, table_name, df)
            else:
                raise ValueError("Table does not exist and creation was cancelled by user.")
        else:
            self.validate_columns(df.columns.tolist(), sql_columns)

        # Step 3: Clear table if mode is overwrite
        if mode.lower() == "overwrite" and (skip_prompt or input(f"Confirm overwriting all rows in '{table_name}'? (y/n): ").lower() == 'y'):
            cursor.execute(f"DELETE FROM {table_name}")
            print(f"All previous data in {table_name} cleared...")
            conn.commit()

        # Step 4: Insert rows into SQL table
        placeholders = ", ".join(["?"] * len(df.columns))
        colnames = ", ".join(df.columns)
        sql = f"INSERT INTO {table_name} ({colnames}) VALUES ({placeholders})"
        data = [tuple(None if pd.isna(val) else val for val in row) for _, row in df.iterrows()]

        cursor.fast_executemany = True
        cursor.executemany(sql, data)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Upload complete: {len(df)} rows {'appended to' if mode == 'append' else 'written to'} {table_name}")

    def delete_table_from_quant_db(self, table_name: str) -> None:
        conn = self.connect_to_quant_db()
        cursor = conn.cursor()
        try:
            cursor.execute(f"DROP TABLE {table_name}")
            conn.commit()
            print(f"Table {table_name} deleted successfully.")
        except Exception as e:
            print(f"Failed to delete table {table_name}: {e}")
        finally:
            cursor.close()
            conn.close()
        
    # Helper functions
    def get_sql_columns(self, cursor: pyodbc.Cursor, table_name: str) -> List[Tuple[str, str]]:
        """
        Pull column names and types from an existing SQL table using INFORMATION_SCHEMA.
        Returns a list of (column_name, data_type).
        """
        schema, table = table_name.split(".")
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query, schema, table)
        return cursor.fetchall()


    def validate_columns(self, excel_columns: List[str], sql_columns: List[Tuple[str, str]]) -> None:
        """
        Check that the Excel file's columns match the SQL table's columns
        in count and name (order-sensitive).
        """
        sql_colnames = [col[0] for col in sql_columns]

        if len(excel_columns) != len(sql_colnames):
            raise ValueError(
                f"Column count mismatch: Excel has {len(excel_columns)}, SQL table has {len(sql_colnames)}")

        mismatched = [i for i in range(len(excel_columns)) if excel_columns[i] != sql_colnames[i]]
        if mismatched:
            details = "\n".join(
                [f"  Position {i+1}: Excel = '{excel_columns[i]}' vs SQL = '{sql_colnames[i]}'" for i in mismatched])
            raise ValueError(f"Column name mismatch:\n{details}")
        
    def create_table_from_dataframe(self, cursor: pyodbc.Cursor, table_name: str, df: pd.DataFrame) -> None:
        dtype_mapping = {
            "object": "NVARCHAR(100)",
            "int64": "BIGINT",
            "float64": "FLOAT",
            "datetime64[ns]": "DATETIME",
            "bool": "BIT"
        }
        columns_sql = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            sql_type = dtype_mapping.get(dtype, "NVARCHAR(100)")
            columns_sql.append(f"[{col}] {sql_type}")

        schema, table = table_name.split(".")
        create_stmt = f"CREATE TABLE {table_name} (" + ", ".join(columns_sql) + ")"
        cursor.execute(create_stmt)
        print(f"Created new table: {table_name}")