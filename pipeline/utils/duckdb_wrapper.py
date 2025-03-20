import sys
import duckdb
from pathlib import Path
import polars as pl
import polars.selectors as cs

try:
    from IPython.display import display, HTML
    IN_NOTEBOOK = True
except ImportError:
    IN_NOTEBOOK = False

try:
    from great_tables import loc, style
    HAS_GREAT_TABLES = True
except ImportError:
    HAS_GREAT_TABLES = False

from rich.console import Console
from rich.table import Table

class DuckDBWrapper:
    def __init__(self, duckdb_path=None):
        """
        Initialize a DuckDB connection.
        If duckdb_path is provided, a persistent DuckDB database will be used.
        Otherwise, it creates an in-memory database.
        """
        if duckdb_path:
            self.con = duckdb.connect(str(duckdb_path), read_only=False)
        else:
            self.con = duckdb.connect(database=':memory:', read_only=False)

        self.registered_tables = []

        # Enable httpfs for remote paths if needed
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")

    def register_data(self, paths, table_names, show_tables=False):
        """
        Registers local data files (Parquet, CSV, JSON) in DuckDB by creating views.
        Automatically detects file type (parquet, csv, json).
        
        If any table fails to register (I/O error, missing file, etc.), 
        it logs the error and continues with the others.

        Args:
            paths (list[str | Path]): List of file paths to register.
            table_names (list[str]): Corresponding table names.
            show_tables (bool): If True, display table catalog after registration.
        """
        if len(paths) != len(table_names):
            raise ValueError("The number of paths must match the number of table names.")

        for path, table_name in zip(paths, table_names):
            path_str = str(path)
            file_extension = Path(path_str).suffix.lower()

            try:
                if file_extension == ".parquet":
                    query = f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{path_str}')"
                elif file_extension == ".csv":
                    query = f"CREATE VIEW {table_name} AS SELECT * FROM read_csv_auto('{path_str}')"
                elif file_extension == ".json":
                    query = f"CREATE VIEW {table_name} AS SELECT * FROM read_json_auto('{path_str}')"
                else:
                    raise ValueError(f"Unsupported file type '{file_extension}' for file: {path_str}")

                self.con.execute(query)
                self.registered_tables.append(table_name)

            except Exception as e:
                print(f"[ERROR] Failed to register '{table_name}' from '{path_str}': {e}. Skipping...")

        # Optionally show tables
        if show_tables:
            self.show_tables()

    def bulk_register_data(self, repo_root, base_path, table_names, wildcard="*.parquet", show_tables=False):
        """
        Constructs paths for each table based on a shared base path plus the table name,
        then appends a wildcard for file matching (e.g., '*.parquet'), and registers the data.
        
        Args:
            repo_root (Path | str): The root path of your repository.
            base_path (str): The relative path from repo_root to your data directory.
            table_names (list[str]): The table names (and folder names) to register.
            wildcard (str): A wildcard pattern for the files (default '*.parquet').
            show_tables (bool): If True, display table catalog after registration.
        """
        paths = []
        for table_name in table_names:
            path = Path(repo_root) / base_path / table_name / wildcard
            paths.append(path)

        self.register_data(paths, table_names, show_tables=False)

        if show_tables:
            self.show_tables()

    def register_partitioned_data(self, base_path, table_name, wildcard="*/*/*.parquet", show_tables=False):
        """
        Registers partitioned Parquet data using Hive partitioning by creating a view.

        Args:
            base_path (str | Path): The base directory where partitioned files are located.
            table_name (str): Name of the view to be created.
            wildcard (str): Glob pattern to locate the parquet files (default '*/*/*.parquet').
            show_tables (bool): If True, display table catalog after registration.
        """
        path_str = str(Path(base_path) / wildcard)

        try:
            query = f"""
            CREATE OR REPLACE VIEW {table_name} AS 
            SELECT * FROM read_parquet('{path_str}', hive_partitioning=true, union_by_name=true)
            """
            self.con.execute(query)
            self.registered_tables.append(table_name)
            print(f"Partitioned view '{table_name}' created for files at '{path_str}'.")
        except Exception as e:
            print(f"[ERROR] Failed to register partitioned data for '{table_name}': {e}. Skipping...")

        if show_tables:
            self.show_tables()

    def bulk_register_partitioned_data(self, repo_root, base_path, table_names, wildcard="*/*/*.parquet", show_tables=False):
        """
        Registers multiple partitioned Parquet datasets, using Hive partitioning, by looping 
        over a list of table names. For each name, it constructs a path from (repo_root / base_path / table_name),
        then appends the wildcard, and calls register_partitioned_data.

        Args:
            repo_root (Path | str): The root path of your repository.
            base_path (str): The relative path from repo_root to your partitioned datasets.
            table_names (list[str]): List of partitioned datasets (folder names) to register.
            wildcard (str): The glob pattern to locate the .parquet files. Default '*/*/*.parquet'.
            show_tables (bool): If True, displays the table catalog after registration.
        """
        for table_name in table_names:
            partition_path = Path(repo_root) / base_path / table_name
            self.register_partitioned_data(
                base_path=partition_path,
                table_name=table_name,
                wildcard=wildcard,
                show_tables=False  # We'll show_tables once at the end
            )

        if show_tables:
            self.show_tables()

    def run_query(self, sql_query, show_results=False):
        """
        Runs a SQL query on the registered tables in DuckDB and returns a Polars DataFrame.
        Optionally displays the result. 
        """
        arrow_table = self.con.execute(sql_query).arrow()
        df = pl.DataFrame(arrow_table)

        if show_results:
            if IN_NOTEBOOK and HAS_GREAT_TABLES:
                styled = (
                    df.style
                    .tab_header(
                        title="DuckDB Query Results",
                        subtitle=f"{sql_query[:50]}..."
                    )
                    .fmt_number(cs.numeric(), decimals=3)
                )
                styled_html = styled._repr_html_()
                scrollable_html = f"""
                <div style="max-width:100%; overflow-x:auto; white-space:nowrap;">
                    {styled_html}
                </div>
                """
                display(HTML(scrollable_html))
            else:
                self.print_query_results(df, title=f"Query: {sql_query[:50]}...")

        return df

    def print_query_results(self, df, title="Query Results"):
        """
        Prints a Polars DataFrame as a Rich table.
        Uses a pager for large outputs in the terminal.
        """
        console = Console()
        with console.pager(styles=True):
            table = Table(title=title, title_style="bold green", show_lines=True)
            for column in df.columns:
                table.add_column(str(column), style="bold cyan", overflow="fold")

            for row in df.iter_rows(named=True):
                values = [str(row[col]) for col in df.columns]
                table.add_row(*values, style="white on black")

            console.print(table)

    def _construct_path(self, path, base_path, file_name, extension):
        """
        Constructs the full file path based on input parameters.
        """
        if path:
            return Path(path)
        elif base_path and file_name:
            return Path(base_path) / f"{file_name}.{extension}"
        else:
            return Path(f"output.{extension}")

    def export(self, result, file_type, path=None, base_path=None, file_name=None, with_header=True):
        """
        Exports a Polars DataFrame (or anything convertible to Polars) 
        to the specified file type (parquet, csv, json).
        """
        file_type = file_type.lower()
        if file_type not in ["parquet", "csv", "json"]:
            raise ValueError("file_type must be one of 'parquet', 'csv', or 'json'.")

        full_path = self._construct_path(path, base_path, file_name, file_type)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(result, pl.DataFrame):
            df = result
        elif hasattr(result, "to_arrow"):
            df = pl.DataFrame(result.to_arrow())
        else:
            raise ValueError("Unsupported result type. Must be a Polars DataFrame or have a 'to_arrow()' method.")

        if file_type == "parquet":
            df.write_parquet(str(full_path))
        elif file_type == "csv":
            df.write_csv(str(full_path), separator=",", include_header=with_header)
        elif file_type == "json":
            df.write_ndjson(str(full_path))

        print(f"File written to: {full_path}")

    def show_tables(self):
        """
        Displays the table names and types currently registered in the catalog,
        in a Rich-styled table.
        """
        query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema='main'
        """
        df = self.run_query(query)
        console = Console()
        table = Table(title="Registered Tables", title_style="bold green", show_lines=True)
        table.add_column("Table Name", justify="left", style="bold yellow")
        table.add_column("Table Type", justify="left", style="bold cyan")

        for row in df.to_dicts():
            table.add_row(row["table_name"], row["table_type"], style="white on black")

        console.print(table)

    def show_schema(self, table_name):
        """
        Displays the schema of the specified DuckDB table or view, using Rich formatting.
        """
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """
        df = self.run_query(query)
        console = Console()
        schema_table = Table(title=f"Schema for '{table_name}'", title_style="bold green")
        schema_table.add_column("Column Name", justify="left", style="bold yellow", no_wrap=True)
        schema_table.add_column("Data Type", justify="left", style="bold cyan")

        for row in df.to_dicts():
            schema_table.add_row(row["column_name"], str(row["data_type"]), style="white on black")

        console.print(schema_table)

    def show_parquet_schema(self, file_path):
        """
        Reads a Parquet file directly using Polars, and prints its schema 
        and row count using Rich formatting.
        """
        df = pl.read_parquet(file_path)

        console = Console()
        schema_table = Table(title="Parquet Schema", title_style="bold green")
        schema_table.add_column("Column Name", justify="left", style="bold yellow", no_wrap=True)
        schema_table.add_column("Data Type", justify="left", style="bold cyan")

        for col_name, col_dtype in df.schema.items():
            schema_table.add_row(col_name, str(col_dtype), style="white on black")

        console.print(schema_table)
        console.print(f"[bold magenta]\nNumber of rows:[/] [bold white]{df.height}[/]")
