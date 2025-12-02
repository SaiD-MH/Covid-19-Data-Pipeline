# src/db_connection.py
#from sqlalchemy import create_engine, Engine, text
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import pandas as pd
from dotenv import load_dotenv
import os
from typing import Optional

load_dotenv()  # loads .env file


class DatabaseConnection:
    """
    Manages PostgreSQL database connections for ETL operations.
    Implements connection pooling and proper resource management.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[str] = None,
    ):
        ENV = ""
        if os.getenv("APP_ENV") == "DEV":
            ENV = "DEV"
        else:
            ENV = "TEST"

        self.host = host or os.getenv(f"{ENV}_DB_HOST")
        self.database = database or os.getenv(f"{ENV}_DB_NAME")
        self.user = user or os.getenv(f"{ENV}_DB_USER")
        self.password = password or os.getenv(f"{ENV}_DB_PWD")
        self.port = port or os.getenv(f"{ENV}_DB_PORT")

        self._validate_db_config()
        self._engine: Optional[Engine] = None

    def _validate_db_config(self):
        """Validate that all required connection parameters are present."""
        required = {
            "host": self.host,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "port": self.port,
        }

        missing_items = [key for key, value in required.items() if not value]

        if missing_items:
            raise ValueError(
                f"Missing required database configuration: {', '.join(missing_items)}. "
                f"Check your .env file."
            )

    @property
    def engine(self) -> Engine:
        """
        Get or create SQLAlchemy engine with connection pooling.
        Using property decorator ensures single engine instance.
        """

        if self._engine is None:
            try:
                connection_string = (
                    f"postgresql://{self.user}:{self.password}"
                    f"@{self.host}:{self.port}/{self.database}"
                )
                self._engine = create_engine(
                    connection_string,
                    pool_pre_ping=True,  # Verifing pool before using
                    pool_size=5,
                    max_overflow=10,
                    echo=False,
                )
                with self._engine.connect() as conn:
                    conn.execute(text("SELECT 1"))

            except Exception as e:
                raise ConnectionError(f"Database connection failed: {e}")

        return self._engine

    def load_dataframe_into_db(
        self, df: pd.DataFrame, db_schema: str, table_name: str
    ) -> int:
        """
        Load pandas DataFrame into database table.

        Returns:
            Number of rows loaded

        Raises:
            ValueError: If DataFrame is empty
            Exception: If load operation fails
        """
        if df.empty:
            raise ValueError("The Dataframe is empty")

        try:
            total_values_to_insert = len(df)

            df.to_sql(
                name=table_name,
                schema=db_schema,
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )
            return total_values_to_insert
        except Exception as e:
            raise type(e)(f"Can't Insert the dataframe {e}")

    def read_dataframe_from_db(self, query) -> pd.DataFrame:
        """
        Execute SQL query and return results as pandas DataFrame.

        Raises:
            Exception: If query execution fails
        """

        try:
            dataframe = pd.read_sql(query, con=self.engine)

            return dataframe
        except Exception as e:
            raise Exception(f"Faild to read the dataframe :{e}")

    def close(self):
        """Dispose of the database engine and close all connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def __enter__(self):
        """Context manager entry - returns self for 'with' statements."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection cleanup."""
        self.close()

    def __repr__(self):
        """String representation for debugging."""
        return f"DatabaseConnection(host={self.host}, database={self.database}, port={self.port})"
