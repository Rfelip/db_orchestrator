from abc import ABC, abstractmethod

class ProfilerStrategy(ABC):
    """
    Abstract base class for database profiling strategies.
    Ensures that different database backends (Oracle, Postgres) implement
    a consistent interface for capturing execution metrics and plans.
    """

    @abstractmethod
    def prepare_query(self, sql: str) -> str:
        """
        Modifies the SQL query if necessary (e.g., injecting hints or wrapping in EXPLAIN).
        
        Args:
            sql (str): The original SQL query.
            
        Returns:
            str: The modified SQL query ready for execution.
        """
        pass

    @abstractmethod
    def post_execution_capture(self, cursor, execution_result) -> None:
        """
        Retrieves metrics and execution plans after the task finishes.
        
        Args:
            cursor: The database cursor used for execution.
            execution_result: The result object from the execution (if applicable).
        """
        pass

    @abstractmethod
    def get_metrics(self) -> dict:
        """
        Returns standardized dictionary of execution metrics.
        
        Returns:
            dict: {
                'duration_ms': float,
                'db_cpu_ms': float,
                'db_io_ms': float,
                'io_requests': int,
                'parallel_degree': int,
                ...
            }
        """
        pass

    @abstractmethod
    def save_plan(self, path: str) -> None:
        """
        Writes the captured execution plan to disk.
        
        Args:
            path (str): The file path to save the plan.
        """
        pass
