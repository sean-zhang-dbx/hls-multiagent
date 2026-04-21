import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from threading import Lock
from typing import Any, Dict, Generator, List, Optional, Sequence, TypedDict

import psycopg
from databricks.sdk import WorkspaceClient
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
)
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    create_text_output_item,
    create_function_call_item,       
    create_function_call_output_item,
)
logger = logging.getLogger(__name__)

class CredentialConnection(psycopg.Connection):
    """Custom connection class that generates fresh OAuth tokens with caching."""

    workspace_client = None
    instance_name = None

    # Cache attributes
    _cached_credential = None
    _cache_timestamp = None
    _cache_duration = 3000  # 50 minutes in seconds (50 * 60)
    _cache_lock = Lock()


    @classmethod
    def connect(cls, conninfo="", **kwargs):
        """Override connect to inject OAuth token with 50-minute caching"""
        if cls.workspace_client is None or cls.instance_name is None:
            raise ValueError(
                "workspace_client and instance_name must be set on CredentialConnection class"
            )

        # Get cached or fresh credential and append the new password to kwargs
        credential_token = cls._get_cached_credential()
        kwargs["password"] = credential_token

        # Call the superclass's connect method with updated kwargs
        return super().connect(conninfo, **kwargs)

    @classmethod
    def _get_cached_credential(cls):
        """Get credential from cache or generate a new one if cache is expired"""
        with cls._cache_lock:
            current_time = time.time()

            # Check if we have a valid cached credential
            if (
                cls._cached_credential is not None
                and cls._cache_timestamp is not None
                and current_time - cls._cache_timestamp < cls._cache_duration
            ):
                return cls._cached_credential

            # Generate new credential
            credential = cls.workspace_client.database.generate_database_credential(
                request_id=str(uuid.uuid4()), instance_names=[cls.instance_name]
            )

            # Cache the new credential
            cls._cached_credential = credential.token
            cls._cache_timestamp = current_time

            return cls._cached_credential

class DatabricksStateManager:
    """
    A comprehensive state management library for Databricks Lakebase PostgreSQL.
    
    Features:
    - Direct connection management with automatic credential rotation and caching
    - Thread-safe operations with proper connection handling
    - Configurable cache duration
    - Built-in PostgresSaver integration for LangGraph checkpointing
    - Support for client ID/secret authentication
    - Standard PostgresSaver checkpoint table management
    
    Note: PostgresSaver uses standard table names (checkpoints, checkpoint_blobs, 
    checkpoint_writes) and doesn't support custom table naming in the current version.
    """
    
    def __init__(
        self,
        lakebase_config: Dict[str, Any],
        workspace_client: Optional[WorkspaceClient] = None,
        token_cache_minutes: int = 50,
        connection_timeout: float = 30.0
    ):
        """
        Initialize the state manager.
        
        Args:
            lakebase_config: Dictionary containing:
                - instance_name: Lakebase instance name
                - conn_host: Database host
                - conn_db_name: Database name (default: 'databricks_postgres')
                - conn_ssl_mode: SSL mode (default: 'require')
                - client_id: Service Principal client ID (optional)
                - client_secret: Service Principal client secret (optional)
                - workspace_host: Databricks workspace URL (required if using client_id/secret)
            workspace_client: Databricks workspace client (creates new if None)
            token_cache_minutes: How long to cache OAuth tokens
            connection_timeout: Connection timeout in seconds
        """
        self.lakebase_config = lakebase_config
        self.connection_timeout = connection_timeout
        # Connection pool configuration
        self.pool_min_size = int(os.getenv("DB_POOL_MIN_SIZE", "1"))
        self.pool_max_size = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
        self.pool_timeout = float(os.getenv("DB_POOL_TIMEOUT", "30.0"))

        # Initialize workspace client based on provided config
        if workspace_client:
            self.workspace_client = workspace_client
        elif lakebase_config.get("client_id") and lakebase_config.get("client_secret"):
            # Use client ID and secret for authentication
            workspace_host = lakebase_config.get("workspace_host")
            if not workspace_host:
                raise ValueError("workspace_host is required when using client_id/client_secret authentication")
            
            self.workspace_client = WorkspaceClient(
                host=workspace_host,
                client_id=lakebase_config["client_id"],
                client_secret=lakebase_config["client_secret"]
            )
            logger.info("WorkspaceClient initialized with client_id/client_secret authentication")
        else:
            # Use default authentication (environment variables, etc.)
            self.workspace_client = WorkspaceClient()
            logger.info("WorkspaceClient initialized with default authentication")
        
        # Token caching
        self._cache_duration = token_cache_minutes * 60
        self._cached_credential = None
        self._cache_timestamp = None
        self._cache_lock = Lock()
        
        # Standard PostgresSaver table names (not configurable in current version)
        self.standard_tables = {
            "checkpoints": "checkpoints",
            "checkpoint_blobs": "checkpoint_blobs", 
            "checkpoint_writes": "checkpoint_writes"
        }
        
        # Connection parameters
        self.username = self._get_username()
        self.host = self.lakebase_config["conn_host"]
        self.database = self.lakebase_config.get("conn_db_name", "databricks_postgres")
        self.ssl_mode = self.lakebase_config.get("conn_ssl_mode", "require")
        self.conn_info = f"dbname={self.database} user={self.username} host={self.host} sslmode={self.ssl_mode}"
        
        self._is_initialized = False
        
        # Initialize the connection pool with rotating credentials
        self._connection_pool = self._create_rotating_pool()
        print("Connection pool initialised")
        
        logger.info(
            f"DatabricksStateManager initialized with direct connections "
            f"using standard PostgresSaver tables: {', '.join(self.standard_tables.values())}"
        )
    
    def _get_username(self) -> str:
        """Get the username for database connection"""
        # If using client_id/secret authentication, use the client_id as username
        if self.lakebase_config.get("client_id"):
            return self.lakebase_config["client_id"]
        
        # Otherwise, determine username from workspace client
        try:
            sp = self.workspace_client.current_service_principal.me()
            return sp.application_id
        except Exception:
            user = self.workspace_client.current_user.me()
            return user.user_name
        
    def _create_rotating_pool(self) -> ConnectionPool:
        """Create a connection pool that automatically rotates credentials with caching"""

        CredentialConnection.workspace_client = self.workspace_client
        CredentialConnection.instance_name = self.lakebase_config["instance_name"]
        # Token cache duration (in minutes, can be overridden via env var)
        cache_duration_minutes = int(os.getenv("DB_TOKEN_CACHE_MINUTES", "50"))
        CredentialConnection._cache_duration = cache_duration_minutes * 60
        # Create pool with custom connection class
        pool = ConnectionPool(
            conninfo=f"dbname={self.database} user={self.username} host={self.host} sslmode={self.ssl_mode}",
            connection_class=CredentialConnection,
            min_size=self.pool_min_size,
            max_size=self.pool_max_size,
            timeout=self.pool_timeout,
            open=True,
            kwargs={
                "autocommit": True,  # Required for the .setup() method to properly commit the checkpoint tables to the database
                "row_factory": dict_row,  # Required because the PostgresSaver implementation accesses database rows using dictionary-style syntax
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            },
        )

        self._test_connection(pool)

        return pool
    
    def _get_cached_credential(self):
        """Get credential from cache or generate a new one if cache is expired"""
        with self._cache_lock:
            current_time = time.time()
            
            # Check if we have a valid cached credential
            if (self._cached_credential is not None and 
                self._cache_timestamp is not None and 
                current_time - self._cache_timestamp < self._cache_duration):
                return self._cached_credential
            
            # Generate new credential
            credential = self.workspace_client.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[self.lakebase_config["instance_name"]]
            )
            
            # Cache the new credential
            self._cached_credential = credential.token
            self._cache_timestamp = current_time
            
            return self._cached_credential
    
    def _test_connection(self, pool: ConnectionPool) -> None:
        """Test the connection to ensure it works"""
        try:
            with pool.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            logger.info(
                f"Connection test successful "
                f"(token_cache={self._cache_duration / 60:.0f} minutes)"
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect using conninfo: {self.conn_info} Error: {e}")
    
    @contextmanager
    def get_connection(self):
        """Context manager to get a connection from the pool"""
        with self._connection_pool.connection() as conn:
            yield conn
    
    def create_checkpointer(self, connection=None) -> PostgresSaver:
        """
        Create a PostgresSaver instance with standard table names.
        
        Args:
            connection: Optional connection to use. If None, uses connection from pool.
            
        Returns:
            PostgresSaver instance with standard table names
            
        Note:
            PostgresSaver uses hardcoded table names and doesn't support 
            custom table naming in the current version.
        """
        if connection is None:
            # This will be used within a context manager
            raise ValueError("Connection must be provided when creating checkpointer")
        
        # Create standard PostgresSaver - no table_name parameter available
        return PostgresSaver(connection)
    
    def get_table_info(self) -> Dict[str, str]:
        """
        Get information about the checkpoint tables being used.
        
        Returns:
            Dictionary with table information
        """
        return {
            "standard_tables": self.standard_tables,
            "database_name": self.lakebase_config.get("conn_db_name", "databricks_postgres"),
            "host": self.lakebase_config["conn_host"],
            "instance_name": self.lakebase_config["instance_name"],
            "note": "PostgresSaver uses standard hardcoded table names"
        }
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the database to verify checkpoint tables exist.
        
        Returns:
            List of table names
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                return [row[0] for row in cur.fetchall()]
    
    def verify_checkpoint_tables(self) -> Dict[str, bool]:
        """
        Verify that the standard checkpoint tables exist.
        
        Returns:
            Dictionary mapping table names to existence status
        """
        try:
            tables = self.list_tables()
            return {
                table_name: table_name in tables
                for table_name in self.standard_tables.values()
            }
        except Exception as e:
            logger.error(f"Error verifying checkpoint tables: {e}")
            return {table_name: False for table_name in self.standard_tables.values()}
    
    def get_checkpoint_config(self, thread_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get checkpoint configuration for LangGraph.
        
        Args:
            thread_id: Thread ID for conversation state. Generates new if None.
            
        Returns:
            Configuration dictionary for LangGraph checkpointing
        """
        if thread_id is None:
            thread_id = str(uuid.uuid4())
            
        return {"configurable": {"thread_id": thread_id}}
    def initialize_checkpoint_tables(
        self, 
        drop_existing: bool = False
    ) -> bool:
        """
        Initialize LangGraph checkpoint tables in PostgreSQL.
        
        Args:
            state_manager: DatabricksStateManager instance (handles auth & connection)
            drop_existing: If True, drop existing tables before creating (DANGER!)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("="*60)
            logger.info("LangGraph Checkpoint Database Initialization")
            logger.info("="*60)
            
            # Test connection using state manager
            logger.info("Testing connection via DatabricksStateManager...")
            try:
                with self.get_connection() as test_conn:
                    logger.info("✓ Database connection successful")
            except Exception as e:
                logger.error(f"✗ Database connection failed: {e}")
                return False
            
            # Drop existing tables if requested
            if drop_existing:
                logger.warning("⚠️  DROP_EXISTING=True - Deleting all checkpoint data!")
                response = input("Are you sure? Type 'yes' to confirm: ")
                if response.lower() != 'yes':
                    logger.info("Aborted by user")
                    return False
                
                logger.info("Dropping existing tables...")
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DROP TABLE IF EXISTS checkpoint_writes CASCADE")
                        cur.execute("DROP TABLE IF EXISTS checkpoint_blobs CASCADE")
                        cur.execute("DROP TABLE IF EXISTS checkpoints CASCADE")
                        conn.commit()
                        logger.info("✓ Existing tables dropped")
            
            # Initialize PostgresSaver (creates tables automatically)
            logger.info("Initializing PostgresSaver...")
            
            with self.get_connection() as conn:
                # Create PostgresSaver instance
                checkpointer = PostgresSaver(conn)
                
                # Call setup to create tables
                checkpointer.setup()
                
                logger.info("✓ PostgresSaver initialized")
                logger.info("✓ Tables created successfully")
            
            # Verify tables exist
            logger.info("\nVerifying table creation...")
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check for required tables
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name IN ('checkpoints', 'checkpoint_blobs', 'checkpoint_writes')
                        ORDER BY table_name
                    """)
                    
                    rows = cur.fetchall()
                    if rows and isinstance(rows[0], dict):
                        tables = [row['table_name'] for row in rows]
                    else:
                        tables = [row[0] for row in rows]
                    
                    expected_tables = ['checkpoint_blobs', 'checkpoint_writes', 'checkpoints']
                    
                    logger.info("\nTable Status:")
                    for table in expected_tables:
                        if table in tables:
                            logger.info(f"  ✓ {table}")
                        else:
                            logger.error(f"  ✗ {table} - MISSING!")
                            return False
                    
                    # Get row counts
                    logger.info("\nTable Statistics:")
                    for table in tables:
                        cur.execute(f"SELECT COUNT(*) FROM {table}")
                        row = cur.fetchone()
                        count = row['count'] if isinstance(row, dict) else row[0]
                        logger.info(f"  {table}: {count} rows")
            
            logger.info("\n" + "="*60)
            logger.info("✓ Initialization Complete!")
            logger.info("="*60)
            logger.info("\nYour agent is ready to use checkpoint persistence.")
            
            return True
            
        except Exception as e:
            logger.error(f"\n✗ Initialization failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def close(self) -> None:
        """Close and cleanup resources (no pool to close in this version)"""
        # Clear cached credentials for security
        with self._cache_lock:
            self._cached_credential = None
            self._cache_timestamp = None
        logger.info("DatabricksStateManager closed and credentials cleared")
    
    def __enter__(self):
        """Context manager entry"""
        self.initialize_checkpoint_tables()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def langchain_to_responses(self, messages: list[BaseMessage]) -> list[dict[str, Any]]:
        """Convert from LangChain messages to Responses API format"""
        responses = []
        for message in messages:
            message_dict = message.model_dump()
            msg_type = message_dict["type"]
            
            if msg_type == "ai":
                if tool_calls := message_dict.get("tool_calls"):
                    for tool_call in tool_calls:
                        responses.append(
                            create_function_call_item(
                                id=message_dict.get("id") or str(uuid.uuid4()),
                                call_id=tool_call["id"],
                                name=tool_call["name"],
                                arguments=json.dumps(tool_call["args"]),
                            )
                        )
                else:
                    responses.append(
                        create_text_output_item(
                            text=message_dict.get("content", ""),
                            id=message_dict.get("id") or str(uuid.uuid4()),
                        )
                    )
            elif msg_type == "tool":
                responses.append(
                    create_function_call_output_item(
                        call_id=message_dict["tool_call_id"],
                        output=message_dict["content"],
                    )
                )
            elif msg_type == "human":
                responses.append({
                    "role": "user",
                    "content": message_dict.get("content", "")
                })
        
        return responses
    def inspect_checkpoint_schema(self) -> Dict[str, Any]:
        """
        Inspect the actual schema of checkpoint tables.
        Useful for debugging.
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                schema_info = {}
                
                for table in ['checkpoints', 'checkpoint_blobs', 'checkpoint_writes']:
                    cur.execute(f"""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """, (table,))
                    
                    columns = cur.fetchall()
                    schema_info[table] = [
                        {
                            'column': row['column_name'] if isinstance(row, dict) else row[0],
                            'type': row['data_type'] if isinstance(row, dict) else row[1]
                        }
                        for row in columns
                    ]
                
                return schema_info

    def get_conversation_history(
        self,
        thread_id: str,
        limit: Optional[int] = None,
        include_metadata: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Retrieve conversation history for a specific thread/conversation.
        Uses LangGraph's PostgresSaver.get_tuple() API for reliable state retrieval.
        
        Args:
            thread_id: The conversation/thread identifier
            limit: Maximum number of messages to return (None = all)
            include_metadata: Include checkpoint metadata like timestamps
            
        Returns:
            List of message dictionaries in chronological order
        """
        try:
            with self.get_connection() as conn:
                # Create checkpointer using LangGraph's API
                checkpointer = PostgresSaver(conn)
                
                # Get the checkpoint config for this thread
                config = {"configurable": {"thread_id": thread_id}}
                
                # Use get_tuple to get the CheckpointTuple
                checkpoint_tuple = checkpointer.get_tuple(config)
                
                if not checkpoint_tuple:
                    logger.info(f"No checkpoint found for thread_id: {thread_id}")
                    return []
                
                # CheckpointTuple has these attributes:
                # - checkpoint: The actual checkpoint dict
                # - config: The config used
                # - metadata: Metadata dict
                # - parent_config: Parent checkpoint config (optional)
                # - pending_writes: List of pending writes
                
                # The checkpoint dict contains 'channel_values' which has the state
                checkpoint = checkpoint_tuple.checkpoint
                
                if not checkpoint:
                    logger.info(f"No checkpoint data for thread_id: {thread_id}")
                    return []
                
                # Extract channel_values from checkpoint
                channel_values = checkpoint.get('channel_values', {})
                
                if not channel_values:
                    logger.info(f"No channel_values in checkpoint for thread_id: {thread_id}")
                    return []
                
                # Get messages from channel_values
                messages_data = channel_values.get('messages', [])
                
                if not messages_data:
                    logger.info(f"No messages found for thread_id: {thread_id}")
                    return []
                
                # Convert LangChain messages to simple format
                messages = []
                for msg in messages_data:
                    try:
                        # Handle different message formats
                        if hasattr(msg, 'model_dump'):
                            msg_dict = msg.model_dump()
                        elif hasattr(msg, 'dict'):
                            msg_dict = msg.dict()
                        elif isinstance(msg, dict):
                            msg_dict = msg
                        else:
                            logger.debug(f"Unknown message type: {type(msg)}")
                            continue
                        
                        msg_type = msg_dict.get('type', '')
                        content = msg_dict.get('content', '')
                        
                        # Skip empty messages
                        if not content:
                            continue
                        
                        # Map LangChain message types to simple roles
                        if msg_type == 'human':
                            role = 'user'
                        elif msg_type == 'ai':
                            role = 'assistant'
                            # Skip AI messages that only contain tool calls without text
                            if msg_dict.get('tool_calls') and not content:
                                continue
                        elif msg_type in ['tool', 'system']:
                            # Skip tool and system messages from user-facing history
                            continue
                        else:
                            continue
                        
                        message = {
                            'role': role,
                            'content': content
                        }
                        
                        if include_metadata:
                            # Get metadata from checkpoint_tuple
                            if checkpoint_tuple.metadata:
                                message['timestamp'] = checkpoint_tuple.metadata.get('ts')
                            message['message_id'] = msg_dict.get('id', str(uuid.uuid4()))
                            if checkpoint_tuple.config:
                                message['checkpoint_id'] = checkpoint_tuple.config.get('configurable', {}).get('checkpoint_id')
                        
                        messages.append(message)
                        
                    except Exception as e:
                        logger.warning(f"Error processing message: {e}")
                        continue
                
                # Apply limit if specified (take last N messages)
                if limit and len(messages) > limit:
                    messages = messages[-limit:]
                
                return messages
                
        except Exception as e:
            logger.error(f"Error retrieving conversation history: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def get_conversation_history_with_checkpoints(
        self,
        thread_id: str,
        include_metadata: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get conversation history by iterating through all checkpoints.
        Useful if you want to see the evolution of the conversation.
        
        Returns:
            List of message dictionaries with checkpoint information
        """
        try:
            with self.get_connection() as conn:
                checkpointer = PostgresSaver(conn)
                config = {"configurable": {"thread_id": thread_id}}
                
                # List all checkpoints for this thread
                checkpoints = []
                for checkpoint_tuple in checkpointer.list(config):
                    checkpoints.append(checkpoint_tuple)
                
                if not checkpoints:
                    logger.info(f"No checkpoints found for thread_id: {thread_id}")
                    return []
                
                # Get the latest checkpoint (last one in the list)
                latest_checkpoint = checkpoints[-1]
                state = checkpointer.get_tuple(latest_checkpoint.config)
                
                if not state or not state.checkpoint or not state.checkpoint.get('channel_values'):
                    return []
                
                # Extract messages
                messages_data = state.checkpoint['channel_values'].get('messages', [])
                
                messages = []
                for msg in messages_data:
                    try:
                        if hasattr(msg, 'model_dump'):
                            msg_dict = msg.model_dump()
                        else:
                            msg_dict = msg
                        
                        msg_type = msg_dict.get('type', '')
                        content = msg_dict.get('content', '')
                        
                        if not content:
                            continue
                        
                        if msg_type == 'human':
                            role = 'user'
                        elif msg_type == 'ai':
                            role = 'assistant'
                            if msg_dict.get('tool_calls') and not content:
                                continue
                        elif msg_type in ['tool', 'system']:
                            continue
                        else:
                            continue
                        
                        message = {
                            'role': role,
                            'content': content
                        }
                        
                        if include_metadata:
                            message['timestamp'] = state.metadata.get('ts') if state.metadata else None
                            message['message_id'] = msg_dict.get('id')
                            message['checkpoint_id'] = state.config.get('configurable', {}).get('checkpoint_id')
                        
                        messages.append(message)
                        
                    except Exception as e:
                        logger.warning(f"Error processing message: {e}")
                        continue
                
                return messages
                
        except Exception as e:
            logger.error(f"Error retrieving conversation with checkpoints: {e}")
            return []
    
    def get_checkpoint_history(
        self,
        thread_id: str,
        limit: int = 10,
        before_checkpoint_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve full checkpoint history for a thread using native LangGraph API.
        This enables time-travel, debugging, and observability features.
        
        Args:
            thread_id: Thread identifier
            limit: Maximum number of checkpoints to return
            before_checkpoint_id: Get checkpoints before this checkpoint (for pagination)
        
        Returns:
            List of checkpoint dictionaries with:
            - checkpoint_id: Unique checkpoint identifier
            - thread_id: Thread this checkpoint belongs to
            - timestamp: ISO timestamp of checkpoint creation
            - created_at: Python datetime of creation
            - next_nodes: List of nodes scheduled to execute next
            - parent_checkpoint_id: Previous checkpoint (for time-travel)
            - message_count: Number of messages in state at this checkpoint
            - last_message: Snippet of last message (for context)
            - state_summary: High-level description of state
        
        Example:
            # Get last 20 checkpoints for debugging
            history = state_manager.get_checkpoint_history("thread-123", limit=20)
            for cp in history:
                print(f"{cp['timestamp']}: {cp['message_count']} messages, next: {cp['next_nodes']}")
        """
        try:
            with self.get_connection() as conn:
                checkpointer = PostgresSaver(conn)
                config = {"configurable": {"thread_id": thread_id}}
                
                # Add before parameter if specified
                list_kwargs = {"limit": limit}
                if before_checkpoint_id:
                    list_kwargs["before"] = {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_id": before_checkpoint_id
                        }
                    }
                
                # Use native LangGraph list() API - key capability for observability
                checkpoint_history = []
                for state in checkpointer.list(config, **list_kwargs):
                    checkpoint_info = {
                        "checkpoint_id": state.config["configurable"]["checkpoint_id"],
                        "thread_id": thread_id,
                        "timestamp": state.metadata.get("ts") if state.metadata else None,
                        "created_at": state.created_at,
                        "next_nodes": state.next,  # Shows what's pending - critical for debugging
                        "parent_checkpoint_id": (
                            state.parent_config["configurable"]["checkpoint_id"]
                            if state.parent_config else None
                        ),
                        "message_count": len(state.values.get("messages", [])) if state.values else 0,
                        "last_message": self._get_last_message_summary(
                            state.values.get("messages", []) if state.values else []
                        ),
                        "state_summary": self._create_state_summary(state)
                    }
                    checkpoint_history.append(checkpoint_info)
                
                return checkpoint_history
                
        except Exception as e:
            logger.error(f"Error retrieving checkpoint history: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def _get_last_message_summary(self, messages: List[Any]) -> Optional[str]:
        """Get snippet of last message for checkpoint identification"""
        if not messages:
            return None
        
        try:
            last_msg = messages[-1]
            if hasattr(last_msg, 'content'):
                content = last_msg.content
            elif isinstance(last_msg, dict):
                content = last_msg.get('content', '')
            else:
                return None
            
            # Return first 100 chars for readability
            return content[:100] if content else None
        except Exception:
            return None
    
    def _create_state_summary(self, state) -> str:
        """Create a human-readable summary of checkpoint state"""
        try:
            if not state.values:
                return "Empty state"
            
            messages = state.values.get("messages", [])
            msg_count = len(messages)
            
            # Get last message type
            last_msg_type = "unknown"
            if messages:
                last_msg = messages[-1]
                if hasattr(last_msg, 'type'):
                    last_msg_type = last_msg.type
                elif isinstance(last_msg, dict):
                    last_msg_type = last_msg.get('type', 'unknown')
            
            # Check for tool calls
            has_tool_calls = False
            if messages and hasattr(messages[-1], 'tool_calls'):
                has_tool_calls = bool(messages[-1].tool_calls)
            
            parts = [f"{msg_count} messages"]
            if last_msg_type:
                parts.append(f"last: {last_msg_type}")
            if has_tool_calls:
                parts.append("with tool calls")
            if state.next:
                parts.append(f"next: {','.join(state.next)}")
            
            return " | ".join(parts)
            
        except Exception:
            return "Unknown state"

    def list_all_conversations(
        self,
        limit: Optional[int] = 100
    ) -> List[Dict[str, Any]]:
        """
        List all conversation threads using LangGraph API.
        
        Args:
            limit: Maximum number of conversations to return
            
        Returns:
            List of conversation summaries
        """
        try:
            with self.get_connection() as conn:
                checkpointer = PostgresSaver(conn)
                
                # Use direct SQL query since LangGraph doesn't have a "list all threads" method
                with conn.cursor() as cur:
                    query = """
                        SELECT DISTINCT 
                            thread_id,
                            COUNT(*) as checkpoint_count,
                            MIN(metadata->>'ts') as first_checkpoint,
                            MAX(metadata->>'ts') as last_checkpoint
                        FROM checkpoints
                        GROUP BY thread_id
                        ORDER BY MAX(metadata->>'ts') DESC
                    """
                    
                    if limit:
                        query += f" LIMIT {limit}"
                    
                    cur.execute(query)
                    rows = cur.fetchall()
                    
                    conversations = []
                    for row in rows:
                        conversations.append({
                            'thread_id': row['thread_id'] if isinstance(row, dict) else row[0],
                            'checkpoint_count': row['checkpoint_count'] if isinstance(row, dict) else row[1],
                            'first_checkpoint': row['first_checkpoint'] if isinstance(row, dict) else row[2],
                            'last_checkpoint': row['last_checkpoint'] if isinstance(row, dict) else row[3]
                        })
                    
                    return conversations
                    
        except Exception as e:
            logger.error(f"Error listing conversations: {e}")
            return []
    
    def update_checkpoint_state(
        self,
        thread_id: str,
        checkpoint_id: str,
        graph_builder: Any,
        values: Optional[Dict[str, Any]] = None,
        new_messages: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, str]:
        """
        Update state at a specific checkpoint without re-execution.
        Creates a NEW checkpoint with updated state - critical for human-in-the-loop workflows.
        
        Args:
            thread_id: Thread identifier
            checkpoint_id: Checkpoint to update from
            graph_builder: Callable that takes checkpointer and returns compiled graph
            values: State updates (e.g., {"custom_state": "new_value"})
            new_messages: New messages to append (format: [{"role": "user", "content": "..."}])
        
        Returns:
            Dict with:
                - thread_id: Same thread
                - new_checkpoint_id: The newly created checkpoint
                - parent_checkpoint_id: The original checkpoint_id
        
        Example - Human correction:
            def my_graph_builder(checkpointer):
                builder = StateGraph(AgentState)
                # ... add nodes ...
                return builder.compile(checkpointer=checkpointer)
            
            # Agent picked wrong table, human corrects
            result = state_manager.update_checkpoint_state(
                thread_id="session-123",
                checkpoint_id="checkpoint-456",
                graph_builder=my_graph_builder,
                values={"selected_table": "customers_v2"},  # Override
                new_messages=[{
                    "role": "user", 
                    "content": "Actually, use customers_v2 table"
                }]
            )
            # Continue from result['new_checkpoint_id']
        """
        try:
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_id": checkpoint_id
                }
            }
            
            with self.get_connection() as conn:
                checkpointer = PostgresSaver(conn)
                
                # Build the graph with this checkpointer
                graph = graph_builder(checkpointer)
                
                # Prepare values to update
                update_values = {}
                
                if values:
                    update_values.update(values)
                
                if new_messages:
                    # Convert messages to LangChain format
                    from langchain_core.messages import HumanMessage, AIMessage
                    lc_messages = []
                    for msg in new_messages:
                        if msg.get("role") == "user":
                            lc_messages.append(HumanMessage(content=msg["content"]))
                        elif msg.get("role") == "assistant":
                            lc_messages.append(AIMessage(content=msg["content"]))
                    
                    if lc_messages:
                        update_values["messages"] = lc_messages
                
                # Use native LangGraph update_state - key method for human-in-the-loop
                new_config = graph.update_state(config, values=update_values)
                
                logger.info(
                    f"Updated checkpoint: {thread_id}/{checkpoint_id} -> "
                    f"{new_config['configurable']['checkpoint_id']}"
                )
                
                return {
                    "thread_id": thread_id,
                    "new_checkpoint_id": new_config["configurable"]["checkpoint_id"],
                    "parent_checkpoint_id": checkpoint_id
                }
                
        except Exception as e:
            logger.error(f"Error updating checkpoint state: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def fork_conversation(
        self,
        thread_id: str,
        checkpoint_id: str,
        new_thread_id: Optional[str] = None,
        branch_label: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Create a new conversation branch from a specific checkpoint.
        Enables A/B testing, hypothesis testing, and multi-path exploration.
        
        Args:
            thread_id: Source thread
            checkpoint_id: Checkpoint to fork from
            new_thread_id: New thread ID (auto-generated if None)
            branch_label: Optional label for tracking (e.g., "approach_A", "hypothesis_vectorsearch")
        
        Returns:
            Dict with:
                - source_thread_id: Original thread
                - source_checkpoint_id: Original checkpoint
                - new_thread_id: New forked thread
                - new_checkpoint_id: Starting checkpoint of fork
                - branch_label: Label if provided
        
        Example - A/B testing different approaches:
            # Test two different search strategies from same point
            fork_a = state_manager.fork_conversation(
                thread_id="session-123",
                checkpoint_id="checkpoint-50",
                new_thread_id="session-123-fork-a",
                branch_label="vector_search_approach"
            )
            
            fork_b = state_manager.fork_conversation(
                thread_id="session-123",
                checkpoint_id="checkpoint-50",
                new_thread_id="session-123-fork-b",
                branch_label="sql_query_approach"
            )
            
            # Continue both forks independently and compare results
        """
        try:
            if new_thread_id is None:
                new_thread_id = f"{thread_id}_fork_{uuid.uuid4().hex[:8]}"
            
            # Get the state at the checkpoint we want to fork from
            source_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_id": checkpoint_id
                }
            }
            
            with self.get_connection() as conn:
                checkpointer = PostgresSaver(conn)
                
                # Get the checkpoint tuple we're forking from
                source_checkpoint_tuple = checkpointer.get_tuple(source_config)
                
                if not source_checkpoint_tuple:
                    raise ValueError(
                        f"Checkpoint {checkpoint_id} not found in thread {thread_id}"
                    )
                
                # Create new config for the fork
                fork_config = {
                    "configurable": {
                        "thread_id": new_thread_id,
                        # Don't specify checkpoint_id - PostgresSaver will create new one
                    }
                }
                
                # Copy the checkpoint to new thread
                checkpoint_data = source_checkpoint_tuple.checkpoint
                metadata = source_checkpoint_tuple.metadata.copy() if source_checkpoint_tuple.metadata else {}
                
                # Add fork metadata for tracking
                if branch_label:
                    metadata["branch_label"] = branch_label
                metadata["forked_from_thread"] = thread_id
                metadata["forked_from_checkpoint"] = checkpoint_id
                metadata["fork_timestamp"] = datetime.utcnow().isoformat()
                
                # Write the forked checkpoint using native PostgresSaver API
                checkpointer.put(
                    config=fork_config,
                    checkpoint=checkpoint_data,
                    metadata=metadata,
                    new_versions={}  # No new versions at fork point
                )
                
                # Get the new checkpoint ID that was created
                new_checkpoint_tuple = checkpointer.get_tuple(fork_config)
                new_checkpoint_id = new_checkpoint_tuple.config["configurable"]["checkpoint_id"]
                
                logger.info(
                    f"Forked conversation: {thread_id}/{checkpoint_id} -> "
                    f"{new_thread_id}/{new_checkpoint_id}"
                    + (f" (label: {branch_label})" if branch_label else "")
                )
                
                return {
                    "source_thread_id": thread_id,
                    "source_checkpoint_id": checkpoint_id,
                    "new_thread_id": new_thread_id,
                    "new_checkpoint_id": new_checkpoint_id,
                    "branch_label": branch_label
                }
                
        except Exception as e:
            logger.error(f"Error forking conversation: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def resume_from_checkpoint(
        self,
        thread_id: str,
        checkpoint_id: str,
        graph_builder: Any,
        new_input: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Resume execution from a specific checkpoint with new input.
        Enables time-travel: "What if I had asked this instead at step 5?"
        
        Args:
            thread_id: Thread identifier
            checkpoint_id: Checkpoint to resume from
            graph_builder: Callable that takes checkpointer and returns compiled graph
            new_input: New input to provide (e.g., {"messages": [HumanMessage(...)]})
        
        Returns:
            Dict with execution results and new checkpoint info
        
        Example - Time travel:
            # "Actually, let me try a different question from checkpoint 10"
            def my_graph_builder(checkpointer):
                return builder.compile(checkpointer=checkpointer)
            
            result = state_manager.resume_from_checkpoint(
                thread_id="session-123",
                checkpoint_id="checkpoint-10",
                graph_builder=my_graph_builder,
                new_input={
                    "messages": [HumanMessage(content="Different question here")]
                }
            )
        """
        try:
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_id": checkpoint_id
                }
            }
            
            with self.get_connection() as conn:
                checkpointer = PostgresSaver(conn)
                
                # Build graph with checkpointer
                graph = graph_builder(checkpointer)
                
                # Invoke graph from this checkpoint
                result = graph.invoke(new_input, config)
                
                # Get the new checkpoint that was created
                new_state = checkpointer.get_tuple({"configurable": {"thread_id": thread_id}})
                
                logger.info(
                    f"Resumed from checkpoint: {thread_id}/{checkpoint_id} -> "
                    f"{new_state.config['configurable']['checkpoint_id']}"
                )
                
                return {
                    "result": result,
                    "new_checkpoint_id": new_state.config["configurable"]["checkpoint_id"],
                    "parent_checkpoint_id": checkpoint_id
                }
                
        except Exception as e:
            logger.error(f"Error resuming from checkpoint: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def get_conversation_summary(
        self,
        thread_id: str
    ) -> Dict[str, Any]:
        """
        Get summary statistics for a conversation using LangGraph API.
        """
        messages = self.get_conversation_history(thread_id, include_metadata=True)
        
        if not messages:
            return {
                'thread_id': thread_id,
                'exists': False,
                'message_count': 0
            }
        
        user_msgs = [m for m in messages if m['role'] == 'user']
        assistant_msgs = [m for m in messages if m['role'] == 'assistant']
        timestamps = [m['timestamp'] for m in messages if m.get('timestamp')]
        
        return {
            'thread_id': thread_id,
            'exists': True,
            'message_count': len(messages),
            'user_message_count': len(user_msgs),
            'assistant_message_count': len(assistant_msgs),
            'first_message_time': min(timestamps) if timestamps else None,
            'last_message_time': max(timestamps) if timestamps else None
        }

    def delete_conversation(
        self,
        thread_id: str,
        confirm: bool = False
    ) -> bool:
        """
        Delete conversation history using LangGraph API.
        """
        if not confirm:
            logger.warning("Delete not confirmed. Set confirm=True to actually delete.")
            return False
        
        try:
            with self.get_connection() as conn:
                checkpointer = PostgresSaver(conn)
                config = {"configurable": {"thread_id": thread_id}}
                
                # Delete using LangGraph's put method with None
                # Actually, LangGraph doesn't have a delete method, so use SQL
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM checkpoint_writes WHERE thread_id = %s", (thread_id,))
                    cur.execute("DELETE FROM checkpoint_blobs WHERE thread_id = %s", (thread_id,))
                    cur.execute("DELETE FROM checkpoints WHERE thread_id = %s", (thread_id,))
                    conn.commit()
                    
                    logger.info(f"Deleted conversation {thread_id}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error deleting conversation: {e}")
            return False

    def get_formatted_conversation(
        self,
        thread_id: str,
        format: str = 'chat'  # 'chat', 'json', or 'markdown'
    ) -> str:
        """
        Get conversation history in a formatted string.
        
        Args:
            thread_id: The conversation/thread identifier
            format: Output format ('chat', 'json', or 'markdown')
            
        Returns:
            Formatted conversation string
        """
        messages = self.get_conversation_history(thread_id, include_metadata=True)
        
        if not messages:
            return f"No conversation found for thread_id: {thread_id}"
        
        if format == 'json':
            return json.dumps(messages, indent=2, default=str)
        
        elif format == 'markdown':
            lines = [f"# Conversation: {thread_id}\n"]
            for msg in messages:
                timestamp = msg.get('timestamp', 'unknown')
                lines.append(f"## {msg['role'].upper()} ({timestamp})")
                lines.append(f"{msg['content']}\n")
            return "\n".join(lines)
        
        else:  # chat format
            lines = [f"Conversation ID: {thread_id}\n{'='*60}\n"]
            for msg in messages:
                role_label = "USER" if msg['role'] == 'user' else "ASSISTANT"
                timestamp = msg.get('timestamp', '')
                time_str = f" [{timestamp}]" if timestamp else ""
                lines.append(f"{role_label}{time_str}:")
                lines.append(f"{msg['content']}\n")
            return "\n".join(lines)

    def delete_conversation(
        self,
        thread_id: str,
        confirm: bool = False
    ) -> bool:
        """
        Delete all conversation history for a thread.
        Uses direct SQL as LangGraph doesn't provide a delete method yet.
        
        Args:
            thread_id: The conversation/thread identifier
            confirm: Must be True to actually delete (safety check)
            
        Returns:
            True if deleted, False otherwise
        """
        if not confirm:
            logger.warning("Delete not confirmed. Set confirm=True to actually delete.")
            return False
        
        try:
            with self.get_connection() as conn:
                checkpointer = PostgresSaver(conn)
                
                # Check if thread exists first
                config = {"configurable": {"thread_id": thread_id}}
                checkpoint_tuple = checkpointer.get_tuple(config)
                
                if not checkpoint_tuple:
                    logger.info(f"No conversation found for thread_id: {thread_id}")
                    return False
                
                # LangGraph doesn't have a built-in delete method yet
                # So we use direct SQL
                with conn.cursor() as cur:
                    # Delete in correct order: children first, then parents
                    cur.execute("DELETE FROM checkpoint_writes WHERE thread_id = %s", (thread_id,))
                    writes_deleted = cur.rowcount
                    
                    cur.execute("DELETE FROM checkpoint_blobs WHERE thread_id = %s", (thread_id,))
                    blobs_deleted = cur.rowcount
                    
                    cur.execute("DELETE FROM checkpoints WHERE thread_id = %s", (thread_id,))
                    checkpoints_deleted = cur.rowcount
                    
                    logger.info(
                        f"Deleted conversation {thread_id}: "
                        f"{checkpoints_deleted} checkpoints, "
                        f"{blobs_deleted} blobs, "
                        f"{writes_deleted} writes"
                    )
                    return True
                    
        except Exception as e:
            logger.error(f"Error deleting conversation: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    def get_checkpoint_details(
        self,
        thread_id: str,
        checkpoint_id: str,
        include_full_state: bool = False
    ) -> Dict[str, Any]:
        """
        Get detailed information about a specific checkpoint.
        Useful for debugging and observability.
        
        Args:
            thread_id: Thread identifier
            checkpoint_id: Checkpoint to inspect
            include_full_state: Include complete state data (can be large)
        
        Returns:
            Dict with checkpoint details including messages, metadata, next nodes, etc.
        """
        try:
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_id": checkpoint_id
                }
            }
            
            with self.get_connection() as conn:
                checkpointer = PostgresSaver(conn)
                
                checkpoint_tuple = checkpointer.get_tuple(config)
                
                if not checkpoint_tuple:
                    return {
                        "found": False,
                        "thread_id": thread_id,
                        "checkpoint_id": checkpoint_id
                    }
                
                details = {
                    "found": True,
                    "thread_id": thread_id,
                    "checkpoint_id": checkpoint_id,
                    "timestamp": checkpoint_tuple.metadata.get("ts") if checkpoint_tuple.metadata else None,
                    "created_at": checkpoint_tuple.created_at,
                    "next_nodes": checkpoint_tuple.next,
                    "parent_checkpoint_id": (
                        checkpoint_tuple.parent_config["configurable"]["checkpoint_id"]
                        if checkpoint_tuple.parent_config else None
                    ),
                    "metadata": checkpoint_tuple.metadata,
                }
                
                if checkpoint_tuple.checkpoint and checkpoint_tuple.checkpoint.get('channel_values'):
                    messages = checkpoint_tuple.checkpoint['channel_values'].get('messages', [])
                    details["message_count"] = len(messages)
                    details["last_message"] = self._get_last_message_summary(messages)
                    
                    if include_full_state:
                        details["full_state"] = checkpoint_tuple.checkpoint['channel_values']
                
                return details
                
        except Exception as e:
            logger.error(f"Error getting checkpoint details: {e}")
            return {
                "found": False,
                "error": str(e),
                "thread_id": thread_id,
                "checkpoint_id": checkpoint_id
            }
    
    def compare_checkpoints(
        self,
        thread_id: str,
        checkpoint_id_1: str,
        checkpoint_id_2: str
    ) -> Dict[str, Any]:
        """
        Compare two checkpoints to see what changed.
        Useful for debugging and understanding agent behavior.
        
        Args:
            thread_id: Thread identifier
            checkpoint_id_1: First checkpoint
            checkpoint_id_2: Second checkpoint
        
        Returns:
            Dict with comparison results
        """
        try:
            cp1 = self.get_checkpoint_details(thread_id, checkpoint_id_1, include_full_state=True)
            cp2 = self.get_checkpoint_details(thread_id, checkpoint_id_2, include_full_state=True)
            
            if not cp1["found"] or not cp2["found"]:
                return {
                    "error": "One or both checkpoints not found",
                    "checkpoint_1_found": cp1["found"],
                    "checkpoint_2_found": cp2["found"]
                }
            
            comparison = {
                "thread_id": thread_id,
                "checkpoint_1": checkpoint_id_1,
                "checkpoint_2": checkpoint_id_2,
                "message_count_diff": cp2.get("message_count", 0) - cp1.get("message_count", 0),
                "timestamp_1": cp1.get("timestamp"),
                "timestamp_2": cp2.get("timestamp"),
                "next_nodes_1": cp1.get("next_nodes", []),
                "next_nodes_2": cp2.get("next_nodes", []),
                "parent_1": cp1.get("parent_checkpoint_id"),
                "parent_2": cp2.get("parent_checkpoint_id"),
            }
            
            return comparison
            
        except Exception as e:
            logger.error(f"Error comparing checkpoints: {e}")
            return {"error": str(e)}
