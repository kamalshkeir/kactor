import asyncio
import json
import logging
import random
import string
import time
import uuid
from typing import Any, Callable, Dict, Optional, Union
import websockets
from websockets.client import WebSocketClientProtocol
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Callable

@dataclass
class StatePoolConfig:
    """Configuration for creating a state pool."""
    name: str
    size: int
    initial: Dict[str, Any]
    state_size_mb: int

@dataclass
class PublishOptions:
    """Options for publishing messages."""
    def __init__(self, on_success: Optional[Callable[[], None]] = None, on_failure: Optional[Callable[[Exception], None]] = None):
        self.on_success = on_success
        self.on_failure = on_failure

@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    max_backoff: int = 5  # seconds

@dataclass
class KactorConfig:
    """Configuration for the WebSocket client."""
    address: str = "localhost:9313"
    path: str = "/ws/kactor"
    id: str = ""
    secure: bool = False
    auto_reconnect: bool = True
    max_retries: int = 5
    backoff_min: int = 100  # ms
    backoff_max: int = 5000  # ms
    on_open: Optional[Callable[[], None]] = None

class Subscription:
    """Interface for managing subscriptions."""
    def __init__(self, topic: str, sub_id: str, client: 'Kactor'):
        self.topic = topic
        self.sub_id = sub_id
        self.client = client
        
    def get_topic(self) -> str:
        """Get the topic of this subscription."""
        return self.topic
        
    async def unsubscribe(self):
        """Unsubscribe from the topic."""
        if self.client:
            await self.client.unsubscribe(self.topic, self.sub_id)

class Kactor:
    """A Python client for the kactor WebSocket server.
    
    Supports both actor pools and state pools with automatic reconnection
    and proper message handling.
    """
    
    def __init__(self, config: KactorConfig):
        """Initialize the client with the given configuration.
        
        Args:
            config: KactorConfig object containing:
                - address: Server address (e.g. 'localhost:9313')
                - path: WebSocket path (e.g. '/ws/kactor')
                - id: Unique client identifier (auto-generated if not provided)
                - secure: Whether to use wss:// (default: False)
                - auto_reconnect: Whether to auto reconnect (default: True)
                - max_retries: Max reconnection attempts (default: 5)
                - backoff_min: Min backoff in ms (default: 100)
                - backoff_max: Max backoff in ms (default: 5000)
                - on_open: Callback when connection is established (default: None)
        """
        if isinstance(config, dict):
            config = KactorConfig(**config)
            
        self.config = config
        if not self.config.id:
            self.config.id = f'client-{int(time.time())}-{"".join(random.choices(string.ascii_letters + string.digits, k=6))}'
            
        self.ws: Optional[WebSocketClientProtocol] = None
        self.connected = False
        self.closed = False
        
        self._pending_messages: Dict[str, asyncio.Future] = {}
        self._actor_pool_handlers: Dict[str, Callable] = {}
        self._state_pool_handlers: Dict[str, Callable] = {}
        self._subscriptions: Dict[str, Dict[str, Any]] = {}
        self._message_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        
        # Connect immediately
        asyncio.create_task(self.connect())
        
    async def connect(self):
        """Connect to the WebSocket server."""
        protocol = 'wss' if self.config.secure else 'ws'
        url = f"{protocol}://{self.config.address}{self.config.path}"
        
        retries = 0
        while not self.connected and not self.closed:
            try:
                self.ws = await websockets.connect(url)
                self.connected = True
                self._message_task = asyncio.create_task(self._handle_messages())
                if self.config.on_open:
                    self.config.on_open()
                return
            except Exception as e:
                if not self.config.auto_reconnect or retries >= self.config.max_retries:
                    raise ConnectionError(f"Failed to connect: {e}")
                
                delay = min(self.config.backoff_min * (2 ** retries), self.config.backoff_max)
                delay_with_jitter = delay + random.randint(0, delay // 2)
                await asyncio.sleep(delay_with_jitter / 1000)
                retries += 1
    
    async def close(self):
        """Close the WebSocket connection."""
        self.closed = True
        if self._message_task:
            self._message_task.cancel()
            try:
                await self._message_task
            except asyncio.CancelledError:
                pass
        if self.ws:
            await self.ws.close()
            
    async def _handle_messages(self):
        """Handle incoming WebSocket messages."""
        if not self.ws:
            return
            
        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)
                    msg_type = data.get('type')
                    msg_id = data.get('id')
                    
                    logging.debug(f"Received message: {data}")
                    
                    if msg_type == 'actor_pool_message':
                        pool = data.get('payload', {}).get('pool')
                        if pool in self._actor_pool_handlers:
                            handler = self._actor_pool_handlers[pool]
                            handler(data.get('payload', {}).get('data', {}))
                            
                    elif msg_type == 'state_pool_message':
                        pool = data.get('pool')
                        if pool in self._state_pool_handlers:
                            handler = self._state_pool_handlers[pool]
                            handler(data.get('data'))
                            
                    elif msg_type == 'message':
                        topic = data.get('topic', '')
                        target = data.get('target', '')
                        sub_key = f"{topic}-{target}"
                        
                        if sub_key in self._subscriptions or topic in self._subscriptions:
                            handler = self._subscriptions.get(sub_key, self._subscriptions.get(topic))['handler']
                            if handler:
                                sub_id = data.get('id')
                                if 'subID' in data.get('payload', {}) and data['payload']['subID']:
                                    sub_id = data['payload']['subID']
                                try:
                                    handler(data.get('payload', {}), {'topic': topic, 'id': sub_id})
                                except Exception as e:
                                    logging.error(f"Handler error: {e}")
                                
                    elif msg_type in ('published', 'subscribed', 'unsubscribed', 'subscribers_status'):
                        if msg_id in self._pending_messages:
                            future = self._pending_messages.pop(msg_id)
                            future.set_result(data)
                            
                    elif msg_type == 'error':
                        if msg_id in self._pending_messages:
                            future = self._pending_messages.pop(msg_id)
                            error_msg = data.get('payload', {}).get('error', 'Unknown server error')
                            future.set_exception(Exception(error_msg))
                            
                    elif msg_type == 'state_pool_state':
                        if msg_id in self._pending_messages:
                            future = self._pending_messages.pop(msg_id)
                            future.set_result(data.get('payload', {}).get('state', {}))
                            
                    elif msg_type in (
                        'actor_pool_created', 'actor_pool_message_sent', 'actor_pool_stopped', 'actor_pool_removed',
                        'state_pool_created', 'state_pool_message_sent', 'state_pool_updated', 'state_pool_stopped', 'state_pool_removed'
                    ):
                        if msg_id in self._pending_messages:
                            future = self._pending_messages.pop(msg_id)
                            future.set_result(data)  # Return entire message
                            
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logging.error(f"Error handling message: {e}")
                    
        except websockets.ConnectionClosed:
            self.connected = False
            if self.config.auto_reconnect and not self.closed:
                await self.connect()
                
    # Actor Pool Methods
    
    async def create_actor_pool(self, name: str, size: int = 1) -> Dict[str, Any]:
        """Create a new actor pool.
        
        Args:
            name: Name of the pool
            size: Number of actors in the pool (default: 1)
            
        Returns:
            Response data from server
        """
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'create_actor_pool',
            'payload': {
                'config': {
                    'name': name,
                    'size': size
                }
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to create_actor_pool")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in create_actor_pool: {str(e)}")
            raise
            
    async def send_to_actor_pool(self, pool_name: str, data: Any) -> Dict[str, Any]:
        """Send a message to an actor pool.
        
        Args:
            pool_name: Name of the target pool
            data: Message data to send
            
        Returns:
            Response data from server
        """
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'actor_pool_message',
            'payload': {
                'pool': pool_name,
                'data': data
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to actor_pool_message")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in actor_pool_message: {str(e)}")
            raise
            
    def on_actor_pool_message(self, pool_name: str, handler: Callable[[Any], None]):
        """Register a message handler for an actor pool.
        
        Args:
            pool_name: Name of the pool to handle messages from
            handler: Callback function that takes message data as argument
        """
        self._actor_pool_handlers[pool_name] = handler
        
    def remove_actor_pool_handler(self, pool_name: str):
        """Remove the message handler for an actor pool.
        
        Args:
            pool_name: Name of the pool to remove handler for
        """
        self._actor_pool_handlers.pop(pool_name, None)
        
    async def stop_actor_pool(self, pool_name: str) -> Dict[str, Any]:
        """Stop an actor pool.
        
        Args:
            pool_name: Name of the pool to stop
            
        Returns:
            Response data from server
        """
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'stop_actor_pool',
            'payload': {
                'pool': pool_name
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to stop_actor_pool")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in stop_actor_pool: {str(e)}")
            raise
            
    async def remove_actor_pool(self, pool_name: str) -> Dict[str, Any]:
        """Remove an actor pool.
        
        Args:
            pool_name: Name of the pool to remove
            
        Returns:
            Response data from server
        """
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'remove_actor_pool',
            'payload': {
                'pool': pool_name
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to remove_actor_pool")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in remove_actor_pool: {str(e)}")
            raise
        
    # State Pool Methods
    
    async def create_state_pool(self, config: Union[StatePoolConfig, Dict[str, Any]]) -> Dict[str, Any]:
        """Create a new state pool."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        if isinstance(config, StatePoolConfig):
            config = {
                'name': config.name,
                'size': config.size,
                'initial': config.initial,
                'state_size_mb': config.state_size_mb
            }
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'create_state_pool',
            'payload': {
                'config': config
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to create_state_pool")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in create_state_pool: {str(e)}")
            raise
            
    async def send_to_state_pool(self, pool_name: str, data: Any) -> Dict[str, Any]:
        """Send a message to a state pool."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'send_state_pool_message',
            'payload': {
                'pool': pool_name,
                'data': data
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to send_state_pool_message")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in send_state_pool_message: {str(e)}")
            raise
            
    def on_state_pool_message(self, pool_name: str, handler: Callable[[Any], None]):
        """Register a message handler for a state pool.
        
        Args:
            pool_name: Name of the pool to handle messages from
            handler: Callback function that takes message data as argument
        """
        self._state_pool_handlers[pool_name] = handler
        
    def remove_state_pool_handler(self, pool_name: str):
        """Remove the message handler for a state pool.
        
        Args:
            pool_name: Name of the pool to remove handler for
        """
        self._state_pool_handlers.pop(pool_name, None)
        
    async def stop_state_pool(self, pool_name: str) -> Dict[str, Any]:
        """Stop a state pool."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'stop_state_pool',
            'payload': {
                'pool': pool_name
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to stop_state_pool")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in stop_state_pool: {str(e)}")
            raise
            
    async def remove_state_pool(self, pool_name: str) -> Dict[str, Any]:
        """Remove a state pool."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'remove_state_pool',
            'payload': {
                'pool': pool_name
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to remove_state_pool")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in remove_state_pool: {str(e)}")
            raise
            
    async def get_state(self, pool_name: str) -> Dict[str, Any]:
        """Get the current state of a state pool."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'state_pool_state',
            'payload': {
                'pool': pool_name
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to state_pool_state")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in state_pool_state: {str(e)}")
            raise
            
    async def update_state_pool(self, pool_name: str, state: Dict[str, Any]) -> Dict[str, Any]:
        """Update the state of a state pool."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'update_state_pool',
            'payload': {
                'pool': pool_name,
                'updates': state
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            return await asyncio.wait_for(future, 5.0)
        except asyncio.TimeoutError:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Timeout waiting for response to update_state_pool")
            raise
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f"Error in update_state_pool: {str(e)}")
            raise
        
    # PubSub Methods
    async def subscribe(self, topic: str, sub_id: str, handler: Callable[[Any, Dict[str, Any]], None]) -> Optional[Subscription]:
        """Subscribe to a topic."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        # Use client.id as default subID if empty
        sub_id = sub_id or self.config.id
        
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'subscribe',
            'topic': topic,
            'target': sub_id
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            await asyncio.wait_for(future, 5.0)
            
            # Store subscription info for both topic and topic+subID
            self._subscriptions[topic] = {'id': sub_id, 'handler': handler}
            self._subscriptions[f"{topic}-{sub_id}"] = {'id': sub_id, 'handler': handler}
            
            return Subscription(topic, sub_id, self)
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f'Subscription failed: {e}')
            return None

    async def publish(self, topic: str, payload: Dict[str, Any], options: Optional[PublishOptions] = None) -> bool:
        """Publish a message to a topic."""
        if not self.ws or not self.connected:
            if options and options.on_failure:
                options.on_failure(Exception("Not connected"))
            return False

        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'type': 'publish',
            'topic': topic,
            'id': msg_id,
            'payload': payload
        }

        if options is None:
            message['payload']['no_ack'] = True
            try:
                async with self._lock:
                    await self.ws.send(json.dumps(message))
                return True
            except Exception as e:
                return False

        future = asyncio.Future()
        self._pending_messages[msg_id] = future

        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            
            try:
                response = await asyncio.wait_for(future, 5.0)
                if response['type'] == 'published':
                    if options.on_success:
                        options.on_success()
                    return True
                elif response['type'] == 'error':
                    error = response.get('payload', {}).get('error', 'publish failed')
                    if options.on_failure:
                        options.on_failure(Exception(error))
                    return False
            except asyncio.TimeoutError:
                if options.on_failure:
                    options.on_failure(Exception('publish timeout'))
                return False
            except Exception as e:
                if options.on_failure:
                    options.on_failure(e)
                return False
            return False
        except Exception as e:
            if options.on_failure:
                options.on_failure(e)
            return False
        finally:
            self._pending_messages.pop(msg_id, None)

    async def publish_with_retry(self, topic: str, payload: Dict[str, Any], retry_config: RetryConfig, options: Optional[PublishOptions] = None) -> bool:
        """Publish a message with retry configuration."""
        if not self.ws or not self.connected:
            if options and options.on_failure:
                options.on_failure(Exception("Not connected"))
            return False

        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'type': 'publishWithRetry',
            'topic': topic,
            'id': msg_id,
            'payload': {
                'data': payload,
                'retry_config': {
                    'max_attempts': retry_config.max_attempts,
                    'max_backoff': retry_config.max_backoff
                }
            }
        }

        if options is None:
            message['payload']['no_ack'] = True
            try:
                async with self._lock:
                    await self.ws.send(json.dumps(message))
                return True
            except Exception as e:
                return False

        future = asyncio.Future()
        self._pending_messages[msg_id] = future

        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            
            try:
                response = await asyncio.wait_for(future, 5.0)
                if response['type'] == 'published':
                    if options.on_success:
                        options.on_success()
                    return True
                elif response['type'] == 'error':
                    error = response.get('payload', {}).get('error', 'publish failed')
                    if options.on_failure:
                        options.on_failure(Exception(error))
                    return False
            except asyncio.TimeoutError:
                if options.on_failure:
                    options.on_failure(Exception('publish timeout'))
                return False
            except Exception as e:
                if options.on_failure:
                    options.on_failure(e)
                return False
            return False
        except Exception as e:
            if options.on_failure:
                options.on_failure(e)
            return False
        finally:
            self._pending_messages.pop(msg_id, None)

    async def publish_to(self, topic: str, target: str, payload: Dict[str, Any], options: Optional[PublishOptions] = None) -> bool:
        """Publish a message directly to a specific client."""
        if not self.ws or not self.connected:
            if options and options.on_failure:
                options.on_failure(Exception("Not connected"))
            return False

        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'type': 'publishTo',
            'topic': topic,
            'id': msg_id,
            'target': target,
            'payload': payload
        }

        if options is None:
            message['payload']['no_ack'] = True
            try:
                async with self._lock:
                    await self.ws.send(json.dumps(message))
                return True
            except Exception as e:
                return False

        future = asyncio.Future()
        self._pending_messages[msg_id] = future

        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            
            try:
                response = await asyncio.wait_for(future, 5.0)
                if response['type'] == 'published':
                    if options.on_success:
                        options.on_success()
                    return True
                elif response['type'] == 'error':
                    error = response.get('payload', {}).get('error', 'publish failed')
                    if options.on_failure:
                        options.on_failure(Exception(error))
                    return False
            except asyncio.TimeoutError:
                if options.on_failure:
                    options.on_failure(Exception('publish timeout'))
                return False
            except Exception as e:
                if options.on_failure:
                    options.on_failure(e)
                return False
            return False
        except Exception as e:
            if options.on_failure:
                options.on_failure(e)
            return False
        finally:
            self._pending_messages.pop(msg_id, None)

    async def publish_to_with_retry(self, topic: str, target: str, payload: Dict[str, Any], retry_config: RetryConfig, options: Optional[PublishOptions] = None) -> bool:
        """Publish a message directly to a specific client with retry configuration."""
        if not self.ws or not self.connected:
            if options and options.on_failure:
                options.on_failure(Exception("Not connected"))
            return False

        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'type': 'publishToWithRetry',
            'topic': topic,
            'id': msg_id,
            'target': target,
            'payload': {
                'data': payload,
                'retry_config': {
                    'max_attempts': retry_config.max_attempts,
                    'max_backoff': retry_config.max_backoff
                }
            }
        }

        if options is None:
            message['payload']['no_ack'] = True
            try:
                async with self._lock:
                    await self.ws.send(json.dumps(message))
                return True
            except Exception as e:
                return False

        future = asyncio.Future()
        self._pending_messages[msg_id] = future

        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            
            try:
                response = await asyncio.wait_for(future, 5.0)
                if response['type'] == 'published':
                    if options.on_success:
                        options.on_success()
                    return True
                elif response['type'] == 'error':
                    error = response.get('payload', {}).get('error', 'publish failed')
                    if options.on_failure:
                        options.on_failure(Exception(error))
                    return False
            except asyncio.TimeoutError:
                if options.on_failure:
                    options.on_failure(Exception('publish timeout'))
                return False
            except Exception as e:
                if options.on_failure:
                    options.on_failure(e)
                return False
            return False
        except Exception as e:
            if options.on_failure:
                options.on_failure(e)
            return False
        finally:
            self._pending_messages.pop(msg_id, None)

    async def has_subscribers(self, topic: str) -> bool:
        """Check if a topic has any subscribers."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'has_subscribers',
            'topic': topic
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            response = await asyncio.wait_for(future, 5.0)
            return response.get('has_subscribers', False)
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f'Failed to check subscribers: {e}')
            return False

    async def publish_to_server(self, server_addr: str, message: Dict[str, Any], options: Dict[str, Callable] = None, path: Optional[str] = None) -> bool:
        """Publish a message to a specific server."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        payload = {
            'server_addr': server_addr,
            'data': message
        }
        if path:
            payload['path'] = path
            
        message = {
            'id': msg_id,
            'type': 'publishToServer',
            'payload': payload
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            await asyncio.wait_for(future, 5.0)
            
            if options and 'onSuccess' in options:
                options['onSuccess']()
            return True
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            if options and 'onFailure' in options:
                options['onFailure'](e)
            return False

    # def on_topic_message(self, topic: str, handler: Callable[[Any, Dict[str, Any]], None]):
    #     """Register a message handler for a topic.
        
    #     Args:
    #         topic: Topic to handle messages from
    #         handler: Callback function that takes message data and subscription info as arguments
    #     """
    #     if topic in self._subscriptions:
    #         self._subscriptions[topic]['handler'] = handler
            
    def remove_topic_handler(self, topic: str):
        """Remove the message handler for a topic.
        
        Args:
            topic: Topic to remove handler for
        """
        if topic in self._subscriptions:
            self._subscriptions[topic].pop('handler', None)

    async def unsubscribe(self, topic: str, sub_id: str):
        """Unsubscribe from a topic."""
        if not self.ws or not self.connected:
            raise ConnectionError("Not connected")
            
        msg_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        message = {
            'id': msg_id,
            'type': 'unsubscribe',
            'payload': {
                'topic': topic,
                'target': sub_id,
                'id': str(uuid.uuid4())
            }
        }
        
        future = asyncio.Future()
        self._pending_messages[msg_id] = future
        
        try:
            async with self._lock:
                await self.ws.send(json.dumps(message))
            await asyncio.wait_for(future, 5.0)
            
            # Remove subscription handlers
            self._subscriptions.pop(topic, None)
            self._subscriptions.pop(f"{topic}-{sub_id}", None)
        except Exception as e:
            self._pending_messages.pop(msg_id, None)
            logging.error(f'Unsubscribe failed: {e}')
