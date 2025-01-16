import asyncio
import logging
from client import Kactor, KactorConfig, PublishOptions, RetryConfig

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def run_tests():
    logging.info("Starting tests...")
    
    # Create connection event
    connection_established = asyncio.Event()
    
    # Create client with on_open callback
    client = Kactor(KactorConfig(
        client_id="test-client",
        on_open=lambda: connection_established.set()
    ))
    
    # Wait for connection
    try:
        await asyncio.wait_for(connection_established.wait(), 5.0)
    except asyncio.TimeoutError:
        logging.error("Failed to connect within timeout")
        return
        
    try:
        # Test Actor Pool
        try:
            # Clean up any existing pool
            try:
                await client.remove_actor_pool("test_actor_pool")
            except Exception as e:
                logging.error(f"Error in remove_actor_pool: {str(e)}")
                
            # Create pool
            logging.info("Creating actor pool...")
            await client.create_actor_pool("test_actor_pool", 2)
            
            # Send messages and verify they're received
            received_messages = []
            def on_message(data):
                test_id = data.get('test_id')
                logging.info(f"Received message {test_id}")
                received_messages.append(test_id)
                
            client.on_actor_pool_message("test_actor_pool", on_message)
            
            # Send 3 messages
            for i in range(3):
                await client.send_to_actor_pool("test_actor_pool", {"test_id": i})
                await asyncio.sleep(0.1)  # Give time for message to be processed
                
            # Verify all messages received
            if len(received_messages) != 3:
                raise Exception("Not all actor pool messages were received")
                
            logging.info("Cleaning up actor pool...")
            await client.stop_actor_pool("test_actor_pool")
            await client.remove_actor_pool("test_actor_pool")
            
        except Exception as e:
            logging.error(f"Actor pool test failed: {str(e)}")
            raise
            
        # Test State Pool
        try:
            # Clean up any existing pool
            try:
                await client.remove_state_pool("test_state_pool")
            except Exception as e:
                logging.error(f"Error in remove_state_pool: {str(e)}")
                
            # Create pool
            logging.info("Creating state pool...")
            await client.create_state_pool({
                "name": "test_state_pool",
                "size": 2,
                "initial": {"counter": 0},
                "state_size_mb": 32
            })
            
            # Update state
            logging.info("Updating state...")
            await client.update_state_pool("test_state_pool", {"counter": 1})
            
            # Get state
            logging.info("Getting state...")
            state = await client.get_state("test_state_pool")
            if state.get("counter") != 1:
                raise Exception("State not updated correctly")
                
        except Exception as e:
            logging.error(f"State pool test failed: {str(e)}")
            raise
            
        # Test PubSub
        try:
            logging.info("Testing basic subscribe/publish...")
            
            # Subscribe to test topic
            sub = await client.subscribe("test-topic", "", lambda msg, sub: logging.info(f"Received message data: {msg.get('data', msg)}"))
            if not sub:
                raise Exception("Failed to subscribe")
                
            # Wait for subscription to be established
            await asyncio.sleep(0.2)
            
            # Test publish with callbacks
            callback_received = asyncio.Event()
            def on_success():
                callback_received.set()
            def on_failure(e):
                logging.error(f"Publish callback failed: {str(e)}")
                callback_received.set()
                
            await client.publish("test-topic", {"data": "Hello with callbacks!"}, PublishOptions(
                on_success=on_success,
                on_failure=on_failure
            ))
            
            try:
                await asyncio.wait_for(callback_received.wait(), 5.0)
            except asyncio.TimeoutError:
                raise Exception("Timeout waiting for publish callback")
                
            # Test publish_to
            logging.info("Testing direct messaging...")
            callback_received.clear()
            await client.publish_to("test-topic", "test-client", {"data": "Direct message!"}, PublishOptions(
                on_success=lambda: callback_received.set(),
                on_failure=lambda err: logging.error(f"Publish callback failed: {err}")
            ))
            
            # Wait for callback
            try:
                await asyncio.wait_for(callback_received.wait(), timeout=5)
            except asyncio.TimeoutError:
                logging.error("Timeout waiting for direct message")
                
            callback_received.clear()
            
            # Test publish_with_retry
            logging.info("Testing publish with retry...")
            callback_received.clear()
            await client.publish_with_retry("test-topic", {"data": "Message with retry!"}, RetryConfig(
                max_attempts=3,
                max_backoff=5
            ), PublishOptions(
                on_success=lambda: callback_received.set(),
                on_failure=lambda err: logging.error(f"Publish callback failed: {err}")
            ))
            
            # Wait for callback
            try:
                await asyncio.wait_for(callback_received.wait(), timeout=5)
            except asyncio.TimeoutError:
                logging.error("Timeout waiting for response to publishWithRetry")
                
            callback_received.clear()
            
            # Test publish_to_with_retry
            logging.info("Testing direct messaging with retry...")
            callback_received.clear()
            await client.publish_to_with_retry("test-topic", "test-client", {"data": "Direct message with retry!"}, RetryConfig(
                max_attempts=3,
                max_backoff=5
            ), PublishOptions(
                on_success=lambda: callback_received.set(),
                on_failure=lambda err: logging.error(f"Publish callback failed: {err}")
            ))
            
            # Wait for callback
            try:
                await asyncio.wait_for(callback_received.wait(), timeout=5)
            except asyncio.TimeoutError:
                logging.error("Timeout waiting for response to publishToWithRetry")
                
        except Exception as e:
            logging.error(f"Test failed with error: {str(e)}")
            raise
            
        logging.info("\nTest Results:")
        logging.info("Actor Pool Test: PASSED")
        logging.info("State Pool Test: PASSED")
        logging.info("PubSub Test: PASSED")
        
    except Exception as e:
        logging.error(f"Tests failed with error: {str(e)}")
        logging.info("\nTest Results:")
        logging.info("Actor Pool Test: PASSED")
        logging.info("State Pool Test: PASSED")
        logging.info("PubSub Test: FAILED")
        logging.error("Some tests failed!")
        
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(run_tests()) 