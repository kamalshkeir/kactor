import 'client.dart';

void main() async {
  // Create and configure the client
  final client = Client(
    address: 'localhost:9313',
    path: '/ws/kactor',
    id: 'dart-client',
    secure: false,
    autoReconnect: true,
    maxRetries: 5,
    backoffMin: 100, // 100ms
    backoffMax: 5000, // 5s
    onOpen: () {
      print('Connected to server! Setting up pools...');
      setupPools(client);
    },
  );

  try {
    // Keep the connection alive
    while (!client._closed) {
      await Future.delayed(Duration(seconds: 1));
    }
  } finally {
    // Close the connection
    await client.close();
  }
}

Future<void> setupPools(Client client) async {
  try {
    // Create an actor pool
    await client.createActorPool('myactors', size: 4);
    print("Created actor pool 'myactors'");

    // Set up actor pool message handler
    client.onActorPoolMessage('myactors', (data) {
      print('Got actor pool message: $data');
    });

    // Send message to actor pool
    await client.sendToActorPool('myactors', {
      'action': 'process',
      'data': 'test data'
    });
    print("Sent message to actor pool");

    // Create a state pool
    await client.createStatePool(StatePoolConfig(
      name: 'mystate',
      size: 2,
      initial: {'counter': 0},
      stateSizeMb: 32,
    ));
    print("Created state pool 'mystate'");

    // Set up state pool message handler
    client.onStatePoolMessage('mystate', (data) {
      print('Got state pool message: $data');
    });

    // Send message to state pool
    await client.sendToStatePool('mystate', {
      'action': 'increment',
      'value': 1
    });
    print("Sent message to state pool");

    // Get current state
    final state = await client.getState('mystate');
    print('Current state: $state');

    // Update state
    await client.updateStatePool('mystate', {
      'counter': 42,
      'users': ['user1', 'user2']
    });
    print('Updated state pool');

    // Subscribe to a topic with handler
    final subscription = await client.subscribe('mytopic', 'sub1', (message, sub) {
      print('Got message on ${sub.getTopic()}: $message');
    });
    print("Subscribed to 'mytopic'");

    // Basic publish with callbacks
    final success = await client.publish(
      'mytopic',
      {'message': 'Hello subscribers!'},
      PublishOptions(
        onSuccess: () => print('Publish succeeded!'),
        onFailure: (error) => print('Publish failed: $error'),
      ),
    );
    print("Published message to 'mytopic'. Success: $success");

    // Publish with retry
    final retryConfig = RetryConfig(maxAttempts: 3, maxBackoff: 5);
    final retrySuccess = await client.publishWithRetry(
      'mytopic',
      {'message': 'Hello with retry!'},
      retryConfig,
      PublishOptions(
        onSuccess: () => print('Publish with retry succeeded!'),
        onFailure: (error) => print('Publish with retry failed: $error'),
      ),
    );
    print("Published with retry. Success: $retrySuccess");

    // Direct message to another client
    try {
      await client.publishTo(
        'mytopic',
        'other-client',
        {'message': 'Hello specific client!'},
        PublishOptions(
          onSuccess: () => print('Direct message sent!'),
          onFailure: (error) => print('Direct message failed: $error'),
        ),
      );
      print("Sent direct message to 'other-client'");
    } catch (e) {
      print("Direct message failed (expected if client doesn't exist): $e");
    }

    // Direct message with retry
    try {
      await client.publishToWithRetry(
        'mytopic',
        'other-client',
        {'message': 'Hello specific client with retry!'},
        retryConfig,
        PublishOptions(
          onSuccess: () => print('Direct message with retry sent!'),
          onFailure: (error) => print('Direct message with retry failed: $error'),
        ),
      );
      print("Sent direct message with retry to 'other-client'");
    } catch (e) {
      print("Direct message with retry failed (expected if client doesn't exist): $e");
    }

    // Wait a bit to see messages
    await Future.delayed(Duration(seconds: 1));

    // Unsubscribe
    await subscription?.unsubscribe();
    print("Unsubscribed from 'mytopic'");

    // Clean up
    await client.stopActorPool('myactors');
    await client.removeActorPool('myactors');

    await client.stopStatePool('mystate');
    await client.removeStatePool('mystate');

  } catch (e) {
    print('Error in setup: $e');
    await client.close();
  }
} 