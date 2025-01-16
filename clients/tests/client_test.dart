import 'package:tests/test.dart';
import '../kactor.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:web_socket_channel/status.dart' as status;
import 'dart:async';

void main() {
  late Kactor client;
  late Completer<void> connectionEstablished;

  setUp(() async {
    connectionEstablished = Completer<void>();
    client = Kactor(
      address: "localhost:9313",
      clientId: "test-client",
      onOpen: () => connectionEstablished.complete(),
    );

    // Wait for connection before each test
    try {
      await connectionEstablished.future.timeout(Duration(seconds: 5));
      // Add extra delay to ensure connection is fully ready
      await Future.delayed(Duration(milliseconds: 100));
      
      // Clean up any existing subscriptions
      try {
        final sub = await client.subscribe("test-topic", "test-client", (_, __) {});
        if (sub != null) {
          await sub.unsubscribe();
          await Future.delayed(Duration(seconds: 2));
        }
      } catch (e) {
        print('Error cleaning up subscriptions in setUp: $e');
      }
    } catch (e) {
      await client.close();
      fail('Failed to connect within timeout: $e');
    }
  });

  tearDown(() async {
    try {
      // Clean up subscriptions first
      try {
        final sub = await client.subscribe("test-topic", "test-client", (_, __) {});
        if (sub != null) {
          await sub.unsubscribe();
          await Future.delayed(Duration(seconds: 2));
        }
      } catch (e) {
        print('Error cleaning up subscriptions in tearDown: $e');
      }
      
      await client.close();
      // Give more time for cleanup and ensure connection is fully closed
      await Future.delayed(Duration(seconds: 1));
    } catch (e) {
      print('Error during teardown: $e');
    }
  });

  group('Actor Pool Tests', () {
    test('should handle actor pool operations', () async {
      // Clean up any existing pool
      try {
        await client.removeActorPool("test_actor_pool");
        // Wait for cleanup to complete
        await Future.delayed(Duration(milliseconds: 500));
      } catch (e) {
        print('Error in remove_actor_pool: $e');
      }

      // Create pool
      print('Creating actor pool...');
      await client.createActorPool("test_actor_pool", size: 2);
      // Wait for pool to be ready
      await Future.delayed(Duration(milliseconds: 500));

      // Send messages and verify they're received
      final receivedMessages = <int>[];
      final messageCompleter = Completer<void>();
      void onMessage(dynamic data) {
        final testId = data['test_id'] as int;
        print('Received message $testId');
        receivedMessages.add(testId);
        if (receivedMessages.length == 3) {
          messageCompleter.complete();
        }
      }

      client.onActorPoolMessage("test_actor_pool", onMessage);

      // Send 3 messages
      for (var i = 0; i < 3; i++) {
        await client.sendToActorPool("test_actor_pool", {"test_id": i});
        await Future.delayed(Duration(milliseconds: 100)); // Give time for message processing
      }

      // Wait for all messages with timeout
      try {
        await messageCompleter.future.timeout(Duration(seconds: 5));
      } catch (e) {
        fail('Timeout waiting for actor pool messages: $e');
      }

      // Verify all messages received
      expect(receivedMessages.length, equals(3), reason: 'Not all actor pool messages were received');

      print('Cleaning up actor pool...');
      await client.stopActorPool("test_actor_pool");
      await Future.delayed(Duration(milliseconds: 500));
      await client.removeActorPool("test_actor_pool");
      await Future.delayed(Duration(milliseconds: 500));
    });
  });

  group('State Pool Tests', () {
    test('should handle state pool operations', () async {
      // Clean up any existing pool
      try {
        await client.removeStatePool("test_state_pool");
        // Wait for cleanup to complete
        await Future.delayed(Duration(milliseconds: 500));
      } catch (e) {
        print('Error in remove_state_pool: $e');
      }

      // Create pool
      print('Creating state pool...');
      await client.createStatePool(StatePoolConfig(
        name: "test_state_pool",
        size: 2,
        initial: {"counter": 0},
        stateSizeMb: 32,
      ));
      // Wait for pool to be ready
      await Future.delayed(Duration(milliseconds: 500));

      // Update state
      print('Updating state...');
      await client.updateStatePool("test_state_pool", {"counter": 1});
      await Future.delayed(Duration(milliseconds: 200));

      // Get state
      print('Getting state...');
      final state = await client.getState("test_state_pool");
      expect(state["counter"], equals(1), reason: 'State not updated correctly');

      // Cleanup
      await client.removeStatePool("test_state_pool");
      await Future.delayed(Duration(milliseconds: 500));
    });
  });

  group('PubSub Tests', () {
    test('should handle basic subscribe/publish operations', () async {
      // Test basic subscribe/publish
      print('Testing basic subscribe/publish...');
      
      final messageReceived = Completer<void>();
      final subId = "test-client-${DateTime.now().millisecondsSinceEpoch}";
      print('Using subscription ID: $subId');
      
      // Wait for connection to be ready
      await Future.delayed(Duration(milliseconds: 500));

      // Subscribe with message handler
      print('Subscribing to test-topic...');
      final subscription = await client.subscribe(
        "test-topic",
        subId,
        (msg, sub) {
          print('Received message in handler: $msg');
          if (msg is Map) {
            print('Message data: $msg');
            if (!messageReceived.isCompleted) {
              print('Completing message received future');
              messageReceived.complete();
            }
          }
        }
      );
      expect(subscription, isNotNull);
      print('Subscription created successfully');
      
      // Wait for subscription to be established
      await Future.delayed(Duration(milliseconds: 500));

      // Test publish with callbacks
      print('Publishing message...');
      bool publishSuccess = false;
      
      try {
        await client.publish(
          "test-topic",
          {"data": "Hello with callbacks!"},
          PublishOptions(
            onSuccess: () {
              print('Publish success callback triggered');
              publishSuccess = true;
            },
            onFailure: (e) {
              print('Publish failed: $e');
              fail('Publish failed: $e');
            },
          ),
        );

        print('Waiting for message to be received...');
        // Wait for message with timeout
        await messageReceived.future.timeout(Duration(seconds: 5));
        print('Message received successfully');
      } catch (e) {
        print('Error during publish/receive: $e');
        fail('Error during publish/receive: $e');
      }

      expect(publishSuccess, isTrue, reason: 'Publish success callback not triggered');

      // Test has_subscribers
      print('Testing has_subscribers...');
      final hasSubscribers = await client.hasSubscribers("test-topic");
      expect(hasSubscribers, isTrue, reason: 'Expected to have subscribers');

      // Cleanup
      print('Cleaning up subscription...');
      if (subscription != null) {
        print('Unsubscribing from test-topic...');
        await subscription.unsubscribe();
        print('Unsubscribed successfully');
        await Future.delayed(Duration(milliseconds: 200));
      }
    });
  });
}
