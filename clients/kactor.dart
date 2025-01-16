import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:web_socket_channel/status.dart' as status;

class StatePoolConfig {
  final String name;
  final int size;
  final Map<String, dynamic> initial;
  final int stateSizeMb;

  StatePoolConfig({
    required this.name,
    required this.size,
    required this.initial,
    required this.stateSizeMb,
  });

  Map<String, dynamic> toJson() => {
    'name': name,
    'size': size,
    'initial': initial,
    'state_size_mb': stateSizeMb,
  };
}

class RetryConfig {
  final int maxAttempts;
  final int maxBackoff;

  RetryConfig({
    this.maxAttempts = 3,
    this.maxBackoff = 5,
  });

  Map<String, dynamic> toJson() => {
    'max_attempts': maxAttempts,
    'max_backoff': maxBackoff,
  };
}

class PublishOptions {
  final void Function()? onSuccess;
  final void Function(dynamic)? onFailure;

  PublishOptions({
    this.onSuccess,
    this.onFailure,
  });
}

class Subscription {
  final String topic;
  final String subId;
  final Kactor client;

  Subscription(this.topic, this.subId, this.client);

  String getTopic() => topic;

  Future<void> unsubscribe() async {
    await client.unsubscribe(topic, subId);
  }
}

// Kactor client for interacting with kactor servers
class Kactor {
  final String address;
  final String path;
  final String clientId;
  final bool secure;
  final bool autoReconnect;
  final int maxRetries;
  final int backoffMin;
  final int backoffMax;
  final Function? onOpen;

  WebSocketChannel? _channel;
  bool _connected = false;
  bool _closed = false;
  
  final _random = Random();
  final _pendingMessages = <String, Completer<dynamic>>{};
  final _subscriptions = <String, Map<String, dynamic>>{};
  final _actorPoolHandlers = <String, Function(dynamic)>{};
  final _statePoolHandlers = <String, Function(dynamic)>{};

  Kactor({
    this.address = 'localhost:9313',
    this.path = '/ws/kactor',
    String? clientId,
    this.secure = false,
    this.autoReconnect = true,
    this.maxRetries = 5,
    this.backoffMin = 100,
    this.backoffMax = 5000,
    this.onOpen,
  }) : clientId = clientId ?? _generateDefaultClientId() {
    _connect().catchError((e) {
      print('Initial connection failed: $e');
    });
  }

  static String _generateDefaultClientId() {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    final random = Random();
    final randomStr = List.generate(6, (index) => chars[random.nextInt(chars.length)]).join();
    return 'client-${DateTime.now().millisecondsSinceEpoch}-$randomStr';
  }

  String _generateMessageId([String? topic, String? subId]) {
    return _generateRandomId();
  }

  String _generateRandomId() {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    return List.generate(16, (index) => chars[_random.nextInt(chars.length)]).join();
  }

  Future<void> _connect() async {
    if (_closed) return;

    final scheme = secure ? 'wss' : 'ws';
    final uri = Uri.parse('$scheme://$address$path');
    
    int retries = 0;
    while (!_connected && !_closed) {
      try {
        // Close any existing connection first
        if (_channel != null) {
          await _channel!.sink.close();
          _channel = null;
        }
        
        final ws = WebSocketChannel.connect(uri);
        _channel = ws;
        await ws.ready;
        
        _connected = true;
        _setupMessageHandler();
        
        if (onOpen != null) {
          onOpen!();
        }
        return;
      } catch (e) {
        print('Connection failed: $e');
        if (!autoReconnect || (maxRetries > 0 && retries >= maxRetries)) {
          _connected = false;
          _channel = null;
          if (!_closed) {
            throw Exception('Failed to connect after $retries retries: $e');
          }
          return;
        }
        
        final delay = min(backoffMin * pow(2, retries), backoffMax).toInt();
        final jitter = Random().nextInt(delay ~/ 2);
        await Future.delayed(Duration(milliseconds: delay + jitter));
        retries++;
      }
    }
  }

  void _setupMessageHandler() {
    _channel?.stream.listen(
      (message) => _handleMessage(message),
      onDone: () {
        print('WebSocket connection closed');
        _connected = false;
        _channel = null;
        if (autoReconnect && !_closed) {
          _connect().catchError((e) {
            print('Reconnection failed: $e');
          });
        }
      },
      onError: (error) {
        print('WebSocket error: $error');
        _connected = false;
        _channel = null;
        if (autoReconnect && !_closed) {
          _connect().catchError((e) {
            print('Reconnection failed: $e');
          });
        }
      },
      cancelOnError: false,
    );
  }

  void _handleMessage(dynamic rawMessage) {
    final data = json.decode(rawMessage) as Map<String, dynamic>;
    final msgType = data['type'] as String?;
    final msgId = data['id'] as String?;
    final topic = data['topic'] as String?;
    final target = data['target'] as String?;

    print('DEBUG: Received message type=$msgType id=$msgId topic=$topic target=$target data=$data');

    switch (msgType) {
      case 'message':
        final topic = data['topic'] as String? ?? '';
        final target = data['target'] as String? ?? '';
        final payload = data['payload'];
        
        // Try topic+target first, then fall back to topic+clientId
        var subInfo = _subscriptions['$topic-$target'] ?? _subscriptions['$topic-$clientId'];
        
        if (subInfo != null) {
          final handler = subInfo['handler'] as Function?;
          final subId = subInfo['subID'] as String?;
          if (handler != null) {
            try {
              // Pass the payload directly since it's already the message data
              handler(payload, {
                'topic': topic,
                'subID': subId ?? target,
              });
            } catch (e) {
              print('Handler error: $e');
            }
          }
        }
        break;

      case 'published':
        if (msgId != null && _pendingMessages.containsKey(msgId)) {
          final completer = _pendingMessages.remove(msgId);
          if (!completer!.isCompleted) {
            completer.complete(data);
          }
        }
        break;
        
      case 'subscribed':
        if (msgId != null && _pendingMessages.containsKey(msgId)) {
          final completer = _pendingMessages.remove(msgId);
          if (!completer!.isCompleted) {
            completer.complete(data);
          }
        }
        break;

      case 'unsubscribed':
        if (msgId != null && _pendingMessages.containsKey(msgId)) {
          final completer = _pendingMessages.remove(msgId);
          if (!completer!.isCompleted) {
            completer.complete(data);
          }
          
          // Clean up subscription state
          final topic = data['topic'] as String? ?? '';
          final target = data['target'] as String? ?? '';
          _subscriptions.remove(topic);
          _subscriptions.remove('$topic-$target');
        }
        break;

      case 'subscribers_status':
      case 'actor_pool_created':
      case 'actor_pool_message_sent':
      case 'actor_pool_stopped':
      case 'actor_pool_removed':
      case 'state_pool_created':
      case 'state_pool_message_sent':
      case 'state_pool_updated':
      case 'state_pool_stopped':
      case 'state_pool_removed':
        if (msgId != null && _pendingMessages.containsKey(msgId)) {
          final completer = _pendingMessages.remove(msgId);
          if (!completer!.isCompleted) {
            completer.complete(data);
          }
        }
        break;

      case 'state_pool_state':
        if (msgId != null && _pendingMessages.containsKey(msgId)) {
          final completer = _pendingMessages.remove(msgId);
          if (!completer!.isCompleted) {
            completer.complete(data['payload']?['state'] ?? {});
          }
        }
        break;

      case 'actor_pool_message':
        final pool = data['payload']?['pool'] as String?;
        if (pool != null && _actorPoolHandlers.containsKey(pool)) {
          _actorPoolHandlers[pool]?.call(data['payload']?['data']);
        }
        break;

      case 'state_pool_message':
        final pool = data['payload']?['pool'] as String?;
        if (pool != null && _statePoolHandlers.containsKey(pool)) {
          _statePoolHandlers[pool]?.call(data['payload']?['data']);
        }
        break;

      case 'error':
        if (msgId != null && _pendingMessages.containsKey(msgId)) {
          final completer = _pendingMessages.remove(msgId);
          if (!completer!.isCompleted) {
            final error = data['payload']?['error'] ?? 'Unknown server error';
            completer.completeError(Exception(error));
          }
        }
        break;
    }
  }

  Future<bool> hasSubscribers(String topic) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    final message = {
      'type': 'has_subscribers',
      'id': messageId,
      'topic': topic,
    };

    try {
      _channel?.sink.add(json.encode(message));
      final response = await completer.future.timeout(Duration(seconds: 5));
      return response['payload']?['has_subscribers'] ?? false;
    } catch (e) {
      print('Failed to check subscribers: $e');
      return false;
    }
  }

  Future<bool> publishToWithRetry(
    String topic,
    String target,
    Map<String, dynamic> payload,
    RetryConfig config, [
    PublishOptions? options,
  ]) async {
    if (!_connected || _channel == null) {
      if (options?.onFailure != null) {
        options!.onFailure!(Exception('Not connected'));
      }
      return false;
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    try {
      final message = {
        'type': 'publishToWithRetry',
        'id': messageId,
        'topic': topic,
        'target': target,
        'payload': {
          'data': payload,
          'retry_config': config.toJson(),
        },
      };

      _channel?.sink.add(json.encode(message));
      
      // Wait for published confirmation
      final response = await completer.future.timeout(Duration(seconds: 5));
      if (response['type'] == 'published') {
        options?.onSuccess?.call();
        return true;
      } else if (response['type'] == 'error') {
        final error = response['payload']?['error'] ?? 'Unknown error';
        options?.onFailure?.call(Exception(error));
        return false;
      }
      return false;
    } catch (e) {
      options?.onFailure?.call(e);
      return false;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<bool> publish(String topic, Map<String, dynamic> payload, [PublishOptions? options]) async {
    if (!_connected || _channel == null) {
      if (options?.onFailure != null) {
        options!.onFailure!(Exception('Not connected'));
      }
      return false;
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    try {
      final message = {
        'type': 'publish',
        'topic': topic,
        'id': messageId,
        'payload': payload,
      };

      _channel!.sink.add(json.encode(message));
      
      // Wait for published confirmation
      final response = await completer.future.timeout(Duration(seconds: 5));
      if (response['type'] == 'published') {
        options?.onSuccess?.call();
        return true;
      } else if (response['type'] == 'error') {
        final error = response['payload']?['error'] ?? 'Unknown error';
        options?.onFailure?.call(Exception(error));
        return false;
      }
      return false;
    } catch (e) {
      options?.onFailure?.call(e);
      return false;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<bool> publishWithRetry(
    String topic,
    Map<String, dynamic> payload,
    RetryConfig config, [
    PublishOptions? options,
  ]) async {
    if (!_connected || _channel == null) {
      if (options?.onFailure != null) {
        options!.onFailure!(Exception('Not connected'));
      }
      return false;
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    try {
      final message = {
        'type': 'publishWithRetry',
        'id': messageId,
        'topic': topic,
        'payload': {
          'data': payload,
          'retry_config': config.toJson(),
        },
      };

      _channel?.sink.add(json.encode(message));
      
      // Wait for published confirmation
      final response = await completer.future.timeout(Duration(seconds: 5));
      if (response['type'] == 'published') {
        options?.onSuccess?.call();
        return true;
      } else if (response['type'] == 'error') {
        final error = response['payload']?['error'] ?? 'Unknown error';
        options?.onFailure?.call(Exception(error));
        return false;
      }
      return false;
    } catch (e) {
      options?.onFailure?.call(e);
      return false;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<bool> publishTo(String topic, String target, Map<String, dynamic> payload, [PublishOptions? options]) async {
    if (!_connected || _channel == null) {
      if (options?.onFailure != null) {
        options!.onFailure!(Exception('Not connected'));
      }
      return false;
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    try {
      final message = {
        'type': 'publishTo',
        'id': messageId,
        'topic': topic,
        'target': target,
        'payload': payload,
      };

      _channel?.sink.add(json.encode(message));
      
      // Wait for published confirmation
      final response = await completer.future.timeout(Duration(seconds: 5));
      if (response['type'] == 'published') {
        options?.onSuccess?.call();
        return true;
      } else if (response['type'] == 'error') {
        final error = response['payload']?['error'] ?? 'Unknown error';
        options?.onFailure?.call(Exception(error));
        return false;
      }
      return false;
    } catch (e) {
      options?.onFailure?.call(e);
      return false;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<Subscription?> subscribe(String topic, String subId, void Function(Map<String, dynamic>, Map<String, dynamic>) handler) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    subId = subId.isEmpty ? clientId : subId;
    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    try {
      final msg = {
        'type': 'subscribe',
        'id': messageId,
        'topic': topic,
        'target': subId,
      };
      
      _channel?.sink.add(json.encode(msg));
      
      // Wait for subscribed confirmation
      final response = await completer.future.timeout(Duration(seconds: 5));
      if (response['type'] == 'subscribed') {
        // Store handlers for both topic and topic+target like Go
        _subscriptions[topic] = {'handler': handler, 'subID': subId};
        _subscriptions['$topic-$subId'] = {'handler': handler, 'subID': subId};
        
        return Subscription(topic, subId, this);
      }
      return null;
    } catch (e) {
      print('Subscription failed: $e');
      return null;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<void> unsubscribe(String topic, String subId) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    try {
      final msg = {
        'type': 'unsubscribe',
        'id': messageId,
        'topic': topic,
        'target': subId,
      };
      
      _channel?.sink.add(json.encode(msg));
      
      // Wait for unsubscribed confirmation
      await completer.future.timeout(Duration(seconds: 5));
      
      // Remove both topic and topic+target handlers like Go
      _subscriptions.remove(topic);
      _subscriptions.remove('$topic-$subId');
    } catch (e) {
      print('Failed to unsubscribe: $e');
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<void> close() async {
    _closed = true;
    _connected = false;

    if (_channel != null) {
      // Clear all pending operations
      for (final completer in _pendingMessages.values) {
        if (!completer.isCompleted) {
          completer.completeError('Connection closed');
        }
      }
      _pendingMessages.clear();
      _subscriptions.clear();
      _actorPoolHandlers.clear();
      _statePoolHandlers.clear();
      
      await _channel!.sink.close(status.goingAway);
      _channel = null;
    }
  }

  void onActorPoolMessage(String pool, Function(dynamic) handler) {
    _actorPoolHandlers[pool] = handler;
  }

  void onStatePoolMessage(String pool, Function(dynamic) handler) {
    _statePoolHandlers[pool] = handler;
  }

  Future<Map<String, dynamic>> createActorPool(String name, {int size = 1}) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    final msg = {
      'type': 'create_actor_pool',
      'id': messageId,
      'payload': {
        'config': {
          'name': name,
          'size': size,
        },
      },
    };

    try {
      _channel?.sink.add(json.encode(msg));
      final response = await completer.future.timeout(Duration(seconds: 5));
      return response;
    } catch (e) {
      print('Failed to create actor pool: $e');
      rethrow;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<Map<String, dynamic>> sendToActorPool(String pool, Map<String, dynamic> message) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    final msg = {
      'type': 'actor_pool_message',
      'id': messageId,
      'payload': {
        'pool': pool,
        'data': message,
      },
    };

    try {
      _channel?.sink.add(json.encode(msg));
      final response = await completer.future.timeout(Duration(seconds: 5));
      return response;
    } catch (e) {
      print('Failed to send message to actor pool: $e');
      rethrow;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<Map<String, dynamic>> stopActorPool(String pool) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    final msg = {
      'type': 'stop_actor_pool',
      'id': messageId,
      'payload': {
        'pool': pool,
      },
    };

    try {
      _channel?.sink.add(json.encode(msg));
      final response = await completer.future.timeout(Duration(seconds: 5));
      return response;
    } catch (e) {
      print('Failed to stop actor pool: $e');
      rethrow;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<Map<String, dynamic>> removeActorPool(String pool) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    final msg = {
      'type': 'remove_actor_pool',
      'id': messageId,
      'payload': {
        'pool': pool,
      },
    };

    try {
      _channel?.sink.add(json.encode(msg));
      final response = await completer.future.timeout(Duration(seconds: 5));
      return response;
    } catch (e) {
      print('Failed to remove actor pool: $e');
      rethrow;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<Map<String, dynamic>> createStatePool(StatePoolConfig config) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    final msg = {
      'type': 'create_state_pool',
      'id': messageId,
      'payload': {
        'config': config.toJson(),
      },
    };

    try {
      _channel?.sink.add(json.encode(msg));
      final response = await completer.future.timeout(Duration(seconds: 5));
      return response;
    } catch (e) {
      print('Failed to create state pool: $e');
      rethrow;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<Map<String, dynamic>> getState(String pool) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    final msg = {
      'type': 'state_pool_state',
      'id': messageId,
      'payload': {
        'pool': pool,
      },
    };

    try {
      _channel?.sink.add(json.encode(msg));
      final response = await completer.future.timeout(Duration(seconds: 5));
      return response;
    } catch (e) {
      print('Failed to get state: $e');
      rethrow;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<Map<String, dynamic>> updateStatePool(String pool, Map<String, dynamic> updates) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    final msg = {
      'type': 'update_state_pool',
      'id': messageId,
      'payload': {
        'pool': pool,
        'updates': updates,
      },
    };

    try {
      _channel?.sink.add(json.encode(msg));
      final response = await completer.future.timeout(Duration(seconds: 5));
      return response;
    } catch (e) {
      print('Failed to update state pool: $e');
      rethrow;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }

  Future<Map<String, dynamic>> removeStatePool(String pool) async {
    if (!_connected || _channel == null) {
      throw Exception('Not connected');
    }

    final messageId = _generateMessageId();
    final completer = Completer<dynamic>();
    _pendingMessages[messageId] = completer;

    final msg = {
      'type': 'remove_state_pool',
      'id': messageId,
      'payload': {
        'pool': pool,
      },
    };

    try {
      _channel?.sink.add(json.encode(msg));
      final response = await completer.future.timeout(Duration(seconds: 5));
      return response;
    } catch (e) {
      print('Failed to remove state pool: $e');
      rethrow;
    } finally {
      _pendingMessages.remove(messageId);
    }
  }
}
