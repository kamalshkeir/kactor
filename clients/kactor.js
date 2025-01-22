/**
 * Configuration for creating a state pool.
 */
class StatePoolConfig {
    /**
     * @param {Object} config - Configuration object
     * @param {string} config.name - Name of the pool
     * @param {number} config.size - Size of the pool
     * @param {Object} config.initial - Initial state
     * @param {number} config.state_size_mb - Maximum state size in MB
     */
    constructor(config) {
        this.name = config.name;
        this.size = config.size;
        this.initial = config.initial;
        this.state_size_mb = config.state_size_mb;
    }
}

/**
 * Options for publishing messages.
 */
class PublishOptions {
    /**
     * @param {Object} options - Options object
     * @param {Function} [options.onSuccess] - Called when publish succeeds
     * @param {Function} [options.onFailure] - Called with error when publish fails
     */
    constructor(options = {}) {
        this.onSuccess = options.onSuccess;
        this.onFailure = options.onFailure;
    }
}

/**
 * Kactor client for interacting with the kactor server.
 */
class Kactor {
    /**
     * @param {Object} config - Configuration object
     * @param {string} [config.address='localhost:9313'] - Server address
     * @param {string} [config.path='/ws/kactor'] - WebSocket path
     * @param {string} [config.clientId] - Unique client identifier
     * @param {boolean} [config.secure=false] - Whether to use wss://
     * @param {Function} [config.onOpen] - Callback when connection is established
     */
    constructor(config = {}) {
        this.address = config.address || 'localhost:9313';
        this.path = config.path || '/ws/kactor';
        this.clientId = config.clientId || this._generateDefaultClientId();
        this.secure = config.secure || false;
        
        this.ws = null;
        this.connected = false;
        this.closed = false;

        // Following Python client's pattern
        this._pending_messages = new Map();
        this._subscriptions = new Map();
        this._actor_pool_handlers = new Map();
        this._state_pool_handlers = new Map();
        
        // Connect automatically in constructor if onOpen provided
        if (config.onOpen) {
            this._on_open = config.onOpen;
            this.connect();
        }
    }

    /**
     * Generate a default client ID.
     * @private
     * @returns {string} Generated client ID
     */
    _generateDefaultClientId() {
        const timestamp = Date.now();
        const random = Math.random().toString(36).substring(2, 8);
        return `client-${timestamp}-${random}`;
    }

    /**
     * Connect to the WebSocket server.
     */
    connect() {
        if (this.closed) return;

        const scheme = this.secure ? 'wss' : 'ws';
        const uri = `${scheme}://${this.address}${this.path}`;

        try {
            this.ws = new WebSocket(uri);
            
            this.ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                this._handleMessage(data);
            };

            this.ws.onopen = () => {
                console.debug('WebSocket connected');
                this.connected = true;
                if (this._on_open) {
                    this._on_open();
                }
            };

            this.ws.onclose = () => {
                console.debug('WebSocket closed');
                this.connected = false;
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
                this.connected = false;
            };

        } catch (error) {
            console.error('Connection failed:', error);
            this.connected = false;
        }
    }

    /**
     * Handle incoming WebSocket messages.
     * @private
     * @param {Object} data - Message data
     */
    _handleMessage(data) {
        console.debug('Received message:', data);
        
        const msgType = data.type;
        const msgId = data.id;
        const payload = data.payload || {};

        switch (msgType) {
            case 'actor_pool_message':
                const pool = payload.pool;
                if (pool && this._actor_pool_handlers.has(pool)) {
                    const handler = this._actor_pool_handlers.get(pool);
                    handler(payload.data || {});
                }
                break;

            case 'state_pool_message':
                const statePool = payload.pool;
                if (statePool && this._state_pool_handlers.has(statePool)) {
                    const handler = this._state_pool_handlers.get(statePool);
                    handler(payload.data);
                }
                break;

            case 'message':
                const topic = payload.topic || data.topic;
                if (topic && this._subscriptions.has(topic)) {
                    const subscription = this._subscriptions.get(topic);
                    const handler = subscription.handler;
                    if (handler) {
                        handler(payload.payload || payload, { topic: topic, id: msgId });
                    }
                }
                break;

            case 'published':
            case 'subscribed':
            case 'unsubscribed':
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
            case 'state_pool_state':
                if (msgId && this._pending_messages.has(msgId)) {
                    const resolver = this._pending_messages.get(msgId);
                    resolver.resolve(data);
                    this._pending_messages.delete(msgId);
                }
                break;

            case 'error':
                if (msgId && this._pending_messages.has(msgId)) {
                    const resolver = this._pending_messages.get(msgId);
                    const error = payload.error || 'Unknown server error';
                    resolver.reject(new Error(error));
                    this._pending_messages.delete(msgId);
                }
                break;
        }
    }

    /**
     * Close the WebSocket connection.
     */
    close() {
        this.closed = true;
        if (this.ws) {
            this.ws.close();
        }
    }

    /**
     * Create an actor pool.
     * @param {string} name - Pool name
     * @param {number} size - Pool size
     * @returns {Promise<Object>} Response data
     */
    async createActorPool(name, size = 1) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'create_actor_pool',
            id: messageId,
            payload: {
                config: {
                    name: name,
                    size: size
                }
            }
        };

        await this.ws.send(JSON.stringify(msg));
        const response = await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });

        if (response.type !== 'actor_pool_created') {
            throw new Error(response.payload?.error || 'Failed to create actor pool');
        }
        return response;
    }

    /**
     * Send a message to an actor pool.
     * @param {string} pool - Pool name
     * @param {Object} message - Message data
     * @returns {Promise<Object>} Response data
     */
    async sendToActorPool(pool, message) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'actor_pool_message',
            id: messageId,
            payload: {
                pool: pool,
                data: message
            }
        };

        await this.ws.send(JSON.stringify(msg));
        return await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });
    }

    /**
     * Set message handler for an actor pool.
     * @param {string} pool - Pool name
     * @param {Function} handler - Message handler
     */
    onActorPoolMessage(pool, handler) {
        this._actor_pool_handlers.set(pool, handler);
    }

    /**
     * Stop an actor pool.
     * @param {string} pool - Pool name
     * @returns {Promise<Object>} Response data
     */
    async stopActorPool(pool) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'stop_actor_pool',
            id: messageId,
            payload: {
                pool: pool
            }
        };

        await this.ws.send(JSON.stringify(msg));
        return await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });
    }

    /**
     * Remove an actor pool.
     * @param {string} pool - Pool name
     * @returns {Promise<Object>} Response data
     */
    async removeActorPool(pool) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'remove_actor_pool',
            id: messageId,
            payload: {
                pool: pool
            }
        };

        await this.ws.send(JSON.stringify(msg));
        return await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });
    }

    /**
     * Create a state pool.
     * @param {StatePoolConfig} config - State pool configuration
     * @returns {Promise<Object>} Response data
     */
    async createStatePool(config) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'create_state_pool',
            id: messageId,
            payload: {
                config: {
                    name: config.name,
                    size: config.size,
                    initial: config.initial,
                    state_size_mb: config.state_size_mb
                }
            }
        };

        await this.ws.send(JSON.stringify(msg));
        const response = await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });

        if (response.type !== 'state_pool_created') {
            throw new Error(response.payload?.error || 'Failed to create state pool');
        }
        return response;
    }

    /**
     * Send a message to a state pool.
     * @param {string} pool - Pool name
     * @param {Object} message - Message data
     * @returns {Promise<Object>} Response data
     */
    async sendToStatePool(pool, message) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'state_pool_message',
            id: messageId,
            payload: {
                pool: pool,
                data: message
            }
        };

        await this.ws.send(JSON.stringify(msg));
        return await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });
    }

    /**
     * Set message handler for a state pool.
     * @param {string} pool - Pool name
     * @param {Function} handler - Message handler
     */
    onStatePoolMessage(pool, handler) {
        this._state_pool_handlers.set(pool, handler);
    }

    /**
     * Stop a state pool.
     * @param {string} pool - Pool name
     * @returns {Promise<Object>} Response data
     */
    async stopStatePool(pool) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'stop_state_pool',
            id: messageId,
            payload: {
                pool: pool
            }
        };

        await this.ws.send(JSON.stringify(msg));
        return await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });
    }

    /**
     * Remove a state pool.
     * @param {string} pool - Pool name
     * @returns {Promise<Object>} Response data
     */
    async removeStatePool(pool) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'remove_state_pool',
            id: messageId,
            payload: {
                pool: pool
            }
        };

        await this.ws.send(JSON.stringify(msg));
        return await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });
    }

    /**
     * Get state from a state pool.
     * @param {string} pool - Pool name
     * @returns {Promise<Object>} State data
     */
    async getState(pool) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'state_pool_state',
            id: messageId,
            payload: {
                pool: pool
            }
        };

        await this.ws.send(JSON.stringify(msg));
        const response = await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });

        if (response.type !== 'state_pool_state') {
            throw new Error(response.payload?.error || 'Failed to get state');
        }
        return response.payload?.state || {};
    }

    /**
     * Update state pool.
     * @param {string} pool - Pool name
     * @param {Object} state - New state
     * @returns {Promise<Object>} Response data
     */
    async updateStatePool(pool, state) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'update_state_pool',
            id: messageId,
            payload: {
                pool: pool,
                updates: state
            }
        };

        await this.ws.send(JSON.stringify(msg));
        return await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });
    }

    /**
     * Create a Subscription object.
     * @private
     */
    _createSubscription(topic, subId) {
        return {
            topic: topic,
            subId: subId,
            client: this,
            getTopic() {
                return this.topic;
            },
            async unsubscribe() {
                if (this.client) {
                    await this.client.unsubscribe(this.topic, this.subId);
                }
            }
        };
    }

    /**
     * Subscribe to a topic.
     * @param {string} topic - Topic to subscribe to
     * @param {string} subId - Subscription ID (optional, uses client_id if not provided)
     * @param {Function} handler - Message handler that receives (message, subscription)
     * @returns {Promise<Object>} Subscription object with getTopic() and unsubscribe() methods
     */
    async subscribe(topic, subId, handler) {
        try {
            // Use clientID as default subID if empty
            subId = subId || this.clientId;

            const messageId = Math.random().toString(36).substring(2, 15);
            const msg = {
                type: 'subscribe',
                topic: topic,
                id: messageId,
                target: subId
            };

            await this.ws.send(JSON.stringify(msg));
            const response = await new Promise((resolve, reject) => {
                const handler = (event) => {
                    const data = JSON.parse(event.data);
                    if (data.id === messageId) {
                        this.ws.removeEventListener('message', handler);
                        resolve(data);
                    }
                };
                this.ws.addEventListener('message', handler);
                setTimeout(() => {
                    this.ws.removeEventListener('message', handler);
                    reject(new Error('Timeout waiting for response'));
                }, 5000);
            });

            if (response.type !== 'subscribed') {
                throw new Error(response.payload?.error || 'Subscription failed');
            }

            // Create subscription object first
            const subscription = this._createSubscription(topic, subId);

            // Store subscription info for both topic and topic+subID
            const subInfo = { 
                id: subId, 
                handler: (msg) => handler(msg, subscription)  // Wrap handler to pass subscription
            };
            this._subscriptions.set(topic, subInfo);
            this._subscriptions.set(`${topic}-${subId}`, subInfo);

            return subscription;
        } catch (e) {
            console.error('Subscription failed:', e);
            return null;
        }
    }

    /**
     * Set message handler for a topic.
     * @param {string} topic - Topic to set handler for
     * @param {Function} handler - Message handler
     */
    onTopicMessage(topic, handler) {
        if (!this._subscriptions.has(topic)) {
            console.warn('Setting handler for unsubscribed topic:', topic);
            return;
        }
        const subscription = this._subscriptions.get(topic);
        subscription.handler = handler;
        this._subscriptions.set(topic, subscription);
    }

    /**
     * Publish a message to a topic.
     * @param {string} topic - Topic to publish to
     * @param {Object} message - Message data
     * @param {PublishOptions} options - Publish options
     * @returns {Promise<boolean>} True if published successfully
     */
    async publish(topic, message, options = null) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'publish',
            topic: topic,
            id: messageId,
            payload: message
        };

        if (options === null) {
            msg.payload.no_ack = true;
            try {
                await this.ws.send(JSON.stringify(msg));
                return true;
            } catch (e) {
                return false;
            }
        }

        try {
            // Create response promise before sending
            const responsePromise = new Promise((resolve, reject) => {
                const handler = {resolve, reject};
                this._pending_messages.set(messageId, handler);
                setTimeout(() => {
                    this._pending_messages.delete(messageId);
                    reject(new Error('publish timeout'));
                }, 5000);
            });

            await this.ws.send(JSON.stringify(msg));
            
            try {
                const response = await responsePromise;
                if (response.type === 'published') {
                    if (options.onSuccess) {
                        options.onSuccess();
                    }
                    return true;
                } else if (response.type === 'error') {
                    const error = response.payload?.error || 'publish failed';
                    if (options.onFailure) {
                        options.onFailure(new Error(error));
                    }
                    return false;
                }
            } catch (e) {
                if (options.onFailure) {
                    options.onFailure(e);
                }
                return false;
            }
            return false;
        } catch (e) {
            if (options.onFailure) {
                options.onFailure(e);
            }
            return false;
        } finally {
            this._pending_messages.delete(messageId);
        }
    }

    /**
     * Publish a message with retry configuration.
     * @param {string} topic - Topic to publish to
     * @param {Object} payload - Message data
     * @param {RetryConfig} retryConfig - Retry configuration
     * @param {PublishOptions} options - Publish options
     * @returns {Promise<boolean>} True if published successfully
     */
    async publishWithRetry(topic, payload, retryConfig = null, options = null) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'publishWithRetry',
            topic: topic,
            id: messageId,
            payload: {
                data: payload,
                retry_config: {
                    max_attempts: retryConfig?.maxAttempts || 3,
                    max_backoff: retryConfig?.maxBackoff || 5
                }
            }
        };

        if (options === null) {
            msg.payload.no_ack = true;
            try {
                await this.ws.send(JSON.stringify(msg));
                return true;
            } catch (e) {
                return false;
            }
        }

        try {
            // Create response promise before sending
            const responsePromise = new Promise((resolve, reject) => {
                const handler = {resolve, reject};
                this._pending_messages.set(messageId, handler);
                setTimeout(() => {
                    this._pending_messages.delete(messageId);
                    reject(new Error('publish timeout'));
                }, 5000);
            });

            await this.ws.send(JSON.stringify(msg));
            
            try {
                const response = await responsePromise;
                if (response.type === 'published') {
                    if (options.onSuccess) {
                        options.onSuccess();
                    }
                    return true;
                } else if (response.type === 'error') {
                    const error = response.payload?.error || 'publish failed';
                    if (options.onFailure) {
                        options.onFailure(new Error(error));
                    }
                    return false;
                }
            } catch (e) {
                if (options.onFailure) {
                    options.onFailure(e);
                }
                return false;
            }
            return false;
        } catch (e) {
            if (options.onFailure) {
                options.onFailure(e);
            }
            return false;
        } finally {
            this._pending_messages.delete(messageId);
        }
    }

    /**
     * Publish a message to a specific client.
     * @param {string} topic - Topic to publish to
     * @param {string} targetId - Target client ID
     * @param {Object} payload - Message data
     * @param {PublishOptions} options - Publish options
     * @returns {Promise<boolean>} True if published successfully
     */
    async publishTo(topic, targetId, payload, options = null) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'publishTo',
            topic: topic,
            id: messageId,
            target: targetId,
            payload: payload
        };

        if (options === null) {
            msg.payload.no_ack = true;
            try {
                await this.ws.send(JSON.stringify(msg));
                return true;
            } catch (e) {
                return false;
            }
        }

        try {
            // Create response promise before sending
            const responsePromise = new Promise((resolve, reject) => {
                const handler = {resolve, reject};
                this._pending_messages.set(messageId, handler);
                setTimeout(() => {
                    this._pending_messages.delete(messageId);
                    reject(new Error('publish timeout'));
                }, 5000);
            });

            await this.ws.send(JSON.stringify(msg));
            
            try {
                const response = await responsePromise;
                if (response.type === 'published') {
                    if (options.onSuccess) {
                        options.onSuccess();
                    }
                    return true;
                } else if (response.type === 'error') {
                    const error = response.payload?.error || 'publish failed';
                    if (options.onFailure) {
                        options.onFailure(new Error(error));
                    }
                    return false;
                }
            } catch (e) {
                if (options.onFailure) {
                    options.onFailure(e);
                }
                return false;
            }
            return false;
        } catch (e) {
            if (options.onFailure) {
                options.onFailure(e);
            }
            return false;
        } finally {
            this._pending_messages.delete(messageId);
        }
    }

    /**
     * Publish a message to a specific client with retry configuration.
     * @param {string} topic - Topic to publish to
     * @param {string} targetId - Target client ID
     * @param {Object} payload - Message data
     * @param {RetryConfig} retryConfig - Retry configuration
     * @param {PublishOptions} options - Publish options
     * @returns {Promise<boolean>} True if published successfully
     */
    async publishToWithRetry(topic, targetId, payload, retryConfig = null, options = null) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'publishToWithRetry',
            topic: topic,
            id: messageId,
            target: targetId,
            payload: {
                data: payload,
                retry_config: {
                    max_attempts: retryConfig?.maxAttempts || 3,
                    max_backoff: retryConfig?.maxBackoff || 5
                }
            }
        };

        if (options === null) {
            msg.payload.no_ack = true;
            try {
                await this.ws.send(JSON.stringify(msg));
                return true;
            } catch (e) {
                return false;
            }
        }

        try {
            // Create response promise before sending
            const responsePromise = new Promise((resolve, reject) => {
                const handler = {resolve, reject};
                this._pending_messages.set(messageId, handler);
                setTimeout(() => {
                    this._pending_messages.delete(messageId);
                    reject(new Error('publish timeout'));
                }, 5000);
            });

            await this.ws.send(JSON.stringify(msg));
            
            try {
                const response = await responsePromise;
                if (response.type === 'published') {
                    if (options.onSuccess) {
                        options.onSuccess();
                    }
                    return true;
                } else if (response.type === 'error') {
                    const error = response.payload?.error || 'publish failed';
                    if (options.onFailure) {
                        options.onFailure(new Error(error));
                    }
                    return false;
                }
            } catch (e) {
                if (options.onFailure) {
                    options.onFailure(e);
                }
                return false;
            }
            return false;
        } catch (e) {
            if (options.onFailure) {
                options.onFailure(e);
            }
            return false;
        } finally {
            this._pending_messages.delete(messageId);
        }
    }

    /**
     * Check if a topic has subscribers.
     * @param {string} topic - Topic to check
     * @returns {Promise<boolean>} True if the topic has subscribers
     */
    async hasSubscribers(topic) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'has_subscribers',
            topic: topic,
            id: messageId
        };

        try {
            await this.ws.send(JSON.stringify(msg));
            const response = await new Promise((resolve, reject) => {
                const handler = (event) => {
                    const data = JSON.parse(event.data);
                    if (data.id === messageId) {
                        this.ws.removeEventListener('message', handler);
                        resolve(data);
                    }
                };
                this.ws.addEventListener('message', handler);
                setTimeout(() => {
                    this.ws.removeEventListener('message', handler);
                    reject(new Error('Timeout waiting for response'));
                }, 5000);
            });
            return response?.payload?.has_subscribers || false;
        } catch (e) {
            console.error('Error checking subscribers:', e);
            return false;
        }
    }

    /**
     * Remove the message handler for an actor pool.
     * @param {string} pool - Pool name to remove handler for
     */
    removeActorPoolHandler(pool) {
        this._actor_pool_handlers.delete(pool);
    }

    /**
     * Remove the message handler for a state pool.
     * @param {string} pool - Pool name to remove handler for
     */
    removeStatePoolHandler(pool) {
        this._state_pool_handlers.delete(pool);
    }

    /**
     * Remove the message handler for a topic.
     * @param {string} topic - Topic to remove handler for
     */
    removeTopicHandler(topic) {
        if (this._subscriptions.has(topic)) {
            const subscription = this._subscriptions.get(topic);
            delete subscription.handler;
            this._subscriptions.set(topic, subscription);
        }
    }

    /**
     * Unsubscribe from a topic.
     * @param {string} topic - Topic to unsubscribe from
     * @param {string} subId - Subscription ID
     * @returns {Promise<Object>} Response data
     */
    async unsubscribe(topic, subId) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const msg = {
            type: 'unsubscribe',
            id: messageId,
            payload: {
                topic: topic,
                target: subId,
                id: messageId
            }
        };

        await this.ws.send(JSON.stringify(msg));
        const response = await new Promise((resolve, reject) => {
            const handler = (event) => {
                const data = JSON.parse(event.data);
                if (data.id === messageId) {
                    this.ws.removeEventListener('message', handler);
                    resolve(data);
                }
            };
            this.ws.addEventListener('message', handler);
            setTimeout(() => {
                this.ws.removeEventListener('message', handler);
                reject(new Error('Timeout waiting for response'));
            }, 5000);
        });

        // Remove subscription handlers
        this._subscriptions.delete(topic);
        this._subscriptions.delete(`${topic}-${subId}`);

        return response;
    }

    /**
     * Publish a message to another server.
     * @param {string} serverAddr - Target server address
     * @param {Object} message - Message data
     * @param {PublishOptions} options - Publish options
     * @param {string} [path] - Optional path
     * @returns {Promise<boolean>} True if published successfully
     */
    async publishToServer(serverAddr, message, options = null, path = null) {
        const messageId = Math.random().toString(36).substring(2, 15);
        const payload = {
            server_addr: serverAddr,
            data: message
        };
        if (path) {
            payload.path = path;
        }

        const msg = {
            type: 'publishToServer',
            id: messageId,
            payload: payload
        };

        try {
            await this.ws.send(JSON.stringify(msg));
            const response = await new Promise((resolve, reject) => {
                const handler = (event) => {
                    const data = JSON.parse(event.data);
                    if (data.id === messageId) {
                        this.ws.removeEventListener('message', handler);
                        resolve(data);
                    }
                };
                this.ws.addEventListener('message', handler);
                setTimeout(() => {
                    this.ws.removeEventListener('message', handler);
                    reject(new Error('Timeout waiting for response'));
                }, 5000);
            });

            if (response.type === 'published') {
                if (options?.onSuccess) {
                    options.onSuccess();
                }
                return true;
            }
            if (options?.onFailure) {
                options.onFailure(new Error('Failed to publish to server'));
            }
            return false;
        } catch (e) {
            if (options?.onFailure) {
                options.onFailure(e);
            }
            return false;
        }
    }
}
