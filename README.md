# n-sock

A Socket.IO based WebSocket implementation compatible with event-driven distributed systems. This library provides a robust and type-safe WebSocket client and server implementation using Socket.IO, specifically designed for TypeScript/JavaScript applications.

## Features

- Type-safe WebSocket client and server implementation
- Channel-based communication
- Event-driven architecture
- Redis adapter support for scalability
- Automatic reconnection handling
- Clean and simple API
- Built with TypeScript for better developer experience

## Installation

```bash
# Using npm
npm install @nivinjoseph/n-sock

# Using yarn
yarn add @nivinjoseph/n-sock
```

## Usage

### Client-side Usage

```typescript
import { SocketClient } from "@nivinjoseph/n-sock/client";

// Create a new socket client
const client = new SocketClient("http://your-server-url");

// Subscribe to a channel and event
const subscription = await client.subscribe("channelName", "eventName");

// Handle incoming data
subscription.onData((data) => {
    console.log("Received data:", data);
});

// Handle connection changes
subscription.onConnectionChange(() => {
    console.log("Connection status changed");
});

// Unsubscribe when done
subscription.unsubscribe();

// Dispose the client when no longer needed
await client.dispose();
```

### Server-side Usage

```typescript
import { SocketServer, SocketService } from "@nivinjoseph/n-sock/server";
import { createClient } from "redis";

// Create a socket server
const server = new SocketServer();

// Configure Redis adapter (optional)
const redisClient = createClient({
    url: "redis://localhost:6379"
});
await redisClient.connect();

server.useRedis({
    host: "localhost",
    port: 6379
});

// Create a socket service for publishing events
const socketService = new SocketService(redisClient);

// Publish events to channels
socketService.publish("channelName", "eventName", { data: "your data" });

// Clean up when done
await server.dispose();
await socketService.dispose();
```

## API Reference

### SocketClient

The main client-side class for WebSocket connections.

#### Constructor
- `constructor(serverUrl: string)`: Creates a new socket client instance with the specified server URL.

#### Methods
- `subscribe(channel: string, event: string): Promise<SocketChannelSubscription>`: Subscribes to a specific channel and event. Returns a subscription that can be used to handle events.
- `dispose(): Promise<void>`: Cleans up resources and closes all connections.

### SocketChannelSubscription

Interface for managing channel subscriptions.

#### Properties
- `eventName: string`: The name of the subscribed event.

#### Methods
- `onData(callback: (data: any) => void): this`: Registers a callback for incoming data. Returns the subscription for method chaining.
- `onConnectionChange(callback: () => void): this`: Registers a callback for connection status changes. Returns the subscription for method chaining.
- `unsubscribe(): void`: Unsubscribes from the channel and cleans up resources.

### SocketServer

The main server-side class for WebSocket connections.

#### Constructor
- `constructor(httpServer: Server, corsOrigin: string, redisClient: RedisClientType)`: Creates a new socket server instance with the specified HTTP server, CORS origin, and Redis client.

#### Methods
- `initialize(): Promise<void>`: Sets up the Redis adapter (using a dedicated subscriber client duplicated from the provided client) and starts listening for connections. Must be called and awaited before the server can handle clients.
- `dispose(): Promise<void>`: Cleans up resources, stops the server, and closes the duplicated subscriber client.

### SocketService

The service class for publishing events to WebSocket channels.

#### Constructor
- `constructor(redisClient: RedisClientType)`: Creates a new socket service instance using a Redis client.

#### Methods
- `publish(channel: string, event: string, data: object): void`: Publishes data to a specific channel and event.
- `dispose(): Promise<void>`: Cleans up resources.

## Dependencies

- Socket.IO: ^4.7.3
- Socket.IO Client: ^4.7.3
- Redis: ^4.6.12
- @socket.io/redis-adapter: ^8.2.1
- @socket.io/redis-emitter: ^5.1.0

## Contributing

Please feel free to submit issues, fork the repository and create pull requests for any improvements.

## License

This project is licensed under the ISC License - see the [LICENSE](LICENSE) file for details.

