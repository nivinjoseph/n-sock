import { Server } from "node:http";
import { RedisClientType } from "redis";
import { Disposable } from "@nivinjoseph/n-util";
/**
 * This should only manage socket connections, should not emit (publish) or listen (subscribe)??
 */
export declare class SocketServer implements Disposable {
    private readonly _socketServer;
    private readonly _redisClient;
    private _subClient;
    private _isInitialized;
    private _isDisposed;
    private _disposePromise;
    constructor(httpServer: Server, corsOrigin: string, redisClient: RedisClientType<any, any, any, any, any>);
    /**
     * Sets up the Redis adapter and starts listening for connections.
     * Must be called (and awaited) before the server can handle clients.
     *
     * A Redis client in subscriber mode cannot issue regular commands, so the
     * adapter requires a dedicated subscriber client distinct from the pub client.
     */
    initialize(): Promise<void>;
    dispose(): Promise<void>;
}
//# sourceMappingURL=socket-server.d.ts.map