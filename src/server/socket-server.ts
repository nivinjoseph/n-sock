import { Server } from "node:http";
import { given } from "@nivinjoseph/n-defensive";
import { RedisClientType } from "redis";
import { Disposable } from "@nivinjoseph/n-util";
import { Socket, Server as SocketIoServer } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";


/**
 * This should only manage socket connections, should not emit (publish) or listen (subscribe)??
 */
export class SocketServer implements Disposable
{
    private readonly _socketServer: SocketIoServer;
    private readonly _redisClient: RedisClientType<any, any, any, any, any>;
    private _subClient: RedisClientType<any, any, any, any, any> | null = null;
    private _isInitialized = false;
    private _isDisposed = false;
    private _disposePromise: Promise<void> | null = null;


    public constructor(httpServer: Server, corsOrigin: string, redisClient: RedisClientType<any, any, any, any, any>)
    {
        given(httpServer, "httpServers").ensureHasValue().ensureIsObject().ensureIsInstanceOf(Server);
        given(corsOrigin, "corsOrigin").ensureHasValue().ensureIsString();
        given(redisClient, "redisClient").ensureHasValue().ensureIsObject();

        // this._socketServer = new SocketIo.Server(httpServer, {
        //     transports: ["websocket"],
        //     pingInterval: 10000,
        //     pingTimeout: 5000,
        //     serveClient: false
        // });

        this._socketServer = new SocketIoServer(httpServer, {
            transports: ["websocket"],
            serveClient: false,
            cors: {
                origin: corsOrigin,
                methods: ["GET", "POST"]
            }
        });

        this._redisClient = redisClient;
    }

    /**
     * Sets up the Redis adapter and starts listening for connections.
     * Must be called (and awaited) before the server can handle clients.
     *
     * A Redis client in subscriber mode cannot issue regular commands, so the
     * adapter requires a dedicated subscriber client distinct from the pub client.
     */
    public async initialize(): Promise<void>
    {
        if (this._isDisposed)
            throw new Error("Cannot initialize a disposed SocketServer.");

        if (this._isInitialized)
            return;

        this._isInitialized = true;

        this._subClient = this._redisClient.duplicate();
        await this._subClient.connect();

        // this._socketServer.adapter(SocketIoRedis.createAdapter({
        //     pubClient: this._redisClient,
        //     subClient: this._subClient
        // }));

        this._socketServer.adapter(createAdapter(this._redisClient, this._subClient));

        this._socketServer.on("connection", (socket: Socket) =>
        {
            if (this._isDisposed)
                return;

            console.log("Client connected", socket.id);

            socket.on("n-sock-join_channel", (data: { channel: string; }) =>
            {
                given(data, "data").ensureHasValue().ensureIsObject().ensureHasStructure({ channel: "string" });

                console.log(`Client ${socket.id} joining channel ${data.channel}`);

                const nsp = this._socketServer.of(`/${data.channel}`);

                socket.emit(`n-sock-joined_channel/${data.channel}`, { channel: nsp.name.substr(1) });

                console.log(`Client ${socket.id} joined channel ${nsp.name.substr(1)}`);
            });
        });
    }

    public dispose(): Promise<void>
    {
        if (!this._isDisposed)
        {
            this._isDisposed = true;

            this._disposePromise = new Promise<void>((resolve, reject) =>
            {
                this._socketServer.disconnectSockets(true);  // close existing sockets
                this._socketServer.removeAllListeners();
                // eslint-disable-next-line @typescript-eslint/no-floating-promises
                this._socketServer.close((err) =>
                {
                    if (err)
                    {
                        reject(err);
                        return;
                    }

                    resolve();
                });
            })
                // close the duplicated subscriber client we created (the pub client is owned by the caller)
                .then(async () =>
                {
                    if (this._subClient != null && this._subClient.isOpen)
                        await this._subClient.quit();
                });
        }

        return this._disposePromise!;
    }
}
