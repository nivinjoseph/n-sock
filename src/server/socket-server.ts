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

        // this._socketServer.adapter(SocketIoRedis.createAdapter({
        //     pubClient: this._redisClient,
        //     subClient: this._redisClient
        // }));

        this._socketServer.adapter(createAdapter(this._redisClient, this._redisClient));

        this._initialize();
    }

    public dispose(): Promise<void>
    {
        if (!this._isDisposed)
        {
            this._isDisposed = true;

            this._disposePromise = new Promise((resolve, reject) =>
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
            });
        }

        return this._disposePromise!;
    }

    private _initialize(): void
    {
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
}
