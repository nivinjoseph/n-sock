import { given } from "@nivinjoseph/n-defensive";
import { Mutex } from "@nivinjoseph/n-util";
import { ObjectDisposedException } from "@nivinjoseph/n-exception";
import { io as SocketIo } from "socket.io-client";
/**
 * This should only listen (subscribe) to events, should not emit (publish)
 */
export class SocketClient {
    _serverUrl;
    _master;
    _channels = new Array();
    _mutex = new Mutex();
    _isDisposed = false;
    _disposePromise = null;
    constructor(serverUrl) {
        given(serverUrl, "serverUrl").ensureHasValue().ensureIsString();
        serverUrl = serverUrl.trim();
        if (serverUrl.endsWith("/"))
            serverUrl = serverUrl.substr(0, serverUrl.length - 1);
        this._serverUrl = serverUrl;
        this._master = SocketIo(this._serverUrl, {
            // WARNING: in that case, there is no fallback to long-polling
            transports: ["websocket"] // or [ 'websocket', 'polling' ], which is the same thing
        });
    }
    async subscribe(channel, event) {
        // should be synchronized;
        given(channel, "channel").ensureHasValue().ensureIsString();
        channel = channel.trim();
        given(event, "event").ensureHasValue().ensureIsString();
        event = event.trim();
        if (this._isDisposed)
            throw new ObjectDisposedException(this);
        await this._mutex.lock();
        try {
            const socketChannel = this._channels.find(t => t.channel === channel) ?? await this._createChannel(channel);
            return socketChannel.subscribe(event);
        }
        finally {
            this._mutex.release();
        }
    }
    // public async unsubscribe(channel: string, event?: string, handler?: (data: any) => void): Promise<void>
    // {
    //     // should be synchronized
    //     given(channel, "channel").ensureHasValue().ensureIsString();
    //     channel = channel.trim();
    //     await this._mutex.lock();
    //     try
    //     {
    //         if (!this._channels.has(channel))
    //             return;
    //         const socket = this._channels.get(channel);
    //         if (!event)
    //         {
    //             socket.close();
    //             this._channels.delete(channel);
    //             return;
    //         }
    //         given(event, "event").ensureHasValue().ensureIsString();
    //         event = event.trim();
    //         given(handler as Function, "handler").ensureIsFunction();
    //         socket.off(event, handler || null);
    //     }
    //     finally
    //     {
    //         this._mutex.release();
    //     }
    // }
    dispose() {
        if (this._disposePromise == null) {
            this._isDisposed = true;
            this._disposePromise = Promise.all(this._channels.map(t => t.dispose()));
            this._master.close();
        }
        return this._disposePromise;
    }
    _createChannel(channel) {
        return new Promise((resolve, reject) => {
            try {
                given(channel, "channel").ensureHasValue().ensureIsString();
                channel = channel.trim();
                this._master.once(`n-sock-joined_channel/${channel}`, (data) => {
                    if (data.channel === channel) {
                        const socket = SocketIo(`${this._serverUrl}/${channel}`, { transports: ["websocket"] });
                        const socketChannel = new SocketChannel(this._serverUrl, channel, socket, this._master);
                        this._channels.push(socketChannel);
                        resolve(socketChannel);
                    }
                    else
                        reject(new Error(`Joined channel mismatch; expected '${channel}', actual '${data.channel}'`));
                });
                this._master.emit("n-sock-join_channel", { channel });
            }
            catch (error) {
                reject(error);
            }
        });
    }
}
class InternalSocketChannelSubscription {
    _eventName;
    _isUnsubscribed = false;
    _eventHandler = null;
    _connectionChangeHandler = null;
    _unsubscribeHandler = null;
    get eventName() { return this._eventName; }
    get eventHandler() { return this._eventHandler; }
    get connectionChangeHandler() { return this._connectionChangeHandler; }
    constructor(eventName) {
        given(eventName, "eventName").ensureHasValue().ensureIsString();
        this._eventName = eventName;
    }
    onData(callback) {
        given(callback, "callback").ensureHasValue().ensureIsFunction();
        this._eventHandler = callback;
        return this;
    }
    onConnectionChange(callback) {
        given(callback, "callback").ensureHasValue().ensureIsFunction();
        this._connectionChangeHandler = callback;
        return this;
    }
    unsubscribe() {
        if (this._isUnsubscribed)
            return;
        if (this._unsubscribeHandler != null)
            this._unsubscribeHandler();
        this._isUnsubscribed = true;
    }
    onUnsubscribe(callback) {
        given(callback, "callback").ensureHasValue().ensureIsFunction();
        this._unsubscribeHandler = callback;
    }
}
class SocketChannel {
    _serverUrl;
    _channel;
    _socket;
    _master;
    _eventNames = new Set();
    _subscriptions = new Array();
    _isReconnecting = false;
    _isDisposed = false;
    get channel() { return this._channel; }
    constructor(serverUrl, channel, socket, master) {
        given(serverUrl, "serverUrl").ensureHasValue().ensureIsString();
        this._serverUrl = serverUrl;
        given(channel, "channel").ensureHasValue().ensureIsString();
        this._channel = channel;
        given(socket, "socket").ensureHasValue().ensureIsObject();
        this._socket = socket;
        given(master, "master").ensureHasValue().ensureIsObject();
        this._master = master;
        this._initialize();
    }
    subscribe(eventName) {
        given(eventName, "eventName").ensureHasValue().ensureIsString();
        eventName = eventName.trim();
        const subscription = new InternalSocketChannelSubscription(eventName);
        subscription.onUnsubscribe(() => this._subscriptions.remove(subscription));
        this._subscriptions.push(subscription);
        if (!this._eventNames.has(eventName)) {
            this._eventNames.add(eventName);
            this._socket.on(eventName, (data) => {
                this._subscriptions
                    .where(t => t.eventName === eventName && t.eventHandler != null)
                    .forEach(t => t.eventHandler(data));
            });
        }
        return subscription;
    }
    dispose() {
        if (!this._isDisposed) {
            this._isDisposed = true;
            this._subscriptions.clear();
            this._socket.off("connect_error");
            this._socket.close();
        }
        return Promise.resolve();
    }
    _initialize() {
        this._eventNames.forEach((eventName) => {
            this._socket.on(eventName, (data) => {
                this._subscriptions
                    .where(t => t.eventName === eventName && t.eventHandler != null)
                    .forEach(t => t.eventHandler(data));
            });
        });
        this._socket.on("connect", () => {
            this._subscriptions.where(t => t.connectionChangeHandler != null).forEach(t => t.connectionChangeHandler());
        });
        this._socket.on("connect_error", (err) => {
            console.warn(`SocketChannel connect_error due to ${err.message}`);
            if (err.message.trim() === "Invalid namespace" && !this._isReconnecting) {
                this._isReconnecting = true;
                this._master.once(`n-sock-joined_channel/${this._channel}`, (data) => {
                    try {
                        if (data.channel === this._channel) {
                            const socket = SocketIo(`${this._serverUrl}/${this._channel}`, { transports: ["websocket"] });
                            this._socket.off("connect_error");
                            this._socket.close();
                            this._socket = socket;
                            this._initialize();
                        }
                        else
                            throw new Error(`Joined channel mismatch; expected '${this._channel}', actual '${data.channel}'`);
                    }
                    catch (error) {
                        console.error(error);
                    }
                    finally {
                        this._isReconnecting = false;
                    }
                });
                this._master.emit("n-sock-join_channel", { channel: this._channel });
            }
            this._subscriptions.where(t => t.connectionChangeHandler != null).forEach(t => t.connectionChangeHandler());
        });
    }
}
//# sourceMappingURL=socket-client.js.map