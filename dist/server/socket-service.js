import { Emitter } from "@socket.io/redis-emitter";
import { given } from "@nivinjoseph/n-defensive";
import { ObjectDisposedException } from "@nivinjoseph/n-exception";
/**
 * This should only emit (publish) events
 */
export class SocketService {
    _socketClient;
    _redisClient;
    _isDisposed = false;
    _disposePromise = null;
    constructor(redisClient) {
        given(redisClient, "redisClient").ensureHasValue().ensureIsObject();
        this._redisClient = redisClient;
        this._socketClient = new Emitter(this._redisClient);
    }
    publish(channel, event, data) {
        given(channel, "channel").ensureHasValue().ensureIsString();
        channel = channel.trim();
        given(event, "event").ensureHasValue().ensureIsString();
        event = event.trim();
        given(data, "data").ensureHasValue().ensureIsObject();
        if (this._isDisposed)
            throw new ObjectDisposedException(this);
        this._socketClient.of(`/${channel}`).emit(event, data);
    }
    dispose() {
        if (!this._isDisposed) {
            this._isDisposed = true;
            this._disposePromise = Promise.resolve();
        }
        return this._disposePromise;
    }
}
//# sourceMappingURL=socket-service.js.map