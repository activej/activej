package io.activej.net.connection;

import io.activej.common.exception.CloseException;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

public interface Connection {
	CloseException CLOSE_EXCEPTION = new CloseException(ConnectionPool.class, "Default close");

	Promise<Void> returnToPool();

	boolean isClosed();

	Promise<Void> closeEx(@NotNull Throwable e);

	default Promise<Void> close() {
		return closeEx(CLOSE_EXCEPTION);
	}
}
