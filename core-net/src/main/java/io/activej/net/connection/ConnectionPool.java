package io.activej.net.connection;

import io.activej.promise.Promise;

public interface ConnectionPool {
	Promise<? extends Connection> getConnection();
}
