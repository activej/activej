package io.activej.launchers.crdt.rpc;

import io.activej.config.Config;
import io.activej.crdt.hash.CrdtMap;
import io.activej.crdt.wal.WriteAheadLog;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.rpc.server.RpcServer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;

public abstract class CrdtRpcServerModule<K extends Comparable<K>, S> extends AbstractModule {
	public static final int DEFAULT_PORT = 9000;
	public static final String PROPERTIES_FILE = "crdt-rpc-server.properties";

	protected abstract List<Class<?>> getMessageTypes();

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(DEFAULT_PORT)))
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofSystemProperties("config"));
	}

	@Provides
	@Eager
	@SuppressWarnings("unchecked")
	RpcServer server(Eventloop eventloop,
					 CrdtMap<K, S> map,
					 WriteAheadLog<K, S> writeAheadLog,
					 Map<Class<?>, RpcRequestHandler<?, ?>> handlers,
					 Config config) {
		RpcServer server = RpcServer.create(eventloop)
				.withListenAddress(config.get(ofInetSocketAddress(), "listenAddresses"))
				.withMessageTypes(getMessageTypes());
		for (Map.Entry<Class<?>, RpcRequestHandler<?, ?>> entry : handlers.entrySet()) {
			server.withHandler((Class<Object>) entry.getKey(), (RpcRequestHandler<Object, Object>) entry.getValue());
		}
		return server;
	}
}
