package io.activej.dataflow.calcite.jdbc;

import io.activej.common.service.BlockingService;
import org.apache.calcite.avatica.server.HttpServer;

public final class JdbcServerService implements BlockingService {
	private final HttpServer server;

	private JdbcServerService(HttpServer server) {
		this.server = server;
	}

	public static JdbcServerService create(HttpServer server) {
		return new JdbcServerService(server);
	}

	@Override
	public void start() throws Exception {
		server.start();
	}

	@Override
	public void stop() throws Exception {
		server.stop();
	}
}
