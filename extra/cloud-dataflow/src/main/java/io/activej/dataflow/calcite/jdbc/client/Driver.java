package io.activej.dataflow.calcite.jdbc.client;

import org.apache.calcite.avatica.DriverVersion;

public class Driver extends org.apache.calcite.avatica.remote.Driver {
	public static final String CONNECT_STRING_PREFIX = "jdbc:activej:dataflow:";

	static {
		new Driver().register();
	}

	public Driver() {
		super();
	}

	@Override
	protected DriverVersion createDriverVersion() {
		return new DataflowDriverVersion();
	}

	@Override
	protected String getConnectStringPrefix() {
		return CONNECT_STRING_PREFIX;
	}

	@Override
	protected String getFactoryClassName(JdbcVersion jdbcVersion) {
		return switch (jdbcVersion) {
			case JDBC_30:
			case JDBC_40:
				throw new IllegalArgumentException("JDBC version not supported: "
						+ jdbcVersion);
			case JDBC_41:
			default:
				yield "io.activej.dataflow.calcite.jdbc.client.DataflowJdbc41Factory";
		};
	}
}
