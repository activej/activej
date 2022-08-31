package io.activej.dataflow.jdbc.driver;

import org.apache.calcite.avatica.DriverVersion;

public final class DataflowDriverVersion extends DriverVersion {
	public DataflowDriverVersion() {
		super(
				"ActiveJ Dataflow JDBC Driver",
				"6.0",
				"ActiveJ Dataflow",
				"6.0",
				true,
				6,
				0,
				6,
				0
		);
	}
}
