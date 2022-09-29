package jdbc;

import io.activej.inject.module.Module;
import io.activej.launchers.dataflow.jdbc.DataflowJdbcServerLauncher;
import module.MultilogDataflowSchemaModule;

public final class MultilogDataflowJdbcServerLauncher extends DataflowJdbcServerLauncher {

	@Override
	protected Module getDataflowSchemaModule() {
		return MultilogDataflowSchemaModule.create();
	}

	public static void main(String[] args) throws Exception {
		new MultilogDataflowJdbcServerLauncher().launch(args);
	}
}
