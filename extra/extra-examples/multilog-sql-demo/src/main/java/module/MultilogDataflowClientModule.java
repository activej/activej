package module;

import io.activej.dataflow.calcite.inject.CalciteClientModule;
import io.activej.inject.module.AbstractModule;

public class MultilogDataflowClientModule extends AbstractModule {
	private MultilogDataflowClientModule() {
	}

	public static MultilogDataflowClientModule create() {
		return new MultilogDataflowClientModule();
	}

	@Override
	protected void configure() {
		install(CalciteClientModule.create());
		install(MultilogDataflowSchemaModule.create());
	}
}
