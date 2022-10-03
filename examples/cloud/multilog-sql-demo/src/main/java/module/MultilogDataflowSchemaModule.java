package module;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import misc.LogItem;
import misc.LogItemRecordFunction;

public class MultilogDataflowSchemaModule extends AbstractModule {
	public static final String LOG_ITEM_TABLE_NAME = "log_item";

	private MultilogDataflowSchemaModule() {
	}

	public static MultilogDataflowSchemaModule create() {
		return new MultilogDataflowSchemaModule();
	}

	@ProvidesIntoSet
	DataflowTable logItemTable(LogItemRecordFunction recordFunction) {
		return DataflowTable.create(LOG_ITEM_TABLE_NAME, LogItem.class, recordFunction);
	}

	@Provides
	LogItemRecordFunction recordFunction(DefiningClassLoader classLoader) {
		return LogItemRecordFunction.create(classLoader);
	}
}
