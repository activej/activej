package module;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import misc.LogItem;
import misc.LogItem_RecordFunction;

public class MultilogDataflowSchemaModule extends AbstractModule {
	public static final String LOG_ITEM_TABLE_NAME = "log_item";

	private MultilogDataflowSchemaModule() {
	}

	public static MultilogDataflowSchemaModule create() {
		return new MultilogDataflowSchemaModule();
	}

	@ProvidesIntoSet
	DataflowTable logItemTable(LogItem_RecordFunction recordFunction) {
		return DataflowTable.create(LOG_ITEM_TABLE_NAME, LogItem.class, recordFunction);
	}

	@Provides
	LogItem_RecordFunction recordFunction(DefiningClassLoader classLoader) {
		return LogItem_RecordFunction.create(classLoader);
	}
}
