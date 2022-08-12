package module;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import misc.LogItem;
import misc.LogItemRecordFunction;

import java.util.Collections;

import static io.activej.dataflow.proto.serializer.ProtobufUtils.ofObject;

public class MultilogDataflowSchemaModule extends AbstractModule {
	public static final String LOG_ITEM_TABLE_NAME = "log_item";

	private MultilogDataflowSchemaModule() {
	}

	public static MultilogDataflowSchemaModule create() {
		return new MultilogDataflowSchemaModule();
	}

	@Provides
	DataflowSchema schema(DataflowTable<LogItem> logItemTable) {
		return DataflowSchema.create(Collections.singletonMap(LOG_ITEM_TABLE_NAME, logItemTable));
	}

	@Provides
	DataflowTable<LogItem> logItemTable(LogItemRecordFunction recordFunction) {
		return DataflowTable.create(LogItem.class, recordFunction, ofObject(() -> recordFunction));
	}

	@Provides
	LogItemRecordFunction recordFunction(DefiningClassLoader classLoader) {
		return LogItemRecordFunction.create(classLoader);
	}
}
