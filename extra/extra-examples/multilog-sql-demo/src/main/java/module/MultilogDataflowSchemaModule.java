package module;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.table.DataflowTable;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import misc.LogItem;

public class MultilogDataflowSchemaModule extends AbstractModule {
	public static final String LOG_ITEM_TABLE_NAME = "log_item";

	private MultilogDataflowSchemaModule() {
	}

	public static MultilogDataflowSchemaModule create() {
		return new MultilogDataflowSchemaModule();
	}

	@ProvidesIntoSet
	DataflowTable<LogItem> logItemTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, LOG_ITEM_TABLE_NAME, LogItem.class)
			.withColumn("date", int.class, logItem -> logItem.date)
			.withColumn("advertiser", int.class, logItem -> logItem.advertiser)
			.withColumn("campaign", int.class, logItem -> logItem.campaign)
			.withColumn("banner", int.class, logItem -> logItem.banner)
			.withColumn("impressions", long.class, logItem -> logItem.impressions)
			.withColumn("clicks", long.class, logItem -> logItem.clicks)
			.withColumn("conversions", long.class, logItem -> logItem.conversions)
			.withColumn("revenue", double.class, logItem -> logItem.revenue)
			.withColumn("testString", String.class, logItem -> logItem.testString)
			.build();
	}
}
