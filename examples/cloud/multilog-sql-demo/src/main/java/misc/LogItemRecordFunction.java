package misc;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.RecordFunction;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

public final class LogItemRecordFunction implements RecordFunction<LogItem> {
	private final RecordScheme scheme;

	private LogItemRecordFunction(RecordScheme scheme) {
		this.scheme = scheme;
	}

	public static LogItemRecordFunction create(DefiningClassLoader classLoader) {
		RecordScheme scheme = createScheme(classLoader);
		return new LogItemRecordFunction(scheme);
	}

	@Override
	public RecordScheme getScheme() {
		return scheme;
	}

	@Override
	public Record apply(LogItem logItem) {
		Record record = getScheme().record();

		record.setInt("date", logItem.date);
		record.setInt("advertiser", logItem.advertiser);
		record.setInt("campaign", logItem.campaign);
		record.setInt("banner", logItem.banner);
		record.setLong("impressions", logItem.impressions);
		record.setLong("clicks", logItem.clicks);
		record.setLong("conversions", logItem.conversions);
		record.setDouble("revenue", logItem.revenue);
		record.set("testString", logItem.testString);

		return record;
	}

	private static RecordScheme createScheme(DefiningClassLoader classLoader) {
		RecordScheme scheme = RecordScheme.create(classLoader)
				.withField("date", int.class)
				.withField("advertiser", int.class)
				.withField("campaign", int.class)
				.withField("banner", int.class)
				.withField("impressions", long.class)
				.withField("clicks", long.class)
				.withField("conversions", long.class)
				.withField("revenue", double.class)
				.withField("testString", String.class);

		return scheme
				.withComparator(scheme.getFields())
				.build();
	}
}
