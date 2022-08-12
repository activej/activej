package misc;

import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

// Taken from Cube tests
public class LogItem {
	@Serialize
	public int date = randomInt(16570, 16580);

	@Serialize
	public int advertiser = randomInt(0, 10);

	@Serialize
	public int campaign = randomInt(0, 10);

	@Serialize
	public int banner = randomInt(0, 10);

	@Serialize
	public long impressions;

	@Serialize
	public long clicks;

	@Serialize
	public long conversions;

	@Serialize
	public double revenue;

	@Serialize
	@SerializeNullable
	public @Nullable String testString;

	public LogItem() {
	}

	public LogItem(int date, int advertiser, int campaign, int banner,
			long impressions, long clicks, long conversions, double revenue) {
		this.date = date;
		this.advertiser = advertiser;
		this.campaign = campaign;
		this.banner = banner;
		this.impressions = impressions;
		this.clicks = clicks;
		this.conversions = conversions;
		this.revenue = revenue;
	}

	public LogItem(long impressions, long clicks, long conversions, double revenue, String testString) {
		this.impressions = impressions;
		this.clicks = clicks;
		this.conversions = conversions;
		this.revenue = revenue;
		this.testString = testString;
	}

	public LogItem(@NotNull String testString) {
		this.testString = testString;
	}

	/* Static factory methods for random facts */
	public static LogItem randomImpressionFact() {
		return new LogItem(1, 0, 0, 0, null);
	}

	public static LogItem randomClickFact() {
		return new LogItem(0, 1, 0, 0, null);
	}

	public static LogItem randomConversionFact() {
		return new LogItem(0, 0, 1, randomDouble(0, 10), null);
	}

	public static LogItem randomTestString() {
		return new LogItem(0, 0, 0, 0, UUID.randomUUID().toString());
	}

	public static List<LogItem> getListOfRandomLogItems(int numberOfItems) {
		List<LogItem> logItems = new ArrayList<>();

		for (int i = 0; i < numberOfItems; ++i) {
			int type = randomInt(0, 3);

			if (type == 0) {
				logItems.add(randomImpressionFact());
			} else if (type == 1) {
				logItems.add(randomClickFact());
			} else if (type == 2) {
				logItems.add(randomConversionFact());
			} else if (type == 3) {
				logItems.add(randomTestString());
			}
		}

		return logItems;
	}

	private final static Random RANDOM = new Random();

	public static int randomInt(int min, int max) {
		return RANDOM.nextInt((max - min) + 1) + min;
	}

	public static double randomDouble(double min, double max) {
		return min + (max - min) * RANDOM.nextDouble();
	}
}

