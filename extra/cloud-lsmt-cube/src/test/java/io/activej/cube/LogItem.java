package io.activej.cube;

import io.activej.aggregation.annotation.Key;
import io.activej.aggregation.annotation.Measures;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Represents a log item (or fact).
 * <p>
 * {@link Serialize} annotation is used to mark fields that are to be serialized.
 * Such fields must also be declared 'public' for serializer to work.
 */
public class LogItem {
	/* Dimensions */
	@Key
	@Serialize
	public int date = randomInt(16570, 16580);

	@Key
	@Serialize
	public int advertiser = randomInt(0, 10);

	@Key
	@Serialize
	public int campaign = randomInt(0, 10);

	@Key
	@Serialize
	public int banner = randomInt(0, 10);

	@Measures
	@Serialize
	public long impressions;

	@Measures
	@Serialize
	public long clicks;

	@Measures
	@Serialize
	public long conversions;

	@Measures
	@Serialize
	public double revenue;

	@Measures
	@Nullable
	@Serialize
	@SerializeNullable
	public String testString;

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

	public LogItem(long impressions, long clicks, long conversions, double revenue) {
		this.impressions = impressions;
		this.clicks = clicks;
		this.conversions = conversions;
		this.revenue = revenue;
	}

	public LogItem(@NotNull String testString) {
		this.testString = testString;
	}

	/* Static factory methods for random facts */
	public static LogItem randomImpressionFact() {
		return new LogItem(1, 0, 0, 0);
	}

	public static LogItem randomClickFact() {
		return new LogItem(0, 1, 0, 0);
	}

	public static LogItem randomConversionFact() {
		return new LogItem(0, 0, 1, randomDouble(0, 10));
	}

	public static List<LogItem> getListOfRandomLogItems(int numberOfItems) {
		List<LogItem> logItems = new ArrayList<>();

		for (int i = 0; i < numberOfItems; ++i) {
			int type = randomInt(0, 2);

			if (type == 0) {
				logItems.add(randomImpressionFact());
			} else if (type == 1) {
				logItems.add(randomClickFact());
			} else if (type == 2) {
				logItems.add(randomConversionFact());
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

