package io.activej.redis;

public final class ScoreInterval {
	private static final String NEGATIVE_INFINITY = "-inf";
	private static final String POSITIVE_INFINITY = "+inf";

	private final String min;
	private final String max;

	private ScoreInterval(String min, String max) {
		this.min = min;
		this.max = max;
	}

	public static ScoreInterval interval(double min, double max) {
		return new ScoreInterval(toInterval(min, false), toInterval(max, false));
	}

	public static ScoreInterval interval(double min, double max, boolean minExcluded, boolean maxExcluded) {
		return new ScoreInterval(toInterval(min, minExcluded), toInterval(max, maxExcluded));
	}

	public static ScoreInterval to(double max, boolean maxExcluded) {
		return new ScoreInterval(NEGATIVE_INFINITY, toInterval(max, maxExcluded));
	}

	public static ScoreInterval to(double max) {
		return new ScoreInterval(NEGATIVE_INFINITY, toInterval(max, false));
	}

	public static ScoreInterval from(double min, boolean minExcluded) {
		return new ScoreInterval(toInterval(min, minExcluded), POSITIVE_INFINITY);
	}

	public static ScoreInterval from(double min) {
		return new ScoreInterval(toInterval(min, false), POSITIVE_INFINITY);
	}

	public static ScoreInterval all() {
		return new ScoreInterval(NEGATIVE_INFINITY, POSITIVE_INFINITY);
	}

	public ScoreInterval reverse() {
		return new ScoreInterval(max, min);
	}

	public String getMin() {
		return min;
	}

	public String getMax() {
		return max;
	}

	private static String toInterval(double value, boolean valueExcluded) {
		return valueExcluded ? "(" + value : String.valueOf(value);
	}
}
