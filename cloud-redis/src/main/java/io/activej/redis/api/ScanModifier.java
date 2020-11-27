package io.activej.redis.api;

import java.util.List;

import static java.util.Arrays.asList;

public final class ScanModifier {
	public static final String MATCH = "MATCH";
	public static final String COUNT = "COUNT";
	public static final String TYPE = "TYPE";

	private final List<String> arguments;

	private ScanModifier(List<String> arguments) {
		this.arguments = arguments;
	}

	public static ScanModifier match(String pattern) {
		return new ScanModifier(asList(MATCH, pattern));
	}

	public static ScanModifier count(long count) {
		return new ScanModifier(asList(COUNT, String.valueOf(count)));
	}

	public List<String> getArguments() {
		return arguments;
	}
}
