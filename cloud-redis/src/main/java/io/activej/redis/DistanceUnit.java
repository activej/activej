package io.activej.redis;

public enum DistanceUnit {
	M, KM, FT, MI;

	private final String argument;

	DistanceUnit() {
		this.argument = name().toLowerCase();
	}

	String getArgument() {
		return argument;
	}
}
