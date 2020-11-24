package io.activej.redis.api;

public enum DistanceUnit {
	M, KM, FT, MI;

	private final String argument;

	DistanceUnit() {
		this.argument = name().toLowerCase();
	}

	public String getArgument() {
		return argument;
	}
}
