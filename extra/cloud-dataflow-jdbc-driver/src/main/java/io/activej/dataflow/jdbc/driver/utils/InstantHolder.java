package io.activej.dataflow.jdbc.driver.utils;

import java.time.Instant;

/*
	A helper class that adds proper {@code toString()} format for Instant
 */
public record InstantHolder(Instant instant) {
	@Override
	public String toString() {
		String result = instant.toString().replace('T', ' ');
		return result.substring(0, result.length() - 1);
	}
}
