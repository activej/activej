package io.activej.dataflow.jdbc.driver.utils;

import java.time.LocalDateTime;

/**
 * A helper class that adds proper {@code toString()} format for {@link LocalDateTime}
 */
public record LocalDateTimeHolder(LocalDateTime localDateTime) {
	@Override
	public String toString() {
		return localDateTime.toString().replace('T', ' ');
	}
}
