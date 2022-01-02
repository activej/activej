/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.common;

import io.activej.common.exception.MalformedDataException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.round;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

public final class StringFormatUtils {

	public static String formatMemSize(MemSize memSize) {
		long bytes = memSize.toLong();

		if (bytes == 0) {
			return "0";
		}

		for (long unit = MemSize.TB; ; unit /= 1024L) {
			long divideResult = bytes / unit;
			long remainder = bytes % unit;

			if (divideResult == 0) {
				continue;
			}
			if (remainder == 0) {
				return divideResult + getUnit(unit);
			}
		}
	}

	private static String getUnit(long unit) {
		if (unit == MemSize.TB) {
			return "Tb";
		} else {
			switch ((int) unit) {
				case (int) MemSize.GB:
					return "Gb";
				case (int) MemSize.MB:
					return "Mb";
				case (int) MemSize.KB:
					return "Kb";
				case 1:
					return "";
				default:
					throw new IllegalArgumentException("Wrong unit");
			}
		}
	}

	private static final Pattern MEM_SIZE_PATTERN = Pattern.compile("(?<size>\\d+)([.](?<floating>\\d+))?\\s*(?<unit>(|g|m|k|t)b?)?(\\s+|$)", Pattern.CASE_INSENSITIVE);

	public static MemSize parseMemSize(String string) {
		Set<String> units = new HashSet<>();
		Matcher matcher = MEM_SIZE_PATTERN.matcher(string.trim().toLowerCase());
		long result = 0;

		int lastEnd = 0;
		while (!matcher.hitEnd()) {
			if (!matcher.find() || matcher.start() != lastEnd) {
				throw new IllegalArgumentException("Invalid MemSize: " + string);
			}
			lastEnd = matcher.end();
			String unit = matcher.group("unit");

			if (unit == null) {
				unit = "";
			}

			if (!unit.endsWith("b")) {
				unit += "b";
			}

			if (!units.add(unit)) {
				throw new IllegalArgumentException("Memory unit " + unit + " occurs more than once in: " + string);
			}

			long memsize = Long.parseLong(matcher.group("size"));
			long numerator = 0;
			long denominator = 1;
			String floatingPoint = matcher.group("floating");
			if (floatingPoint != null) {
				if (unit.equals("b")) {
					throw new IllegalArgumentException("MemSize unit bytes cannot be fractional");
				}
				numerator = Long.parseLong(floatingPoint);
				for (int i = 0; i < floatingPoint.length(); i++) {
					denominator *= 10;
				}
			}

			double fractional = (double) numerator / denominator;
			switch (unit) {
				case "tb":
					result += memsize * MemSize.TB;
					result += round(MemSize.TB * fractional);
					break;
				case "gb":
					result += memsize * MemSize.GB;
					result += round(MemSize.GB * fractional);
					break;
				case "mb":
					result += memsize * MemSize.MB;
					result += round(MemSize.MB * fractional);
					break;
				case "kb":
					result += memsize * MemSize.KB;
					result += round(MemSize.KB * fractional);
					break;
				case "b":
				case "":
					result += memsize;
					break;
			}
		}
		return MemSize.of(result);
	}

	public static String formatDuration(Duration value) {
		if (value.isZero()) {
			return "0 seconds";
		}
		String result = "";
		long days, hours, minutes, seconds, nano, milli;
		days = value.toDays();
		if (days != 0) {
			result += days + " days ";
		}
		hours = value.toHours() - days * 24;
		if (hours != 0) {
			result += hours + " hours ";
		}
		minutes = value.toMinutes() - days * 1440 - hours * 60;
		if (minutes != 0) {
			result += minutes + " minutes ";
		}
		seconds = value.getSeconds() - days * 86400 - hours * 3600 - minutes * 60;
		if (seconds != 0) {
			result += seconds + " seconds ";
		}
		nano = value.getNano();
		milli = (nano - nano % 1000000) / 1000000;
		if (milli != 0) {
			result += milli + " millis ";
		}
		nano = nano % 1000000;
		if (nano != 0) {
			result += nano + " nanos ";
		}
		return result.trim();
	}

	private static final Pattern DURATION_PATTERN = Pattern.compile("(?<time>-?\\d+)([.](?<floating>\\d+))?\\s+(?<unit>days?|hours?|minutes?|seconds?|millis?|nanos?)(\\s+|$)");
	private static final int NANOS_IN_MILLI = 1000000;
	private static final int MILLIS_IN_SECOND = 1000;
	private static final int SECONDS_PER_MINUTE = 60;
	private static final int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
	private static final int SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

	public static Duration parseDuration(String string) {
		string = string.trim();
		if (string.startsWith("-P") || string.startsWith("P")) {
			return Duration.parse(string);
		}
		Set<String> units = new HashSet<>();
		int days = 0, hours = 0, minutes = 0;
		long seconds = 0, millis = 0, nanos = 0;
		double doubleSeconds = 0.0;
		long result;

		Matcher matcher = DURATION_PATTERN.matcher(string.trim().toLowerCase());
		int lastEnd = 0;
		while (!matcher.hitEnd()) {
			if (!matcher.find() || matcher.start() != lastEnd) {
				throw new IllegalArgumentException("Invalid duration: " + string);
			}
			lastEnd = matcher.end();
			String unit = matcher.group("unit");
			if (!unit.endsWith("s")) {
				unit += "s";
			}
			if (!units.add(unit)) {
				throw new IllegalArgumentException("Time unit " + unit + " occurs more than once in: " + string);
			}

			result = Long.parseLong(matcher.group("time"));
			int numerator = 0;
			int denominator = 1;
			String floatingPoint = matcher.group("floating");
			if (floatingPoint != null) {
				if (unit.equals("nanos")) {
					throw new IllegalArgumentException("Time unit nanos cannot be fractional");
				}
				numerator = Integer.parseInt(floatingPoint);
				for (int i = 0; i < floatingPoint.length(); i++) {
					denominator *= 10;
				}
			}

			double fractional = (double) numerator / denominator;
			switch (unit) {
				case "days":
					days = (int) result;
					doubleSeconds += SECONDS_PER_DAY * fractional;
					break;
				case "hours":
					hours += (int) result;
					doubleSeconds += SECONDS_PER_HOUR * fractional;
					break;
				case "minutes":
					minutes += (int) result;
					doubleSeconds += SECONDS_PER_MINUTE * fractional;
					break;
				case "seconds":
					seconds += (int) result;
					doubleSeconds += fractional;
					break;
				case "millis":
					millis += result;
					doubleSeconds += fractional / MILLIS_IN_SECOND;
					break;
				case "nanos":
					nanos += result;
					break;
			}
		}

		return Duration.ofDays(days)
				.plusHours(hours)
				.plusMinutes(minutes)
				.plusSeconds(seconds)
				.plusMillis(millis)
				.plusNanos(nanos)
				.plusSeconds(round(doubleSeconds))
				.plusNanos(round((doubleSeconds - round(doubleSeconds)) * (NANOS_IN_MILLI * MILLIS_IN_SECOND)));
	}

	public static String formatPeriod(Period value) {
		if (value.isZero()) {
			return "0 days";
		}
		String result = "";
		int years = value.getYears(), months = value.getMonths(),
				days = value.getDays();
		if (years != 0) {
			result += years + " years ";
		}
		if (months != 0) {
			result += months + " months ";
		}
		if (days != 0) {
			result += days + " days ";
		}
		return result.trim();
	}

	private static final Pattern PERIOD_PATTERN = Pattern.compile("(?<str>((?<time>-?\\d+)([.](?<floating>\\d+))?\\s+(?<unit>years?|months?|days?))(\\s+|$))");

	/**
	 * Parses value to Period.
	 * 1 year 2 months 3 days == Period.of(1, 2, 3)
	 * Every value can be negative, but you can't make all Period negative by negating year.
	 * In ISO format you can write -P1Y2M, which means -1 years -2 months in this format
	 * There can't be any spaces between '-' and DIGIT:
	 * -1  - Right
	 * - 2 - Wrong
	 */
	public static Period parsePeriod(String string) {
		string = string.trim();
		if (string.startsWith("-P") || string.startsWith("P")) {
			return Period.parse(string);
		}
		int years = 0, months = 0, days = 0;
		Set<String> units = new HashSet<>();

		Matcher matcher = PERIOD_PATTERN.matcher(string.trim().toLowerCase());
		int lastEnd = 0;
		while (!matcher.hitEnd()) {
			if (!matcher.find() || matcher.start() != lastEnd) {
				throw new IllegalArgumentException("Invalid period: " + string);
			}
			lastEnd = matcher.end();
			String unit = matcher.group("unit");
			if (!unit.endsWith("s")) {
				unit += "s";
			}
			if (!units.add(unit)) {
				throw new IllegalArgumentException("Time unit: " + unit + " occurs more than once.");
			}
			int result = Integer.parseInt(matcher.group("time"));
			switch (unit) {
				case "years":
					years = result;
					break;
				case "months":
					months = result;
					break;
				case "days":
					days = result;
					break;
			}
		}
		return Period.of(years, months, days);
	}

	private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
			.parseCaseInsensitive()
			.append(ISO_LOCAL_DATE)
			.appendLiteral(' ')
			.append(ISO_LOCAL_TIME)
			.toFormatter();

	public static String formatLocalDateTime(LocalDateTime value) {
		return value.format(DATE_TIME_FORMATTER);
	}

	public static LocalDateTime parseLocalDateTime(String string) {
		try {
			return LocalDateTime.parse(string, DATE_TIME_FORMATTER);
		} catch (DateTimeParseException e) {
			return LocalDateTime.parse(string);
		}
	}

	public static String formatInstant(Instant value) {
		String result = value.toString().replace('T', ' ');
		return result.substring(0, result.length() - 1);
	}

	public static Instant parseInstant(String string) {
		string = string.trim();
		return Instant.parse(string.replace(' ', 'T') + "Z");
	}

	public static InetSocketAddress parseInetSocketAddress(String addressAndPort) throws MalformedDataException {
		int portPos = addressAndPort.lastIndexOf(':');
		if (portPos == -1) {
			try {
				return new InetSocketAddress(Integer.parseInt(addressAndPort));
			} catch (NumberFormatException nfe) {
				throw new MalformedDataException(nfe);
			}
		}
		String addressStr = addressAndPort.substring(0, portPos);
		String portStr = addressAndPort.substring(portPos + 1);
		int port;
		try {
			port = Integer.parseInt(portStr);
		} catch (NumberFormatException nfe) {
			throw new MalformedDataException(nfe);
		}

		if (port < 0 || port >= 65536) {
			throw new MalformedDataException("Invalid address. Port is not in range [0, 65536) " + addressStr);
		}
		if ("*".equals(addressStr)) {
			return new InetSocketAddress(port);
		}
		try {
			InetAddress address = InetAddress.getByName(addressStr);
			return new InetSocketAddress(address, port);
		} catch (UnknownHostException e) {
			throw new MalformedDataException(e);
		}
	}

}
