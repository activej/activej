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

package io.activej.http;

import io.activej.bytebuf.ByteBuf;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpUtils.*;

final class HttpDate {
	private static final int HOUR_SECONDS = 60 * 60;
	private static final int DAY_SECONDS = 24 * HOUR_SECONDS;
	private static final int YEAR_SECONDS = 365 * DAY_SECONDS;
	private static final int FOUR_YEAR_SECONDS = 1461 * DAY_SECONDS;

	private static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	private static final int[] DAYS_IN_MONTH_LEAP = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

	private static final byte[] GMT = encodeAscii("GMT");

	private static final byte[][] DAYS_OF_WEEK = new byte[7][];

	static {
		DAYS_OF_WEEK[0] = encodeAscii("Sun");
		DAYS_OF_WEEK[1] = encodeAscii("Mon");
		DAYS_OF_WEEK[2] = encodeAscii("Tue");
		DAYS_OF_WEEK[3] = encodeAscii("Wed");
		DAYS_OF_WEEK[4] = encodeAscii("Thu");
		DAYS_OF_WEEK[5] = encodeAscii("Fri");
		DAYS_OF_WEEK[6] = encodeAscii("Sat");
	}

	private static final byte[][] MONTHS_IN_YEAR = new byte[12][];

	static {
		MONTHS_IN_YEAR[0] = encodeAscii("Jan");
		MONTHS_IN_YEAR[1] = encodeAscii("Feb");
		MONTHS_IN_YEAR[2] = encodeAscii("Mar");
		MONTHS_IN_YEAR[3] = encodeAscii("Apr");
		MONTHS_IN_YEAR[4] = encodeAscii("May");
		MONTHS_IN_YEAR[5] = encodeAscii("Jun");
		MONTHS_IN_YEAR[6] = encodeAscii("Jul");
		MONTHS_IN_YEAR[7] = encodeAscii("Aug");
		MONTHS_IN_YEAR[8] = encodeAscii("Sep");
		MONTHS_IN_YEAR[9] = encodeAscii("Oct");
		MONTHS_IN_YEAR[10] = encodeAscii("Nov");
		MONTHS_IN_YEAR[11] = encodeAscii("Dec");
	}

	static long decode(byte[] bytes, int start) throws MalformedHttpException {
		try {
			int day = trimAndDecodePositiveInt(bytes, start + 5, 2);

			int month = -1;
			for (int i = 0; i < MONTHS_IN_YEAR.length; i++) {
				byte[] entry = MONTHS_IN_YEAR[i];
				if (entry[0] == bytes[start + 8] &&
						entry[1] == bytes[start + 9] &&
						entry[2] == bytes[start + 10]) {
					month = i;
				}
			}

			int yearLength = '0' <= bytes[start + 12 + 2] && bytes[start + 12 + 2] <= '9' ? 4 : 2;

			int year = (yearLength == 2 ? 2000 : 0) + trimAndDecodePositiveInt(bytes, start + 12, yearLength);
			int hour = trimAndDecodePositiveInt(bytes, start + 13 + yearLength, 2);
			int minutes = trimAndDecodePositiveInt(bytes, start + 16 + yearLength, 2);
			int seconds = trimAndDecodePositiveInt(bytes, start + 19 + yearLength, 2);
			boolean isLeapYear = isLeap(year);

			int[] days = isLeapYear ? DAYS_IN_MONTH_LEAP : DAYS_IN_MONTH;

			year = year - 1970;
			int yearsLeft = year % 4;
			long timestamp = 0;
			int fy = (year - yearsLeft) / 4;
			for (int i = 0; i < fy; i++) {
				timestamp += FOUR_YEAR_SECONDS;
			}

			timestamp += yearsLeft * YEAR_SECONDS;
			if (yearsLeft > 2) {
				// 1972 was a leap year and this code is assumed to be either deprecated or fixed before year 2100
				timestamp += DAY_SECONDS;
			}

			for (int i = 0; i < month; i++) {
				timestamp += (long) DAY_SECONDS * days[i];
			}

			for (int i = 1; i < day; i++) {
				timestamp += DAY_SECONDS;
			}

			timestamp += ((60L * hour + minutes) * 60) + seconds;

			return timestamp;
		} catch (RuntimeException ignored) {
			throw new MalformedHttpException("Failed to decode date");
		}
	}

	static void render(long epochSeconds, ByteBuf buf) {
		int pos = render(epochSeconds, buf.array(), buf.tail());
		buf.tail(pos);
	}

	static int render(long epochSeconds, byte[] bytes, int pos) {
		int fourYears = (int) (epochSeconds / FOUR_YEAR_SECONDS);
		int year = fourYears * 4 + 1970;
		int seconds = (int) (epochSeconds - fourYears * FOUR_YEAR_SECONDS);
		boolean isLeapYear = false;

		if (seconds >= YEAR_SECONDS) {
			year++;
			seconds -= YEAR_SECONDS;
			if (seconds >= YEAR_SECONDS) {
				year++;
				seconds -= YEAR_SECONDS;
				if (seconds >= (YEAR_SECONDS + DAY_SECONDS)) {
					year++;
					seconds -= YEAR_SECONDS + DAY_SECONDS;
				} else {
					isLeapYear = true;
				}
			}
		}

		int dayOfYear = seconds / DAY_SECONDS;
		seconds -= dayOfYear * DAY_SECONDS;

		int[] monthsDays = isLeapYear ? DAYS_IN_MONTH_LEAP : DAYS_IN_MONTH;
		int month = 0;
		int day = dayOfYear;
		while (day >= monthsDays[month]) {
			day -= monthsDays[month++];
		}

		int hours = seconds / (60 * 60);
		seconds -= hours * (60 * 60);

		int minutes = seconds / 60;
		seconds -= minutes * 60;

		int dayOfWeek = (int) ((epochSeconds / (24 * 60 * 60) + 4) % 7) + 1;

		byte[] stringDay = DAYS_OF_WEEK[dayOfWeek - 1];
		for (byte b : stringDay) {
			bytes[pos++] = b;
		}
		bytes[pos++] = COMMA;
		bytes[pos++] = SP;

		if (day < 9) {
			bytes[pos++] = ZERO;
		}
		day += 1;
		pos += encodePositiveInt(bytes, pos, day);
		bytes[pos++] = SP;

		byte[] stringMonth = MONTHS_IN_YEAR[month];
		for (byte b : stringMonth) {
			bytes[pos++] = b;
		}
		bytes[pos++] = SP;

		pos += encodePositiveInt(bytes, pos, year);
		bytes[pos++] = SP;

		if (hours < 10) {
			bytes[pos++] = ZERO;
		}
		pos += encodePositiveInt(bytes, pos, hours);
		bytes[pos++] = COLON;

		if (minutes < 10) {
			bytes[pos++] = ZERO;
		}
		pos += encodePositiveInt(bytes, pos, minutes);
		bytes[pos++] = COLON;

		if (seconds < 10) {
			bytes[pos++] = ZERO;
		}
		pos += encodePositiveInt(bytes, pos, seconds);
		bytes[pos++] = SP;

		for (byte b : GMT) {
			bytes[pos++] = b;
		}

		return pos;
	}

	private static boolean isLeap(int year) {
		if (year % 4 == 0) {
			if (year % 100 == 0) {
				return year % 400 == 0;
			} else {
				return true;
			}
		}
		return false;
	}
}
