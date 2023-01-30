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

package io.activej.cube.http;

import io.activej.common.exception.MalformedDataException;
import io.activej.cube.CubeQuery.Ordering;
import io.activej.cube.ReportType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static io.activej.common.Utils.not;

public class Utils {
	static final String MEASURES_PARAM = "measures";
	static final String ATTRIBUTES_PARAM = "attributes";
	static final String WHERE_PARAM = "where";
	static final String HAVING_PARAM = "having";
	static final String SORT_PARAM = "sort";
	static final String LIMIT_PARAM = "limit";
	static final String OFFSET_PARAM = "offset";
	static final String REPORT_TYPE_PARAM = "reportType";

	private static final Pattern splitter = Pattern.compile(",");

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	static String formatOrderings(List<Ordering> orderings) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Ordering ordering : orderings) {
			sb.append((first ? "" : ",") + ordering.getField() + ":" + (ordering.isAsc() ? "ASC" : "DESC"));
			first = false;
		}
		return sb.toString();
	}

	static List<Ordering> parseOrderings(String string) throws MalformedDataException {
		List<Ordering> result = new ArrayList<>();
		List<String> tokens = splitter.splitAsStream(string)
				.map(String::trim)
				.filter(not(String::isEmpty))
				.toList();
		for (String s : tokens) {
			int i = s.indexOf(':');
			if (i == -1) {
				throw new MalformedDataException("Failed to parse orderings, missing semicolon");
			}
			String field = s.substring(0, i);
			String tail = s.substring(i + 1).toLowerCase();
			if ("asc".equals(tail))
				result.add(Ordering.asc(field));
			else if ("desc".equals(tail))
				result.add(Ordering.desc(field));
			else {
				throw new MalformedDataException("Tail is neither 'asc' nor 'desc'");
			}
		}
		return result;
	}

	static int parseNonNegativeInteger(String parameter) throws MalformedDataException {
		try {
			int value = Integer.parseInt(parameter);
			if (value < 0) throw new MalformedDataException("Must be non negative value: " + parameter);
			return value;
		} catch (NumberFormatException e) {
			throw new MalformedDataException("Could not parse: " + parameter, e);
		}
	}

	static ReportType parseReportType(String parameter) throws MalformedDataException {
		try {
			return ReportType.valueOf(parameter.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new MalformedDataException("'" + parameter + "' neither of: " + Arrays.toString(ReportType.values()), e);
		}
	}
}
