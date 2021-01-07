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

package io.activej.redis;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class SortModifier {
	public static final String BY = "BY";
	public static final String LIMIT = "LIMIT";
	public static final String GET = "GET";
	public static final String ALPHA = "ALPHA";

	public static final String ASC = "ASC";
	public static final String DESC = "DESC";

	public static final String STORE = "STORE";

	private static final SortModifier ASC_MODIFIER = new SortModifier(singletonList(ASC));
	private static final SortModifier DESC_MODIFIER = new SortModifier(singletonList(DESC));
	private static final SortModifier ALPHA_MODIFIER = new SortModifier(singletonList(ALPHA));

	private final List<String> arguments;

	private SortModifier(List<String> arguments) {
		this.arguments = arguments;
	}

	public static SortModifier alpha() {
		return ALPHA_MODIFIER;
	}

	public static SortModifier asc() {
		return ASC_MODIFIER;
	}

	public static SortModifier desc() {
		return DESC_MODIFIER;
	}

	public static SortModifier by(String pattern) {
		return new SortModifier(asList(BY, pattern));
	}

	public static SortModifier limit(long offset, long count) {
		return new SortModifier(asList(LIMIT, String.valueOf(offset), String.valueOf(count)));
	}

	public static SortModifier get(String pattern) {
		return new SortModifier(asList(GET, pattern));
	}

	public List<String> getArguments() {
		return arguments;
	}
}
