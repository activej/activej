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

import static io.activej.common.Checks.checkArgument;
import static java.util.Arrays.asList;

public final class LposModifier {
	public static final String RANK = "RANK";
	public static final String COUNT = "COUNT";
	public static final String MAXLEN = "MAXLEN";

	private final List<String> arguments;

	private LposModifier(List<String> arguments) {
		this.arguments = arguments;
	}

	public static LposModifier rank(long rank) {
		checkArgument(rank != 0, "RANK cannot be zero");
		return new LposModifier(asList(RANK, String.valueOf(rank)));
	}

	public static LposModifier maxlen(long maxlen) {
		checkArgument(maxlen > 0, "MAXLEN cannot be negative");
		return new LposModifier(asList(MAXLEN, String.valueOf(maxlen)));
	}

	public List<String> getArguments() {
		return arguments;
	}
}
