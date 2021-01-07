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
