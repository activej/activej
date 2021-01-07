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

import java.time.Duration;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class RestoreModifier {
	public static final String REPLACE = "REPLACE";
	public static final String ABSTTL = "ABSTTL";
	public static final String IDLETIME = "IDLETIME";
	public static final String FREQ = "FREQ";

	private static final RestoreModifier REPLACE_MODIFIER = new RestoreModifier(singletonList(REPLACE));
	private static final RestoreModifier ABSTTL_MODIFIER = new RestoreModifier(singletonList(ABSTTL));

	private final List<String> arguments;

	private RestoreModifier(List<String> arguments) {
		this.arguments = arguments;
	}

	public static RestoreModifier replace() {
		return REPLACE_MODIFIER;
	}

	public static RestoreModifier absttl() {
		return ABSTTL_MODIFIER;
	}

	public static RestoreModifier idletime(long seconds) {
		return new RestoreModifier(asList(IDLETIME, String.valueOf(seconds)));
	}

	public static RestoreModifier idletime(Duration time) {
		return idletime(time.getSeconds());
	}

	public static RestoreModifier freq(long frequency) {
		return new RestoreModifier(asList(FREQ, String.valueOf(frequency)));
	}

	public List<String> getArguments() {
		return arguments;
	}
}
