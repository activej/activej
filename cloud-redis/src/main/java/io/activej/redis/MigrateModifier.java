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

public final class MigrateModifier {
	public static final String COPY = "COPY";
	public static final String REPLACE = "REPLACE";
	public static final String AUTH = "AUTH";
	public static final String AUTH2 = "AUTH2";
	public static final String KEYS = "KEYS";

	private static final MigrateModifier COPY_MODIFIER = new MigrateModifier(singletonList(COPY));
	private static final MigrateModifier REPLACE_MODIFIER = new MigrateModifier(singletonList(REPLACE));

	private final List<String> arguments;

	private MigrateModifier(List<String> arguments) {
		this.arguments = arguments;
	}

	public static MigrateModifier copy() {
		return COPY_MODIFIER;
	}

	public static MigrateModifier replace() {
		return REPLACE_MODIFIER;
	}

	public static MigrateModifier auth(String password) {
		return new MigrateModifier(asList(AUTH, password));
	}

	public static MigrateModifier auth2(String username, String password) {
		return new MigrateModifier(asList(AUTH2, password));
	}

	public static MigrateModifier keys(String key, String... otherKeys) {
		return new MigrateModifier(Utils.list(KEYS, key, otherKeys));
	}

	public List<String> getArguments() {
		return arguments;
	}
}
