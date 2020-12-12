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

public final class GeoradiusModifier {
	public static final String WITHCOORD = "WITHCOORD";
	public static final String WITHDIST = "WITHDIST";
	public static final String WITHHASH = "WITHHASH";

	public static final String COUNT = "COUNT";

	public static final String ASC = "ASC";
	public static final String DESC = "DESC";

	public static final String STORE = "STORE";
	public static final String STOREDIST = "STOREDIST";

	private static final GeoradiusModifier WITHCOORD_MODIFIER = new GeoradiusModifier(singletonList(WITHCOORD));
	private static final GeoradiusModifier WITHDIST_MODIFIER = new GeoradiusModifier(singletonList(WITHDIST));
	private static final GeoradiusModifier WITHHASH_MODIFIER = new GeoradiusModifier(singletonList(WITHHASH));
	private static final GeoradiusModifier ASC_MODIFIER = new GeoradiusModifier(singletonList(ASC));
	private static final GeoradiusModifier DESC_MODIFIER = new GeoradiusModifier(singletonList(DESC));

	private final List<String> arguments;

	private GeoradiusModifier(List<String> arguments) {
		this.arguments = arguments;
	}

	public static GeoradiusModifier withCoord() {
		return WITHCOORD_MODIFIER;
	}

	public static GeoradiusModifier withDist() {
		return WITHDIST_MODIFIER;
	}

	public static GeoradiusModifier withHash() {
		return WITHHASH_MODIFIER;
	}

	public static GeoradiusModifier asc() {
		return ASC_MODIFIER;
	}

	public static GeoradiusModifier desc() {
		return DESC_MODIFIER;
	}

	public static GeoradiusModifier count(long count) {
		return new GeoradiusModifier(asList(COUNT, String.valueOf(count)));
	}

	public static GeoradiusModifier store(String key) {
		return new GeoradiusModifier(asList(STORE, key));
	}

	public static GeoradiusModifier storeDist(String key) {
		return new GeoradiusModifier(asList(STOREDIST, key));
	}

	public List<String> getArguments() {
		return arguments;
	}
}
