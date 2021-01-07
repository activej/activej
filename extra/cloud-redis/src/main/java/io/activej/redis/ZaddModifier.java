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

public final class ZaddModifier {
	// Key existence modifiers
	public static final String NX = "NX";
	public static final String XX = "XX";

	// Update score modifiers
	public static final String LT = "LT";
	public static final String GT = "GT";

	// Return modifier
	public static final String CH = "CH";
	public static final String INCR = "INCR";

	private static final ZaddModifier NX_MODIFIER = new ZaddModifier(NX);
	private static final ZaddModifier XX_MODIFIER = new ZaddModifier(XX);
	private static final ZaddModifier LT_MODIFIER = new ZaddModifier(LT);
	private static final ZaddModifier GT_MODIFIER = new ZaddModifier(GT);
	private static final ZaddModifier CH_MODIFIER = new ZaddModifier(CH);

	private final String argument;

	private ZaddModifier(String argument) {
		this.argument = argument;
	}

	public static ZaddModifier updateIfExists() {
		return XX_MODIFIER;
	}

	public static ZaddModifier addIfNotExists() {
		return NX_MODIFIER;
	}

	public static ZaddModifier updateIfLessThanCurrent() {
		return LT_MODIFIER;
	}

	public static ZaddModifier updateIfGreaterThanCurrent() {
		return GT_MODIFIER;
	}

	public static ZaddModifier returnChangedCount() {
		return CH_MODIFIER;
	}

	public String getArgument() {
		return argument;
	}
}
