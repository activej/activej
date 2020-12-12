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

import static io.activej.common.Checks.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class SetModifier {
	// TTL modifiers
	public static final String EX = "EX";
	public static final String PX = "PX";
	public static final String KEEPTTL = "KEEPTTL";

	// Key existence modifiers
	public static final String NX = "NX";
	public static final String XX = "XX";

	// Return modifier
	public static final String GET = "GET";

	private static final SetModifier KEEPTTL_MODIFIER = new SetModifier(singletonList(KEEPTTL));
	private static final SetModifier NX_MODIFIER = new SetModifier(singletonList(NX));
	private static final SetModifier XX_MODIFIER = new SetModifier(singletonList(XX));
	private static final SetModifier GET_MODIFIER = new SetModifier(singletonList(GET));

	private final List<String> arguments;

	private SetModifier(List<String> arguments) {
		this.arguments = arguments;
	}

	public static SetModifier expireInSeconds(long seconds) {
		checkArgument(seconds >= 0);
		return new SetModifier(asList(EX, String.valueOf(seconds)));
	}

	public static SetModifier expireInMillis(long millis) {
		checkArgument(millis >= 0);
		return new SetModifier(asList(PX, String.valueOf(millis)));
	}

	public static SetModifier expireIn(Duration expiration) {
		checkArgument(!expiration.isNegative());
		return expireInMillis(expiration.toMillis());
	}

	public static SetModifier setIfNotExists(){
		return NX_MODIFIER;
	}

	public static SetModifier setIfExists(){
		return XX_MODIFIER;
	}

	public static SetModifier retainTTL(){
		return KEEPTTL_MODIFIER;
	}

	public static SetModifier returnOldValue(){
		return GET_MODIFIER;
	}

	public List<String> getArguments() {
		return arguments;
	}
}
