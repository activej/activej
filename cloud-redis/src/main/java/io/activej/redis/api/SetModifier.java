package io.activej.redis.api;

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
