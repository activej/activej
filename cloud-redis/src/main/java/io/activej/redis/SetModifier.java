package io.activej.redis;

import java.time.Duration;
import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class SetModifier {
	private static final String EX = "EX";
	private static final String PX = "PX";

	private static final SetModifier NX = new SetModifier(singletonList("NX"));
	private static final SetModifier XX = new SetModifier(singletonList("XX"));
	private static final SetModifier KEEPTTL = new SetModifier(singletonList("KEEPTTL"));
	private static final SetModifier GET = new SetModifier(singletonList("GET"));

	private final List<String> arguments;

	public SetModifier(List<String> arguments) {
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
		return NX;
	}

	public static SetModifier setIfExists(){
		return XX;
	}

	public static SetModifier retainTTL(){
		return KEEPTTL;
	}

	public static SetModifier returnOldValue(){
		return GET;
	}

	List<String> getArguments() {
		return arguments;
	}
}
