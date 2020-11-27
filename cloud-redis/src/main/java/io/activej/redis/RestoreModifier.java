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
