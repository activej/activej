package io.activej.redis.api;

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
