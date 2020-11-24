package io.activej.redis.api;

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

	public GeoradiusModifier(List<String> arguments) {
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
