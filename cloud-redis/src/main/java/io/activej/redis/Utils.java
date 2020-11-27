package io.activej.redis;

import io.activej.csp.AbstractChannelSupplier;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelSupplierTransformer;
import io.activej.promise.Promise;
import io.activej.redis.api.*;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;
import static io.activej.redis.RedisConnection.UNEXPECTED_NIL;
import static io.activej.redis.api.GeoradiusModifier.*;
import static io.activej.redis.api.MigrateModifier.KEYS;
import static io.activej.redis.api.SetModifier.*;
import static io.activej.redis.api.ZaddModifier.GT;
import static io.activej.redis.api.ZaddModifier.LT;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	// Keywords
	static final String OK = "OK";
	static final String WEIGHTS = "WEIGHTS";
	static final String AGGREGATE = "AGGREGATE";
	static final String WITHSCORES = "WITHSCORES";
	static final String LIMIT = "LIMIT";
	static final String QUEUED = "QUEUED";
	static final String ASYNC = "ASYNC";
	static final String NOKEY = "NOKEY";

	static final String EMPTY_KEY = "";
	public static final String ZERO_CURSOR = "0";


	@SafeVarargs
	static <T> List<T> list(T main, T... otherArguments) {
		List<T> result = new ArrayList<>(otherArguments.length + 1);
		result.add(main);
		Collections.addAll(result, otherArguments);
		return result;
	}

	@SafeVarargs
	public static <T> List<T> list(T first, T second, T... otherArguments) {
		List<T> result = new ArrayList<>(otherArguments.length + 2);
		result.add(first);
		result.add(second);
		Collections.addAll(result, otherArguments);
		return result;
	}

	@SafeVarargs
	static <T> List<T> list(T first, T second, T third, T... otherArguments) {
		List<T> result = new ArrayList<>(otherArguments.length + 3);
		result.add(first);
		result.add(second);
		result.add(third);
		Collections.addAll(result, otherArguments);
		return result;
	}

	static <T> List<T> list(T first, T[] otherArguments, T nextArgument) {
		List<T> result = new ArrayList<>(otherArguments.length + 3);
		result.add(first);
		Collections.addAll(result, otherArguments);
		result.add(nextArgument);
		return result;
	}

	static int deepHashCode(List<?> array) {
		int result = 1;
		for (Object o : array) {
			if (o == null) result = 31 * result + 1;
			else if (o instanceof byte[]) result = 31 * result + Arrays.hashCode((byte[]) o);
			else if (o instanceof List) result = 31 * result + deepHashCode((List<?>) o);
			else result = 31 * result + o.hashCode();
		}
		return result;
	}

	static boolean deepEquals(@Nullable List<?> thisArray, @Nullable List<?> thatArray) {
		if (thisArray == thatArray) return true;
		if (thisArray == null || thatArray == null) return false;

		if (thisArray.size() != thatArray.size()) return false;

		Iterator<?> thisIterator = thisArray.iterator();
		Iterator<?> thatIterator = thatArray.iterator();

		while (thisIterator.hasNext()) {
			Object nextThis = thisIterator.next();
			Object nextThat = thatIterator.next();

			if (nextThis == nextThat) continue;
			if (nextThis == null || nextThat == null) return false;

			if (nextThis instanceof List && nextThat instanceof List) {
				if (!deepEquals((List<?>) nextThis, (List<?>) nextThat)) return false;
				continue;
			}

			if (nextThis.getClass() != nextThat.getClass()) return false;

			if (nextThis instanceof byte[]) {
				if (!Arrays.equals((byte[]) nextThis, (byte[]) nextThat)) return false;
				continue;
			}


			if (!nextThis.equals(nextThat)) return false;
		}

		return !thatIterator.hasNext();
	}

	static void checkSetModifiers(SetModifier... modifiers) {
		Set<String> modifierTypes = new HashSet<>(modifiers.length);
		for (SetModifier modifier : modifiers) {
			String modifierType = modifier.getArguments().get(0);
			if (!modifierTypes.add(modifierType)) {
				throw new IllegalArgumentException("Multiple '" + modifierType + "' modifiers");
			}
		}

		checkConflictingModifiers(modifierTypes, type -> EX.equals(type) || PX.equals(type) || KEEPTTL.equals(type));
		checkConflictingModifiers(modifierTypes, type -> NX.equals(type) || XX.equals(type));
	}

	static void checkLposModifiers(LposModifier... modifiers) {
		checkArgument(modifiers.length <= 2, () -> "Too many modifiers: " +
				Arrays.stream(modifiers)
						.map(LposModifier::getArguments)
						.flatMap(Collection::stream)
						.collect(Collectors.joining(" ")));

		if (modifiers.length == 2) {
			String firstModifier = modifiers[0].getArguments().get(0);
			String secondModifier = modifiers[1].getArguments().get(0);
			checkArgument(!firstModifier.equals(secondModifier), "Same modifier is set twice: '", firstModifier + '\'');
		}
	}

	static void checkZaddModifiers(ZaddModifier... modifiers) {
		Set<String> modifierTypes = new HashSet<>(modifiers.length);
		for (ZaddModifier modifier : modifiers) {
			String modifierType = modifier.getArgument();
			if (!modifierTypes.add(modifierType)) {
				throw new IllegalArgumentException("Multiple '" + modifierType + "' modifiers");
			}
		}

		checkConflictingModifiers(modifierTypes, type -> GT.equals(type) || LT.equals(type) || NX.equals(type));
		checkConflictingModifiers(modifierTypes, type -> NX.equals(type) || XX.equals(type));
	}

	static void checkGeoradiusModifiers(boolean readOnly, GeoradiusModifier... modifiers) {
		Set<String> modifierTypes = new HashSet<>(modifiers.length);
		for (GeoradiusModifier modifier : modifiers) {
			String modifierType = modifier.getArguments().get(0);
			if (!modifierTypes.add(modifierType)) {
				throw new IllegalArgumentException("Multiple '" + modifierType + "' modifiers");
			}
		}

		checkConflictingModifiers(modifierTypes, type -> ASC.equals(type) || DESC.equals(type));
		if (readOnly && (modifierTypes.contains(STORE) || modifierTypes.contains(STOREDIST))) {
			throw new IllegalArgumentException("Cannot use STORE or STOREDIST modifiers in Read-Only mode");
		}
		if (!readOnly)
			if (!modifierTypes.contains(STORE) && !modifierTypes.contains(STOREDIST)) {
				throw new IllegalArgumentException("If you do not use STORE or STOREDIST, than use 'Read-Only' version of the command");
			}
		if (modifierTypes.contains(WITHHASH) || modifierTypes.contains(WITHCOORD) || modifierTypes.contains(WITHDIST)) {
			throw new IllegalArgumentException("Cannot use WITHHASH, WITHCOORD or WITHDIST modifiers together with STORE or STOREDIR");
		}
	}

	static void checkRestoreModifiers(RestoreModifier... modifiers) {
		Set<String> modifierTypes = new HashSet<>(modifiers.length);
		for (RestoreModifier modifier : modifiers) {
			String modifierType = modifier.getArguments().get(0);
			if (!modifierTypes.add(modifierType)) {
				throw new IllegalArgumentException("Multiple '" + modifierType + "' modifiers");
			}
		}
	}

	static void checkSortModifiers(SortModifier... modifiers) {
		Set<String> modifierTypes = new HashSet<>(modifiers.length);
		for (SortModifier modifier : modifiers) {
			String modifierType = modifier.getArguments().get(0);
			if (!modifierTypes.add(modifierType)) {
				if (!GET.equals(modifierType)) {
					throw new IllegalArgumentException("Multiple '" + modifierType + "' modifiers");
				}
			}
		}
		checkConflictingModifiers(modifierTypes, type -> ASC.equals(type) || DESC.equals(type));
	}

	static void checkMigrateModifiers(boolean withKeys, MigrateModifier... modifiers) {
		Set<String> modifierTypes = new HashSet<>(modifiers.length);
		boolean keysSeen = false;
		for (MigrateModifier modifier : modifiers) {
			String modifierType = modifier.getArguments().get(0);
			if (KEYS.equals(modifierType)) {
				if (!withKeys) {
					throw new IllegalArgumentException("Cannot use KEYS modifier when specifying a key directly, " +
							"use version of 'migrate(...)' without an explicit key");
				}
				keysSeen = true;
			}
			if (!modifierTypes.add(modifierType)) {
				throw new IllegalArgumentException("Multiple '" + modifierType + "' modifiers");
			}
		}
		if (withKeys && !keysSeen) {
			throw new IllegalArgumentException("KEYS modifier should be passed if using version of 'migrate(...)' " +
					"without an explicit key");
		}
	}

	static void checkScanModifiers(ScanModifier... modifiers) {
		Set<String> modifierTypes = new HashSet<>(modifiers.length);
		for (ScanModifier modifier : modifiers) {
			String modifierType = modifier.getArguments().get(0);
			if (!modifierTypes.add(modifierType)) {
				throw new IllegalArgumentException("Multiple '" + modifierType + "' modifiers");
			}
		}
	}

	static void checkCursor(String cursor) {
		try {
			// 64-bit unsigned
			BigInteger bigInteger = new BigInteger(cursor);
			if (bigInteger.signum() == -1) {
				throw new IllegalArgumentException("Cursor cannot be a negative number");
			}
			if (bigInteger.bitLength() > 64) {
				throw new IllegalArgumentException("Cursor must be a 64 bit number");
			}
		} catch (NumberFormatException ignored) {
			throw new IllegalArgumentException("Cursor is not a valid integer");
		}
	}

	private static void checkConflictingModifiers(Set<String> modifierTypes, Predicate<String> conflictingTypesPredicate) {
		if (modifierTypes.stream().filter(conflictingTypesPredicate).count() > 1) {
			throw new IllegalArgumentException("Conflicting modifiers: " + String.join(", ", modifierTypes));
		}
	}

	public static final class ScanChannelSupplier extends AbstractChannelSupplier<byte[]> {
		private final Function<String, Promise<ScanResult>> scanFn;

		@Nullable
		private String cursor = ZERO_CURSOR;
		private Iterator<byte[]> iterator = Collections.emptyIterator();

		public ScanChannelSupplier(Function<String, Promise<ScanResult>> scanFn) {
			this.scanFn = scanFn;
		}

		@Override
		protected Promise<byte[]> doGet() {
			if (iterator.hasNext()) {
				return Promise.of(iterator.next());
			}
			if (cursor == null) {
				close();
				return Promise.of(null);
			}
			return scanFn.apply(cursor)
					.then(result -> {
						String resultCursor = result.getCursor();
						cursor = ZERO_CURSOR.equals(resultCursor) ? null : resultCursor;
						iterator = result.getElementsAsBinary().iterator();
						return get();
					});
		}
	}

	public static final class MapTransformer<K, V> implements ChannelSupplierTransformer<byte[], ChannelSupplier<Map.Entry<K, V>>> {
		private final RedisFunction<byte[], K> keyFn;
		private final RedisFunction<byte[], V> valueFn;

		public MapTransformer(RedisFunction<byte[], K> keyFn, RedisFunction<byte[], V> valueFn) {
			this.keyFn = keyFn;
			this.valueFn = valueFn;
		}

		@Override
		public ChannelSupplier<Map.Entry<K, V>> transform(ChannelSupplier<byte[]> supplier) {
			return new AbstractChannelSupplier<Map.Entry<K, V>>(supplier) {
				@Override
				protected Promise<Map.Entry<K, V>> doGet() {
					return supplier.get()
							.then(keyBytes -> {
								if (keyBytes == null) return Promise.of(null);
								return apply(keyBytes, keyFn)
										.then(key -> supplier.get()
												.then(valueBytes -> {
													if (valueBytes == null) {
														closeEx(UNEXPECTED_NIL);
														return Promise.ofException(UNEXPECTED_NIL);
													}
													return apply(valueBytes, valueFn)
															.map(value -> new SimpleEntry<>(key, value));
												}));
							});
				}

				private <T> Promise<T> apply(byte[] keyBytes, RedisFunction<byte[], T> fn) {
					try {
						return Promise.of(fn.apply(keyBytes));
					} catch (RedisException e) {
						closeEx(e);
						return Promise.ofException(e);
					}
				}
			};
		}
	}

	interface RedisFunction<T, R> {
		R apply(T item) throws RedisException;
	}

}
