package io.activej.redis;

import io.activej.redis.api.GeoradiusModifier;
import io.activej.redis.api.LposModifier;
import io.activej.redis.api.SetModifier;
import io.activej.redis.api.ZaddModifier;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;
import static io.activej.redis.api.GeoradiusModifier.*;
import static io.activej.redis.api.SetModifier.*;
import static io.activej.redis.api.ZaddModifier.GT;
import static io.activej.redis.api.ZaddModifier.LT;

final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	static final String WEIGHTS = "WEIGHTS";
	static final String AGGREGATE = "AGGREGATE";
	static final String WITHSCORES = "WITHSCORES";
	static final String LIMIT = "LIMIT";

	@SafeVarargs
	static <T> List<T> list(T main, T... otherArguments) {
		List<T> result = new ArrayList<>(otherArguments.length + 1);
		result.add(main);
		Collections.addAll(result, otherArguments);
		return result;
	}

	@SafeVarargs
	static <T> List<T> list(T first, T second, T... otherArguments) {
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

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
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
			if (!modifierTypes.contains(STORE) && !modifierTypes.contains(STOREDIST)){
				throw new IllegalArgumentException("If you do not use STORE or STOREDIST, than use 'Read-Only' version of the command");
			}
			if (modifierTypes.contains(WITHHASH) || modifierTypes.contains(WITHCOORD) || modifierTypes.contains(WITHDIST)) {
				throw new IllegalArgumentException("Cannot use WITHHASH, WITHCOORD or WITHDIST modifiers together with STORE or STOREDIR");
			}
	}

	private static void checkConflictingModifiers(Set<String> modifierTypes, Predicate<String> conflictingTypesPredicate) {
		if (modifierTypes.stream().filter(conflictingTypesPredicate).count() > 1) {
			throw new IllegalArgumentException("Conflicting modifiers: " + String.join(", ", modifierTypes));
		}
	}
}
