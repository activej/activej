package io.activej.types;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.HashMap;
import java.util.Map;

import static io.activej.types.Types.*;

/**
 * Various utility methods for type inference
 */
public class IsAssignableUtils {

	/**
	 * Tests whether a {@code from} type is assignable to {@code to} type
	 *
	 * @param to   a 'to' type that should be checked for possible assignment
	 * @param from a 'from' type that should be checked for possible assignment
	 * @return whether an object of type {@code from} is assignable to an object of type {@code to}
	 */
	public static boolean isAssignable(@NotNull Type to, @NotNull Type from) {
		// shortcut
		if (to instanceof Class && from instanceof Class) return ((Class<?>) to).isAssignableFrom((Class<?>) from);
		return isAssignable(to, from, new HashMap<>(), false);
	}

	private static boolean isAssignable(Type to, Type from, Map<Type, Type> dejaVu, boolean strict) {
		if (from.equals(to) || dejaVu.get(from) == to) {
			return true;
		}
		dejaVu.put(from, to);
		try {
			if (to instanceof WildcardType || from instanceof WildcardType) {
				Type[] toUppers, toLowers;
				if (to instanceof WildcardType) {
					WildcardType wildcardTo = (WildcardType) to;
					toUppers = wildcardTo.getUpperBounds();
					toLowers = wildcardTo.getLowerBounds();
				} else {
					toUppers = new Type[]{to};
					toLowers = strict ? toUppers : NO_TYPES;
				}

				Type[] fromUppers, fromLowers;
				if (from instanceof WildcardType) {
					WildcardType wildcardTo = (WildcardType) from;
					fromUppers = wildcardTo.getUpperBounds();
					fromLowers = wildcardTo.getLowerBounds();
				} else {
					fromUppers = new Type[]{from};
					fromLowers = strict ? fromUppers : NO_TYPES;
				}

				for (Type toUpper : toUppers) {
					for (Type fromUpper : fromUppers) {
						if (!isAssignable(toUpper, fromUpper, dejaVu, false)) return false;
					}
				}
				if (toLowers.length == 0) return true;
				if (fromLowers.length == 0) return false;
				for (Type toLower : toLowers) {
					for (Type fromLower : fromLowers) {
						if (!isAssignable(fromLower, toLower, dejaVu, false)) return false;
					}
				}
				return true;
			}
			if (to instanceof GenericArrayType) to = getRawType(to);
			if (from instanceof GenericArrayType) from = getRawType(from);
			if (to instanceof TypeVariable) {
				for (Type bound : ((TypeVariable<?>) to).getBounds()) {
					if (!isAssignable(bound, from, dejaVu, false)) return false;
				}
				return true;
			}
			if (from instanceof TypeVariable) {
				for (Type bound : ((TypeVariable<?>) from).getBounds()) {
					if (!isAssignable(bound, to, dejaVu, false)) return false;
				}
				return true;
			}
			if (!strict && to instanceof Class) {
				return ((Class<?>) to).isAssignableFrom(getRawType(from));
			}
			Class<?> toRawClazz = getRawType(to);
			Type[] toTypeArguments = getActualTypeArguments(to);
			return isAssignable(toRawClazz, toTypeArguments, from, dejaVu, strict);
		} finally {
			dejaVu.remove(from);
		}
	}

	private static boolean isAssignable(Class<?> toRawClazz, Type[] toTypeArguments, Type from, Map<Type, Type> dejaVu, boolean strict) {
		Class<?> fromRawClazz = getRawType(from);
		if (strict && !toRawClazz.equals(fromRawClazz)) return false;
		if (!strict && !toRawClazz.isAssignableFrom(fromRawClazz)) return false;
		if (toRawClazz.isArray()) return true;
		Type[] fromTypeArguments = getActualTypeArguments(from);
		if (toRawClazz == fromRawClazz) {
			if (toTypeArguments.length > fromTypeArguments.length) return false;
			for (int i = 0; i < toTypeArguments.length; i++) {
				if (!isAssignable(toTypeArguments[i], fromTypeArguments[i], dejaVu, true)) return false;
			}
			return true;
		}
		Map<TypeVariable<?>, Type> typeBindings = getTypeBindings(from);
		for (Type anInterface : fromRawClazz.getGenericInterfaces()) {
			if (isAssignable(toRawClazz, toTypeArguments, bind(anInterface, key -> typeBindings.getOrDefault(key, Types.wildcardTypeAny())), dejaVu, false)) {
				return true;
			}
		}
		Type superclass = fromRawClazz.getGenericSuperclass();
		return superclass != null && isAssignable(toRawClazz, toTypeArguments, bind(superclass, typeBindings), dejaVu, false);
	}

}
