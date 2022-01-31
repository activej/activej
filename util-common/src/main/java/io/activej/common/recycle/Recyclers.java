package io.activej.common.recycle;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import static io.activej.common.Utils.first;

/**
 * A registry of known {@link Recycler}s
 */
@SuppressWarnings("unchecked")
public class Recyclers {
	private static final Recycler<?> NO_RECYCLER = item -> {};

	private static final Map<Class<?>, Recycler<?>> REGISTRY = new HashMap<>();
	private static final ConcurrentHashMap<Class<?>, Recycler<?>> CACHED_RECYCLERS = new ConcurrentHashMap<>();

	static {
		register(Recyclable.class, Recyclable::recycle);
		register(AutoCloseable.class, Recyclers::recycleCloseable);
		register(List.class, Recyclers::recycleList);
		register(Map.class, Recyclers::recycleMap);
		register(Optional.class, optional -> optional.ifPresent(Recyclers::recycle));
		register(CompletionStage.class, future -> future.thenAccept(Recyclers::recycle));
	}

	/**
	 * Registers a new recycler for some type
	 *
	 * @param type a class of object that will be recycled
	 * @param item a recycler for a given type
	 * @param <T>  a type of object that will be recycled
	 * @throws IllegalStateException if a recycler for a type already exists
	 *                               and is not equal to a given recycler
	 */
	public static synchronized <T> void register(Class<T> type, Recycler<T> item) {
		REGISTRY.put(type, item);
		for (Map.Entry<Class<?>, Recycler<?>> entry : CACHED_RECYCLERS.entrySet()) {
			Class<?> cachedType = entry.getKey();
			Recycler<?> cachedRecyclerOld = entry.getValue();
			Recycler<?> cachedRecyclerNew = lookup(cachedType);
			if (!cachedRecyclerOld.equals(cachedRecyclerNew)) {
				throw new IllegalStateException("Changed recycler for " + type + " in cache entry " + cachedType);
			}
		}
	}

	/**
	 * Recycles a given object if there is a registered recycler for an object's class.
	 * Otherwise, does nothing
	 *
	 * @param object an object to be recycled
	 */
	public static void recycle(Object object) {
		if (object == null) return;
		//noinspection unchecked
		Recycler<Object> recycler = (Recycler<Object>) ensureRecycler(object.getClass());
		if (recycler != NO_RECYCLER) {
			recycler.recycle(object);
		}
	}

	/**
	 * Ensures a recycler for a given type
	 *
	 * @param type a type for which a recycler will be ensured
	 * @return an ensured recycler for a given type
	 * @throws IllegalArgumentException if there are conflicting recyclers that match given type
	 */
	public static @NotNull Recycler<?> ensureRecycler(@NotNull Class<?> type) {
		Recycler<?> recycler = CACHED_RECYCLERS.get(type);
		if (recycler != null) return recycler;
		return doCache(type);
	}

	private static synchronized @NotNull Recycler<?> doCache(@NotNull Class<?> type) {
		Recycler<?> recycler = lookup(type);
		CACHED_RECYCLERS.put(type, recycler);
		return recycler;
	}

	private static Recycler<?> lookup(@NotNull Class<?> type) {
		if (type.isArray()) {
			Class<?> componentType = type.getComponentType();
			while (componentType.isArray()) {
				componentType = componentType.getComponentType();
			}
			if (componentType.isPrimitive()) return NO_RECYCLER;
			return (Recycler<Object[]>) Recyclers::recycleArray;
		}
		Map<Class<?>, Recycler<?>> recyclers = doLookup(type);
		if (recyclers.isEmpty()) return NO_RECYCLER;
		else if (recyclers.size() == 1) return first(recyclers.values());
		else throw new IllegalArgumentException("Conflicting recyclers for " + type + " : " + recyclers.keySet());
	}

	private static @NotNull Map<Class<?>, Recycler<?>> doLookup(@NotNull Class<?> type) {
		@Nullable Recycler<?> recycler = REGISTRY.get(type);
		if (recycler != null) return Map.of(type, recycler);
		Map<Class<?>, Recycler<?>> map = new HashMap<>();
		@Nullable Class<?> superclass = type.getSuperclass();
		if (superclass != null) {
			map.putAll(doLookup(superclass));
		}
		for (Class<?> anInterface : type.getInterfaces()) {
			map.putAll(doLookup(anInterface));
		}
		Map<Class<?>, Recycler<?>> result = new HashMap<>();
		for (Map.Entry<Class<?>, Recycler<?>> entry : map.entrySet()) {
			boolean isAssignable = false;
			Class<?> key1 = entry.getKey();
			for (Class<?> key2 : map.keySet()) {
				if (key1 != key2 && key1.isAssignableFrom(key2)) {
					isAssignable = true;
					break;
				}
			}
			if (!isAssignable) result.put(key1, entry.getValue());
		}
		return result;
	}

	private static void recycleCloseable(AutoCloseable closeable) {
		try {
			closeable.close();
		} catch (Exception ignored) {
		}
	}

	private static void recycleList(List<?> list) {
		//noinspection ForLoopReplaceableByForEach
		for (int i = 0, size = list.size(); i < size; i++) {
			recycle(list.get(i));
		}
	}

	private static void recycleArray(Object[] array) {
		//noinspection ForLoopReplaceableByForEach
		for (int i = 0, length = array.length; i < length; i++) {
			recycle(array[i]);
		}
	}

	private static void recycleMap(Map<?, ?> map) {
		for (Object item : map.values()) {
			recycle(item);
		}
	}

}
