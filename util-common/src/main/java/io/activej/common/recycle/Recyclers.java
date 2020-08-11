package io.activej.common.recycle;

import io.activej.common.collection.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;

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
		register(Iterable.class, Recyclers::recycleIterable);
		register(Iterator.class, Recyclers::recycleIterator);
		register(Stream.class, stream -> stream.forEach(Recyclers::recycle));
		register(Optional.class, optional -> optional.ifPresent(Recyclers::recycle));
		register(CompletionStage.class, future -> future.thenAccept(Recyclers::recycle));
	}

	synchronized public static <T> void register(Class<T> type, Recycler<T> item) {
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

	public static void recycle(Object object) {
		if (object == null) return;
		//noinspection unchecked
		Recycler<Object> recycler = (Recycler<Object>) ensureRecycler(object.getClass());
		if (recycler != NO_RECYCLER) {
			recycler.recycle(object);
		}
	}

	public static @NotNull Recycler<?> ensureRecycler(@NotNull Class<?> type) {
		Recycler<?> recycler = CACHED_RECYCLERS.get(type);
		if (recycler != null) return recycler;
		return doCache(type);
	}

	@NotNull
	synchronized private static Recycler<?> doCache(@NotNull Class<?> type) {
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
		else if (recyclers.size() == 1) return CollectionUtils.first(recyclers.values());
		else throw new IllegalArgumentException("Conflicting recyclers for " + type + " : " + recyclers.keySet());
	}

	private static @NotNull Map<Class<?>, Recycler<?>> doLookup(@NotNull Class<?> type) {
		@Nullable Recycler<?> recycler = REGISTRY.get(type);
		if (recycler != null) return singletonMap(type, recycler);
		Map<Class<?>, Recycler<?>> map = new HashMap<>();
		@Nullable Class<?> superclass = type.getSuperclass();
		if (superclass != null) {
			map.putAll(doLookup(superclass));
		}
		for (Class<?> anInterface : type.getInterfaces()) {
			map.putAll(doLookup(anInterface));
		}
		Map<Class<?>, Recycler<?>> result = new HashMap<>();
		for (Class<?> key1 : map.keySet()) {
			boolean isAssignable = false;
			for (Class<?> key2 : map.keySet()) {
				if (key1 != key2 && key1.isAssignableFrom(key2)) {
					isAssignable = true;
					break;
				}
			}
			if (!isAssignable) result.put(key1, map.get(key1));
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

	private static void recycleIterable(Iterable<?> iterable) {
		for (Object item : iterable) {
			recycle(item);
		}
	}

	private static void recycleIterator(Iterator<?> iterator) {
		while (iterator.hasNext()) {
			recycle(iterator.next());
		}
	}
}
