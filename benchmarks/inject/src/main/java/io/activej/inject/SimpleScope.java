//package io.activej.inject;
//
//import com.google.common.collect.Maps;
//import com.google.inject.Key;
//import com.google.inject.OutOfScopeException;
//import com.google.inject.Provider;
//import com.google.inject.Scopes;
//
//import java.util.Map;
//
//import static com.google.common.base.Preconditions.checkState;
//
//public class SimpleScope implements com.google.inject.Scope {
//	private static final Provider<Object> SEEDED_KEY_PROVIDER =
//		() -> {
//			throw new IllegalStateException(
//				"If you got here then it means that" +
//				" your code asked for scoped object which should have been" +
//				" explicitly seeded in this scope by calling" +
//				" SimpleScope.seed(), but was not.");
//		};
//	private static final ThreadLocal<Map<Key<?>, Object>> THREAD_LOCAL = new ThreadLocal<>();
//
//	public static void enter() {
//		checkState(THREAD_LOCAL.get() == null, "A scoping block is already in progress");
//		THREAD_LOCAL.set(Maps.newHashMap());
//	}
//
//	public static void exit() {
//		checkState(THREAD_LOCAL.get() != null, "No scoping block in progress");
//		THREAD_LOCAL.remove();
//	}
//
//	public <T> void seed(Key<T> key, T value) {
//		Map<Key<?>, Object> scopedObjects = getScopedObjectMap(key);
//		checkState(!scopedObjects.containsKey(key),
//			"A value for the key %s was " +
//			"already seeded in this scope. Old value: %s New value: %s", key,
//			scopedObjects.get(key), value);
//		scopedObjects.put(key, value);
//	}
//
//	public <T> void seed(Class<T> clazz, T value) {
//		seed(Key.get(clazz), value);
//	}
//
//	@Override
//	public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
//		return () -> {
//			Map<Key<?>, Object> scopedObjects = getScopedObjectMap(key);
//
//			@SuppressWarnings("unchecked")
//			T current = (T) scopedObjects.get(key);
//			if (current == null && !scopedObjects.containsKey(key)) {
//				current = unscoped.get();
//
//				// don't remember proxies; these exist only to serve circular dependencies
//				if (Scopes.isCircularProxy(current)) {
//					return current;
//				}
//
//				scopedObjects.put(key, current);
//			}
//			return current;
//		};
//	}
//
//	private <T> Map<Key<?>, Object> getScopedObjectMap(Key<T> key) {
//		Map<Key<?>, Object> scopedObjects = THREAD_LOCAL.get();
//		if (scopedObjects == null) {
//			throw new OutOfScopeException(
//				"Cannot access " + key
//				+ " outside of a scoping block");
//		}
//		return scopedObjects;
//	}
//
//	/**
//	 * Returns a provider that always throws exception complaining that the object
//	 * in question must be seeded before it can be injected.
//	 *
//	 * @return typed provider
//	 */
//	@SuppressWarnings("unchecked")
//	public static <T> Provider<T> seededKeyProvider() {
//		return (Provider<T>) SEEDED_KEY_PROVIDER;
//	}
//}
