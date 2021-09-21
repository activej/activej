/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.cube;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.initializer.WithInitializer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CubeClassLoaderCache implements CubeClassLoaderCacheMBean, WithInitializer<CubeClassLoaderCache> {
	static final class Key {
		final Set<String> attributes;
		final Set<String> measures;
		final Set<String> filterDimensions;

		Key(Set<String> attributes, Set<String> measures, Set<String> filterDimensions) {
			this.attributes = attributes;
			this.measures = measures;
			this.filterDimensions = filterDimensions;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Key that = (Key) o;
			return attributes.equals(that.attributes) &&
					measures.equals(that.measures) &&
					filterDimensions.equals(that.filterDimensions);
		}

		@Override
		public int hashCode() {
			int result = attributes.hashCode();
			result = 31 * result + measures.hashCode();
			result = 31 * result + filterDimensions.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "{" + attributes + ", " + measures + ", " + filterDimensions + '}';
		}
	}

	private final DefiningClassLoader rootClassLoader;
	private final LinkedHashMap<Key, DefiningClassLoader> cache = new LinkedHashMap<Key, DefiningClassLoader>(16, 0.75f, false) {
		@Override
		protected boolean removeEldestEntry(Map.Entry<Key, DefiningClassLoader> eldest) {
			return size() > targetCacheKeys;
		}
	};

	private int targetCacheKeys;

	// JMX
	private int cacheRequests;
	private int cacheMisses;

	private CubeClassLoaderCache(DefiningClassLoader rootClassLoader, int targetCacheKeys) {
		this.rootClassLoader = rootClassLoader;
		this.targetCacheKeys = targetCacheKeys;
	}

	public static CubeClassLoaderCache create(DefiningClassLoader root, int cacheSize) {
		return new CubeClassLoaderCache(root, cacheSize);
	}

	public synchronized DefiningClassLoader getOrCreate(Key key) {
		cacheRequests++;
		return cache.computeIfAbsent(key, $ -> {
			cacheMisses++;
			return DefiningClassLoader.create(rootClassLoader);
		});

	}

	// JMX
	@Override
	public synchronized void clear() {
		cache.clear();
	}

	@Override
	public int getTargetCacheKeys() {
		return targetCacheKeys;
	}

	@Override
	public void setTargetCacheKeys(int targetCacheKeys) {
		this.targetCacheKeys = targetCacheKeys;
	}

	@Override
	public synchronized int getDefinedClassesCount() {
		int result = 0;
		for (DefiningClassLoader classLoader : cache.values()) {
			result += classLoader.getCachedClassesCount();
		}
		return result;
	}

	@Override
	public synchronized int getDefinedClassesCountMaxPerKey() {
		int result = 0;
		for (DefiningClassLoader classLoader : cache.values()) {
			result = Math.max(result, classLoader.getCachedClassesCount());
		}
		return result;
	}

	@Override
	public int getCacheKeys() {
		return cache.size();
	}

	@Override
	public int getCacheRequests() {
		return cacheRequests;
	}

	@Override
	public int getCacheMisses() {
		return cacheMisses;
	}

	@Override
	public Map<String, String> getCacheContents() {
		Map<String, String> map;
		synchronized (this) {
			map = new LinkedHashMap<>(cache.size());
		}

		for (Map.Entry<Key, DefiningClassLoader> entry : cache.entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().toString());
		}

		return map;
	}
}
