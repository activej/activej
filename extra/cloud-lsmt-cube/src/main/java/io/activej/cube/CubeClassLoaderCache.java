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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CubeClassLoaderCache implements CubeClassLoaderCacheMBean {
	record Key(Set<String> attributes, Set<String> measures, Set<String> filterDimensions) {}

	private final DefiningClassLoader rootClassLoader;
	private final LinkedHashMap<Key, DefiningClassLoader> cache = new LinkedHashMap<>(16, 0.75f, false) {
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
