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

package io.activej.dns;

import io.activej.common.Check;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.activej.common.Preconditions.checkState;

/**
 * Represents a cache for storing resolved domains during its time to live.
 */
public final class DnsCache {
	private static final Logger logger = LoggerFactory.getLogger(DnsCache.class);
	private static final Boolean CHECK = Check.isEnabled(DnsCache.class);

	public static final Duration DEFAULT_TIMED_OUT_EXCEPTION_TTL = Duration.ofSeconds(1);
	public static final Duration DEFAULT_ERROR_CACHE_EXPIRATION = Duration.ofMinutes(1);
	public static final Duration DEFAULT_HARD_EXPIRATION_DELTA = Duration.ofMinutes(1);

	private final Map<DnsQuery, CachedDnsQueryResult> cache = new ConcurrentHashMap<>();
	private final Eventloop eventloop;

	private long errorCacheExpiration = DEFAULT_ERROR_CACHE_EXPIRATION.toMillis();
	private long timedOutExceptionTtl = DEFAULT_TIMED_OUT_EXCEPTION_TTL.toMillis();
	private long hardExpirationDelta = DEFAULT_HARD_EXPIRATION_DELTA.toMillis();
	private long maxTtl = Long.MAX_VALUE;

	private final AtomicBoolean cleaningUpNow = new AtomicBoolean(false);
	private final PriorityQueue<CachedDnsQueryResult> expirations = new PriorityQueue<>();

	@NotNull
	CurrentTimeProvider now;

	/**
	 * Creates a new DNS cache.
	 *
	 * @param eventloop eventloop
	 */
	private DnsCache(@NotNull Eventloop eventloop) {
		this.eventloop = eventloop;
		this.now = eventloop;
	}

	public static DnsCache create(Eventloop eventloop) {
		return new DnsCache(eventloop);
	}

	/**
	 * @param errorCacheExpiration expiration time for errors without time to live
	 */
	public DnsCache withErrorCacheExpirationSeconds(Duration errorCacheExpiration) {
		this.errorCacheExpiration = errorCacheExpiration.toMillis();
		return this;
	}

	/**
	 * @param hardExpirationDelta delta between time at which entry is considered resolved, but needs
	 */
	public DnsCache withHardExpirationDelta(Duration hardExpirationDelta) {
		this.hardExpirationDelta = hardExpirationDelta.toMillis();
		return this;
	}

	/**
	 * @param timedOutExceptionTtl expiration time for timed out exception
	 */
	public DnsCache withTimedOutExceptionTtl(Duration timedOutExceptionTtl) {
		this.timedOutExceptionTtl = timedOutExceptionTtl.toMillis();
		return this;
	}

	/**
	 * Tries to get status of the entry for some query from the cache.
	 *
	 * @param query DNS query
	 * @return DnsQueryCacheResult for this query
	 */
	@Nullable
	public DnsQueryCacheResult tryToResolve(DnsQuery query) {
		CachedDnsQueryResult cachedResult = cache.get(query);

		if (cachedResult == null) {
			logger.trace("{} cache miss", query);
			return null;
		}

		DnsResponse result = cachedResult.response;
		assert result != null; // results with null responses should never be in cache map
		if (result.isSuccessful()) {
			logger.trace("{} cache hit", query);
		} else {
			logger.trace("{} error cache hit", query);
		}

		if (isExpired(cachedResult)) {
			logger.trace("{} hard TTL expired", query);
			return null;
		} else if (isSoftExpired(cachedResult)) {
			logger.trace("{} soft TTL expired", query);
			return new DnsQueryCacheResult(result, true);
		}
		return new DnsQueryCacheResult(result, false);
	}

	private boolean isExpired(CachedDnsQueryResult cachedResult) {
		return now.currentTimeMillis() >= cachedResult.expirationTime + hardExpirationDelta;
	}

	private boolean isSoftExpired(CachedDnsQueryResult cachedResult) {
		return now.currentTimeMillis() >= cachedResult.expirationTime;
	}

	/**
	 * Adds DnsResponse to this cache
	 *
	 * @param response response to add
	 */
	public void add(DnsQuery query, DnsResponse response) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Concurrent cache adds are not allowed");
		long expirationTime = now.currentTimeMillis();
		if (response.isSuccessful()) {
			assert response.getRecord() != null; // where are my advanced contracts so that the IDE would know it's true here without an assert?
			long minTtl = response.getRecord().getMinTtl() * 1000;
			if (minTtl == 0) {
				return;
			}
			expirationTime += Math.min(minTtl, maxTtl);
		} else {
			expirationTime += response.getErrorCode() == DnsProtocol.ResponseErrorCode.TIMED_OUT ?
					timedOutExceptionTtl :
					errorCacheExpiration;
		}
		CachedDnsQueryResult cachedResult = new CachedDnsQueryResult(response, expirationTime);
		CachedDnsQueryResult old = cache.put(query, cachedResult);
		expirations.add(cachedResult);

		if (old != null) {
			old.response = null; // mark old cache response as refreshed (see performCleanup)
			logger.trace("Refreshed cache entry for {}", query);
		} else {
			logger.trace("Added cache entry for {}", query);
		}
	}

	public void performCleanup() {
		if (!cleaningUpNow.compareAndSet(false, true)) {
			return;
		}
		long currentTime = now.currentTimeMillis();

		CachedDnsQueryResult peeked;
		while ((peeked = expirations.peek()) != null && peeked.expirationTime <= currentTime) {
			DnsResponse response = peeked.response;
			if (response != null) { // if it was not refreshed(so there is a newer response in the queue)
				DnsQuery query = response.getTransaction().getQuery();
				cache.remove(query); // we drop it from cache
				logger.trace("Cache entry expired for {}", query);
			}
			expirations.poll();
		}
		cleaningUpNow.set(false);
	}

	public long getMaxTtl() {
		return maxTtl;
	}

	public void setMaxTtl(Duration maxTtl) {
		this.maxTtl = maxTtl.getSeconds();
	}

	public long getTimedOutExceptionTtl() {
		return timedOutExceptionTtl;
	}

	public void clear() {
		if (CHECK) checkState(eventloop.inEventloopThread());
		cache.clear();
		expirations.clear();
	}

	public int getNumberOfCachedDomainNames() {
		return cache.size();
	}

	public int getNumberOfCachedExceptions() {
		return (int) cache.values().stream()
				.filter(cachedResult -> {
					assert cachedResult.response != null;
					return !cachedResult.response.isSuccessful();
				})
				.count();
	}

	public String[] getSuccessfullyResolvedDomainNames() {
		return cache.entrySet()
				.stream()
				.filter(entry -> {
					assert entry.getValue().response != null;
					return entry.getValue().response.isSuccessful();
				})
				.map(Entry::getKey)
				.map(DnsQuery::getDomainName)
				.toArray(String[]::new);
	}

	public String[] getDomainNamesOfFailedRequests() {
		return cache.entrySet()
				.stream()
				.filter(entry -> {
					assert entry.getValue().response != null;
					return !entry.getValue().response.isSuccessful();
				})
				.map(Entry::getKey)
				.map(DnsQuery::getDomainName)
				.toArray(String[]::new);
	}

	public String[] getAllCacheEntriesWithHeaderLine() {
		if (cache.isEmpty()) {
			return new String[0];
		}

		List<String> cacheEntries = new ArrayList<>();
		StringBuilder sb = new StringBuilder();

		cacheEntries.add("domainName;ips;secondsToSoftExpiration;secondsToHardExpiration;status");
		cache.forEach((domainName, cachedResult) -> {
			long softExpirationSecond = cachedResult.expirationTime;
			long hardExpirationSecond = softExpirationSecond + hardExpirationDelta;
			long currentSecond = now.currentTimeMillis();
			long secondsToSoftExpiration = softExpirationSecond - currentSecond;
			long secondsToHardExpiration = hardExpirationSecond - currentSecond;
			DnsResponse result = cachedResult.response;
			//noinspection ConstantConditions - for getRecord() != null after isSuccessful() check
			cacheEntries.add(sb
					.append(domainName)
					.append(";")
					.append(result.isSuccessful() ? Arrays.toString(result.getRecord().getIps()) : "[]")
					.append(";")
					.append(secondsToSoftExpiration <= 0 ? "expired" : secondsToSoftExpiration)
					.append(";")
					.append(secondsToHardExpiration <= 0 ? "expired" : secondsToHardExpiration)
					.append(";")
					.append(result.getErrorCode())
					.toString());
			sb.setLength(0);
		});

		return cacheEntries.toArray(new String[0]);
	}

	public static final class DnsQueryCacheResult {
		private final DnsResponse response;
		private final boolean needsRefreshing;

		public DnsQueryCacheResult(DnsResponse response, boolean needsRefreshing) {
			this.response = response;
			this.needsRefreshing = needsRefreshing;
		}

		public Promise<DnsResponse> getResponseAsPromise() {
			if (response.getErrorCode() == DnsProtocol.ResponseErrorCode.NO_ERROR) {
				return Promise.of(response);
			}
			return Promise.ofException(new DnsQueryException(DnsCache.class, response));
		}

		public boolean doesNeedRefreshing() {
			return needsRefreshing;
		}
	}

	static final class CachedDnsQueryResult implements Comparable<CachedDnsQueryResult> {
		@Nullable
		DnsResponse response;
		final long expirationTime;

		CachedDnsQueryResult(@Nullable DnsResponse response, long expirationTime) {
			this.response = response;
			this.expirationTime = expirationTime;
		}

		@Override
		public int compareTo(CachedDnsQueryResult o) {
			return Long.compare(expirationTime, o.expirationTime);
		}
	}
}
