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

import io.activej.common.Checks;
import io.activej.common.StringFormatUtils;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.dns.protocol.DnsProtocol.ResponseErrorCode;
import io.activej.dns.protocol.DnsQuery;
import io.activej.dns.protocol.DnsQueryException;
import io.activej.dns.protocol.DnsResponse;
import io.activej.eventloop.Eventloop;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkState;

/**
 * Represents a cache for storing resolved domains during its time to live.
 */
public final class DnsCache {
	private static final Logger logger = LoggerFactory.getLogger(DnsCache.class);
	private static final boolean CHECK = Checks.isEnabled(DnsCache.class);

	public static final Duration DEFAULT_ERROR_CACHE_EXPIRATION = Duration.ofMinutes(1);
	public static final Duration DEFAULT_TIMED_OUT_EXPIRATION = Duration.ofSeconds(1);
	public static final Duration DEFAULT_HARD_EXPIRATION_DELTA = Duration.ofMinutes(1);
	public static final Duration DEFAULT_MAX_TTL = null;

	private final Map<DnsQuery, CachedDnsQueryResult> cache = new ConcurrentHashMap<>();
	private final Eventloop eventloop;

	private long errorCacheExpiration = DEFAULT_ERROR_CACHE_EXPIRATION.toMillis();
	private long timedOutExpiration = DEFAULT_TIMED_OUT_EXPIRATION.toMillis();
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
	public DnsCache withErrorCacheExpiration(Duration errorCacheExpiration) {
		this.errorCacheExpiration = errorCacheExpiration.toMillis();
		return this;
	}

	/**
	 * @param timedOutExpiration expiration time for timed out exception
	 */
	public DnsCache withTimedOutExpiration(Duration timedOutExpiration) {
		this.timedOutExpiration = timedOutExpiration.toMillis();
		return this;
	}

	/**
	 * @param hardExpirationDelta delta between time at which entry is considered resolved, but needs
	 */
	public DnsCache withHardExpirationDelta(Duration hardExpirationDelta) {
		this.hardExpirationDelta = hardExpirationDelta.toMillis();
		return this;
	}

	public DnsCache withMaxTtl(@Nullable Duration maxTtl) {
		this.maxTtl = maxTtl == null ? Long.MAX_VALUE : maxTtl.toMillis();
		return this;
	}

	/**
	 * Tries to get status of the entry for some query from the cache.
	 *
	 * @param query DNS query
	 * @return DnsQueryCacheResult for this query
	 */
	@Nullable
	public DnsCache.DnsQueryCacheResult tryToResolve(DnsQuery query) {
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
			expirationTime += response.getErrorCode() == ResponseErrorCode.TIMED_OUT ?
					timedOutExpiration :
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

	@JmxAttribute
	public Duration getErrorCacheExpiration() {
		return Duration.ofMillis(errorCacheExpiration);
	}

	@JmxAttribute
	public void setErrorCacheExpiration(Duration errorCacheExpiration) {
		this.errorCacheExpiration = errorCacheExpiration.toMillis();
	}

	@JmxAttribute
	public Duration getTimedOutExpiration() {
		return Duration.ofMillis(timedOutExpiration);
	}

	@JmxAttribute
	public void setTimedOutExpiration(Duration timedOutExpiration) {
		this.timedOutExpiration = timedOutExpiration.toMillis();
	}

	@JmxAttribute
	public Duration getHardExpirationDelta() {
		return Duration.ofMillis(hardExpirationDelta);
	}

	@JmxAttribute
	public void setHardExpirationDelta(Duration hardExpirationDelta) {
		this.hardExpirationDelta = hardExpirationDelta.toMillis();
	}

	@JmxAttribute
	public String getMaxTtl() {
		return maxTtl == Long.MAX_VALUE ? "" : StringFormatUtils.formatDuration(Duration.ofMillis(maxTtl));
	}

	@JmxAttribute
	public void setMaxTtl(String s) {
		if (s == null || s.isEmpty()) {
			maxTtl = Long.MAX_VALUE;
		} else {
			maxTtl = StringFormatUtils.parseDuration(s).toMillis();
		}
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getDomainsCount() {
		return cache.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getFailedDomainsCount() {
		return (int) cache.values().stream()
				.filter(cachedResult -> {
					assert cachedResult.response != null;
					return !cachedResult.response.isSuccessful();
				})
				.count();
	}

	@JmxOperation
	public List<String> getResolvedDomains() {
		return getDomainNames(DnsResponse::isSuccessful);
	}

	@JmxOperation
	public List<String> getFailedDomains() {
		return getDomainNames(response -> !response.isSuccessful());
	}

	private List<String> getDomainNames(Predicate<DnsResponse> predicate) {
		return cache.entrySet()
				.stream()
				.filter(entry -> predicate.test(entry.getValue().response))
				.map(Entry::getKey)
				.map(DnsQuery::getDomainName)
				.collect(Collectors.toList());
	}

	private class RecordFormatter {
		final String domain;
		final CachedDnsQueryResult result;

		RecordFormatter(String domain, CachedDnsQueryResult result) {
			this.domain = domain;
			this.result = result;
		}

		ResponseErrorCode getStatus() {
			if (result.response == null)
				return ResponseErrorCode.UNKNOWN;
			return result.response.getErrorCode();
		}

		Collection<InetAddress> getIps() {
			if (result.response == null || result.response.getRecord() == null)
				return Collections.emptyList();
			return Arrays.asList(result.response.getRecord().getIps());
		}

		int getMinTtlSeconds() {
			if (result.response == null || result.response.getRecord() == null)
				return 0;
			return result.response.getRecord().getMinTtl();
		}

		String getSecondsToSoftExpiration() {
			long secs = (result.expirationTime - now.currentTimeMillis()) / 1000;
			return formatExpired(secs);
		}

		String getSecondsToHardExpiration() {
			long secs = (result.expirationTime + hardExpirationDelta - now.currentTimeMillis()) / 1000;
			return formatExpired(secs);
		}

		private String formatExpired(long secs) {
			return secs < 0 ? "expired" : Long.toString(secs);
		}

		public List<String> formatMultiline() {
			List<String> lines = new ArrayList<>();
			lines.add("DomainName:\t" + domain);
			lines.add("Status:\t" + getStatus());
			lines.add("IP:\t" + getIps());
			lines.add("MinTtlSeconds:\t" + getMinTtlSeconds());
			lines.add("SecondsToSoftExpiration:\t" + getSecondsToSoftExpiration());
			lines.add("SecondsToHardExpiration:\t" + getSecondsToHardExpiration());
			return lines;
		}
	}

	@JmxOperation
	public List<String> getDomainRecord(String domain) {
		Optional<Entry<DnsQuery, CachedDnsQueryResult>> first = cache.entrySet().stream().filter(e -> e.getKey().getDomainName().equalsIgnoreCase(domain)).findFirst();
		if (!first.isPresent())
			return Collections.emptyList();
		return new RecordFormatter(first.get().getKey().getDomainName(), first.get().getValue()).formatMultiline();
	}

	@JmxOperation
	public List<String> getDomainRecords() {
		if (cache.isEmpty())
			return Collections.emptyList();

		List<String> lines = new ArrayList<>(cache.size());
		lines.add("DomainName;Status;IP;MinTtlSeconds;SecondsToSoftExpiration;SecondsToHardExpiration");
		StringBuilder sb = new StringBuilder();
		cache.forEach((domainName, cachedResult) -> {
			RecordFormatter formatter = new RecordFormatter(domainName.getDomainName(), cachedResult);
			lines.add(sb
					.append(formatter.domain)
					.append(";")
					.append(formatter.getStatus())
					.append(";")
					.append(formatter.getIps())
					.append(";")
					.append(formatter.getMinTtlSeconds())
					.append(";")
					.append(formatter.getSecondsToSoftExpiration())
					.append(";")
					.append(formatter.getSecondsToHardExpiration())
					.toString());
			sb.setLength(0);
		});
		return lines;
	}

	@JmxOperation
	public void clear() {
		cache.clear();
		eventloop.submit(expirations::clear);
	}

	public static final class DnsQueryCacheResult {
		private final DnsResponse response;
		private final boolean needsRefreshing;

		public DnsQueryCacheResult(DnsResponse response, boolean needsRefreshing) {
			this.response = response;
			this.needsRefreshing = needsRefreshing;
		}

		public Promise<DnsResponse> getResponseAsPromise() {
			if (response.getErrorCode() == ResponseErrorCode.NO_ERROR) {
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
