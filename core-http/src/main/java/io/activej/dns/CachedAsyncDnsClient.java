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
import io.activej.dns.DnsCache.DnsQueryCacheResult;
import io.activej.dns.protocol.DnsQuery;
import io.activej.dns.protocol.DnsQueryException;
import io.activej.dns.protocol.DnsResponse;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.activej.common.Checks.checkState;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;

/**
 * Implementation of {@link AsyncDnsClient} that asynchronously
 * connects to some DNS server and gets the response from it.
 */
public final class CachedAsyncDnsClient implements AsyncDnsClient, EventloopJmxBean {
	private final Logger logger = LoggerFactory.getLogger(CachedAsyncDnsClient.class);
	private static final boolean CHECK = Checks.isEnabled(CachedAsyncDnsClient.class);

	private final Eventloop eventloop;
	private final AsyncDnsClient client;

	private DnsCache cache;
	private final Map<DnsQuery, Promise<DnsResponse>> pending = new HashMap<>();
	private final Set<DnsQuery> refreshingNow = Collections.newSetFromMap(new ConcurrentHashMap<>());

	private CachedAsyncDnsClient(Eventloop eventloop, AsyncDnsClient client, DnsCache cache) {
		this.eventloop = eventloop;
		this.client = client;
		this.cache = cache;
	}

	public static CachedAsyncDnsClient create(Eventloop eventloop, AsyncDnsClient client, DnsCache cache) {
		return new CachedAsyncDnsClient(eventloop, client, cache);
	}

	public static CachedAsyncDnsClient create(Eventloop eventloop, AsyncDnsClient client) {
		return new CachedAsyncDnsClient(eventloop, client, DnsCache.create(eventloop));
	}

	public CachedAsyncDnsClient withCache(DnsCache cache) {
		this.cache = cache;
		return this;
	}

	public CachedAsyncDnsClient withExpiration(Duration errorCacheExpiration, Duration hardExpirationDelta) {
		return withExpiration(errorCacheExpiration, hardExpirationDelta, DnsCache.DEFAULT_TIMED_OUT_EXPIRATION);
	}

	public CachedAsyncDnsClient withExpiration(Duration errorCacheExpiration, Duration hardExpirationDelta, Duration timedOutExceptionTtl) {
		cache
				.withErrorCacheExpiration(errorCacheExpiration)
				.withHardExpirationDelta(hardExpirationDelta)
				.withTimedOutExpiration(timedOutExceptionTtl);
		return this;
	}

	public AsyncDnsClient adaptToAnotherEventloop(Eventloop anotherEventloop) {
		if (anotherEventloop == eventloop) {
			return this;
		}
		return new AsyncDnsClient() {
			@Override
			public Promise<DnsResponse> resolve(DnsQuery query) {
				if (CHECK) checkState(anotherEventloop.inEventloopThread());
				DnsResponse fromQuery = AsyncDnsClient.resolveFromQuery(query);
				if (fromQuery != null) {
					logger.trace("{} already contained an IP address within itself", query);
					return Promise.of(fromQuery);
				}

				DnsQueryCacheResult cacheResult = cache.tryToResolve(query);
				if (cacheResult != null) {
					if (cacheResult.doesNeedRefreshing() && !refreshingNow.add(query)) {
						eventloop.execute(() -> refresh(query));
					}
					return cacheResult.getResponseAsPromise();
				}

				anotherEventloop.startExternalTask(); // keep other eventloop alive while we wait for an answer in main one
				return Promise.ofCallback(cb ->
						eventloop.execute(() ->
								CachedAsyncDnsClient.this.resolve(query)
										.run((result, e) -> {
											anotherEventloop.execute(wrapContext(cb, () -> cb.accept(result, e)));
											anotherEventloop.completeExternalTask();
										})));
			}

			@Override
			public void close() {
				if (CHECK) checkState(anotherEventloop.inEventloopThread());
				eventloop.execute(CachedAsyncDnsClient.this::close);
			}
		};
	}

	private void addToCache(DnsQuery query, DnsResponse response, @Nullable Exception e) {
		if (e == null) {
			cache.add(query, response);
		} else if (e instanceof DnsQueryException) {
			cache.add(query, ((DnsQueryException) e).getResult());
		}
	}

	private void refresh(DnsQuery query) {
		if (!refreshingNow.add(query)) {
			logger.trace("{} needs refreshing, but it does so right now", query);
			return;
		}
		logger.trace("Refreshing {}", query);
		client.resolve(query)
				.run((response, e) -> {
					addToCache(query, response, e);
					refreshingNow.remove(query);
				});
	}

	@Override
	public Promise<DnsResponse> resolve(DnsQuery query) {
		if (CHECK) {
			checkState(eventloop.inEventloopThread(), "Concurrent resolves are not allowed, to reuse the cache use adaptToOtherEventloop");
		}

		DnsResponse fromQuery = AsyncDnsClient.resolveFromQuery(query);
		if (fromQuery != null) {
			logger.trace("{} already contained an IP address within itself", query);
			return Promise.of(fromQuery);
		}

		logger.trace("Resolving {}", query);
		DnsQueryCacheResult cacheResult = cache.tryToResolve(query);
		if (cacheResult != null) {
			if (cacheResult.doesNeedRefreshing()) {
				refresh(query);
			}
			return cacheResult.getResponseAsPromise();
		}
		cache.performCleanup();
		Promise<DnsResponse> promise = pending.get(query);
		if (promise != null) return promise;
		Promise<DnsResponse> resolve = client.resolve(query);
		resolve.run((response, e) -> addToCache(query, response, e));
		if (resolve.isComplete()) return resolve;
		pending.put(query, resolve);
		return resolve.whenComplete(() -> pending.remove(query));
	}

	@Override
	public void close() {
		client.close();
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute(name = "")
	public DnsCache getCache() {
		return cache;
	}

	@JmxOperation
	public List<String> getResolvedDomains() {
		return cache.getResolvedDomains();
	}

	@JmxOperation
	public List<String> getFailedDomains() {
		return cache.getFailedDomains();
	}

	@JmxOperation
	public List<String> getDomainRecord(String domain) {
		return cache.getDomainRecord(domain);
	}

	@JmxOperation
	public List<String> getDomainRecords() {
		return cache.getDomainRecords();
	}

	@JmxOperation
	public void clear() {
		cache.clear();
	}

}
