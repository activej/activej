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

import io.activej.common.initializer.WithInitializer;
import io.activej.dns.DnsCache.DnsQueryCacheResult;
import io.activej.dns.protocol.DnsQuery;
import io.activej.dns.protocol.DnsQueryException;
import io.activej.dns.protocol.DnsResponse;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBean;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.activej.reactor.util.RunnableWithContext.wrapContext;

/**
 * Implementation of {@link AsyncDnsClient} that asynchronously
 * connects to some DNS server and gets the response from it.
 */
public final class DnsClient_Cached extends AbstractReactive
		implements AsyncDnsClient, ReactiveJmxBean, WithInitializer<DnsClient_Cached> {
	private final Logger logger = LoggerFactory.getLogger(DnsClient_Cached.class);

	private final AsyncDnsClient client;

	private DnsCache cache;
	private final Map<DnsQuery, Promise<DnsResponse>> pending = new HashMap<>();
	private final Set<DnsQuery> refreshingNow = Collections.newSetFromMap(new ConcurrentHashMap<>());

	private DnsClient_Cached(Reactor reactor, AsyncDnsClient client, DnsCache cache) {
		super(reactor);
		this.client = client;
		this.cache = cache;
	}

	public static DnsClient_Cached create(Reactor reactor, AsyncDnsClient client, DnsCache cache) {
		return new DnsClient_Cached(reactor, client, cache);
	}

	public static DnsClient_Cached create(Reactor reactor, AsyncDnsClient client) {
		return new DnsClient_Cached(reactor, client, DnsCache.create(reactor));
	}

	public DnsClient_Cached withCache(DnsCache cache) {
		this.cache = cache;
		return this;
	}

	public DnsClient_Cached withExpiration(Duration errorCacheExpiration, Duration hardExpirationDelta) {
		return withExpiration(errorCacheExpiration, hardExpirationDelta, DnsCache.DEFAULT_TIMED_OUT_EXPIRATION);
	}

	public DnsClient_Cached withExpiration(Duration errorCacheExpiration, Duration hardExpirationDelta, Duration timedOutExceptionTtl) {
		cache
				.withErrorCacheExpiration(errorCacheExpiration)
				.withHardExpirationDelta(hardExpirationDelta)
				.withTimedOutExpiration(timedOutExceptionTtl);
		return this;
	}

	public AsyncDnsClient adaptToAnotherReactor(Reactor anotherReactor) {
		if (anotherReactor == reactor) {
			return this;
		}
		return new AsyncDnsClient() {
			@Override
			public Promise<DnsResponse> resolve(DnsQuery query) {
				anotherReactor.checkInReactorThread();
				DnsResponse fromQuery = AsyncDnsClient.resolveFromQuery(query);
				if (fromQuery != null) {
					logger.trace("{} already contained an IP address within itself", query);
					return Promise.of(fromQuery);
				}

				DnsQueryCacheResult cacheResult = cache.tryToResolve(query);
				if (cacheResult != null) {
					if (cacheResult.doesNeedRefreshing() && !refreshingNow.add(query)) {
						reactor.execute(() -> refresh(query));
					}
					return cacheResult.getResponseAsPromise();
				}

				anotherReactor.startExternalTask(); // keep other reactor alive while we wait for an answer in main one
				return Promise.ofCallback(cb ->
						reactor.execute(() ->
								DnsClient_Cached.this.resolve(query)
										.run((result, e) -> {
											anotherReactor.execute(wrapContext(cb, () -> cb.accept(result, e)));
											anotherReactor.completeExternalTask();
										})));
			}

			@Override
			public void close() {
				anotherReactor.checkInReactorThread();
				reactor.execute(DnsClient_Cached.this::close);
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
		checkInReactorThread();
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
