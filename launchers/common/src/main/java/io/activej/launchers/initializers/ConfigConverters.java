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

package io.activej.launchers.initializers;

import io.activej.config.Config;
import io.activej.config.converter.ConfigConverter;
import io.activej.dns.DnsCache;
import io.activej.eventloop.Eventloop;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

import static io.activej.config.converter.ConfigConverters.ofDuration;
import static io.activej.dns.DnsCache.*;

public final class ConfigConverters {

	public static ConfigConverter<DnsCache> ofDnsCache(Eventloop eventloop) {
		return new ConfigConverter<DnsCache>() {
			@NotNull
			@Override
			public DnsCache get(Config config) {
				Duration errorCacheExpiration = config.get(ofDuration(), "errorCacheExpiration", DEFAULT_ERROR_CACHE_EXPIRATION);
				Duration timedOutExceptionTtl = config.get(ofDuration(), "timedOutExpiration", DEFAULT_TIMED_OUT_EXPIRATION);
				Duration hardExpirationDelta = config.get(ofDuration(), "hardExpirationDelta", DEFAULT_HARD_EXPIRATION_DELTA);
				Duration maxTtl = config.get(ofDuration(), "maxTtl", DEFAULT_MAX_TTL);
				return DnsCache.create(eventloop)
						.withErrorCacheExpiration(errorCacheExpiration)
						.withTimedOutExpiration(timedOutExceptionTtl)
						.withHardExpirationDelta(hardExpirationDelta)
						.withMaxTtl(maxTtl);
			}

			@Nullable
			@Override
			@Contract("_, !null -> !null")
			public DnsCache get(Config config, @Nullable DnsCache defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				} else {
					return get(config);
				}
			}
		};
	}

}
