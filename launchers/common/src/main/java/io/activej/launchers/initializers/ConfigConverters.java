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

import io.activej.common.annotation.StaticFactories;
import io.activej.config.Config;
import io.activej.config.converter.ConfigConverter;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.csp.process.frame.impl.LZ4;
import io.activej.csp.process.frame.impl.LZ4Legacy;
import io.activej.dns.DnsCache;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.dns.DnsCache.*;

@StaticFactories(ConfigConverter.class)
public class ConfigConverters {

	public static ConfigConverter<DnsCache> ofDnsCache(Reactor reactor) {
		return new ConfigConverter<>() {
			@Override
			public DnsCache get(Config config) {
				Duration errorCacheExpiration = config.get(ofDuration(), "errorCacheExpiration", DEFAULT_ERROR_CACHE_EXPIRATION);
				Duration timedOutExceptionTtl = config.get(ofDuration(), "timedOutExpiration", DEFAULT_TIMED_OUT_EXPIRATION);
				Duration hardExpirationDelta = config.get(ofDuration(), "hardExpirationDelta", DEFAULT_HARD_EXPIRATION_DELTA);
				Duration maxTtl = config.get(ofDuration(), "maxTtl", DEFAULT_MAX_TTL);
				return DnsCache.builder(reactor)
					.withErrorCacheExpiration(errorCacheExpiration)
					.withTimedOutExpiration(timedOutExceptionTtl)
					.withHardExpirationDelta(hardExpirationDelta)
					.withMaxTtl(maxTtl)
					.build();
			}

			@Override
			@Contract("_, !null -> !null")
			public @Nullable DnsCache get(Config config, @Nullable DnsCache defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				} else {
					return get(config);
				}
			}
		};
	}

	public static ConfigConverter<FrameFormat> ofFrameFormat() {
		return new ConfigConverter<>() {
			@Override
			public FrameFormat get(Config config) {
				return doGet(config, config.getValue());
			}

			private FrameFormat doGet(Config config, String formatName) {
				return switch (formatName) {
					case "identity" -> FrameFormats.identity();
					case "size-prefixed" -> FrameFormats.sizePrefixed();
					case "lz4" -> {
						LZ4.Builder formatBuilder = LZ4.builder();
						if (config.hasChild("compressionLevel")) {
							formatBuilder.withCompressionLevel(config.get(ofInteger(), "compressionLevel"));
						}
						yield formatBuilder.build();
					}
					case "legacy-lz4" -> {
						LZ4Legacy.Builder legacyFormatBuilder = LZ4Legacy.builder();
						if (config.hasChild("compressionLevel")) {
							legacyFormatBuilder.withCompressionLevel(config.get(ofInteger(), "compressionLevel"));
						}
						if (config.hasChild("ignoreMissingEndOfStream")) {
							legacyFormatBuilder.withIgnoreMissingEndOfStream(config.get(ofBoolean(), "ignoreMissingEndOfStream"));
						}
						yield legacyFormatBuilder.build();
					}
					case "compound" -> {
						Config compoundFormatsConfig = config.getChild("compoundFormats");
						List<String> formatNames = ofList(ofString()).get(compoundFormatsConfig);
						List<FrameFormat> formats = new ArrayList<>(formatNames.size());
						for (String name : formatNames) {
							if (compoundFormatsConfig.hasChild(name)) {
								formats.add(doGet(compoundFormatsConfig.getChild(name), name));
							} else {
								formats.add(doGet(compoundFormatsConfig, name));
							}
						}
						yield FrameFormats.compound(formats.get(0), formats.subList(1, formats.size()).toArray(new FrameFormat[0]));
					}
					default ->
						throw new IllegalArgumentException("No frame format named " + config.getValue() + " exists");
				};
			}

			@Override
			public FrameFormat get(Config config, FrameFormat defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}

}
