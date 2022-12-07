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

package io.activej.crdt.util;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;
import com.dslplatform.json.runtime.Settings;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.crdt.messaging.CrdtRequest;
import io.activej.crdt.messaging.CrdtResponse;
import io.activej.crdt.messaging.Version;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamFilter;
import io.activej.datastream.processor.StreamTransformer;
import io.activej.fs.util.StructuredStreamCodec;
import io.activej.promise.Promise;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.serializer.stream.StreamCodecs.SubtypeBuilder;
import io.activej.types.TypeT;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static io.activej.crdt.wal.FileWriteAheadLog.EXT_FINAL;
import static java.util.stream.Collectors.toList;

public final class Utils {
	private static final StreamCodec<Version> VERSION_CODEC = StructuredStreamCodec.create(Version::new,
			Version::major, StreamCodecs.ofVarInt(),
			Version::minor, StreamCodecs.ofVarInt()
	);

	public static final StreamCodec<CrdtRequest> CRDT_REQUEST_CODEC = createCrdtRequestStreamCodec();
	public static final StreamCodec<CrdtResponse> CRDT_RESPONSE_CODEC = createCrdtResponseStreamCodec();

	public static Promise<List<Path>> getWalFiles(Executor executor, Path walDir) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Stream<Path> list = Files.list(walDir)) {
						return list
								.filter(file -> Files.isRegularFile(file) && file.toString().endsWith(EXT_FINAL))
								.collect(toList());
					}
				});
	}

	public static Promise<Void> deleteWalFiles(Executor executor, Collection<Path> walFiles) {
		return Promise.ofBlocking(executor, () -> {
			for (Path walFile : walFiles) {
				Files.deleteIfExists(walFile);
			}
		});
	}

	// region JSON
	private static final DslJson<?> DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());
	private static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(DSL_JSON::newWriter);
	private static final ThreadLocal<JsonReader<?>> READERS = ThreadLocal.withInitial(DSL_JSON::newReader);

	public static <T> ByteBuf toJson(@NotNull Type manifest, @Nullable T object) {
		if (object == null) return ByteBuf.wrap(new byte[]{'n', 'u', 'l', 'l'}, 0, 4);
		JsonWriter jsonWriter = WRITERS.get();
		jsonWriter.reset();
		if (!DSL_JSON.serialize(jsonWriter, manifest, object)) {
			throw new IllegalArgumentException("Cannot serialize " + manifest);
		}
		return ByteBuf.wrapForReading(jsonWriter.toByteArray());
	}

	public static <T> T fromJson(@NotNull Type manifest, @NotNull ByteBuf buf) throws MalformedDataException {
		return fromJson(manifest, buf.getArray());
	}

	public static <T> T fromJson(@NotNull TypeT<T> typeT, byte[] bytes) throws MalformedDataException {
		return fromJson(typeT.getType(), bytes);
	}

	public static <T> T fromJson(@NotNull Type manifest, byte[] bytes) throws MalformedDataException {
		try {
			//noinspection unchecked
			JsonReader.ReadObject<T> readObject = (JsonReader.ReadObject<T>) DSL_JSON.tryFindReader(manifest);
			if (readObject == null) {
				throw new IllegalArgumentException("Unknown type: " + manifest);
			}
			JsonReader<?> jsonReader = READERS.get().process(bytes, bytes.length);
			jsonReader.getNextToken();
			T deserialized = readObject.read(jsonReader);
			if (jsonReader.length() != jsonReader.getCurrentIndex()) {
				String unexpectedData = jsonReader.toString().substring(jsonReader.getCurrentIndex());
				throw new MalformedDataException("Unexpected JSON data: " + unexpectedData);
			}
			return deserialized;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}
	// endregion

	public static <T> StreamTransformer<T, T> ackTransformer(UnaryOperator<Promise<Void>> ackFn) {
		return new StreamAckTransformer<>(ackFn);
	}

	public static <T> StreamTransformer<T, T> onItem(Runnable consumer) {
		return new StreamFilter<>() {
			@Override
			protected @NotNull StreamDataAcceptor<T> onResumed(@NotNull StreamDataAcceptor<T> output) {
				return item -> {
					consumer.run();
					output.accept(item);
				};
			}
		};
	}

	private static StreamCodec<CrdtRequest> createCrdtRequestStreamCodec() {
		SubtypeBuilder<CrdtRequest> builder = new SubtypeBuilder<>();

		builder.add(CrdtRequest.Download.class, StructuredStreamCodec.create(CrdtRequest.Download::new,
				CrdtRequest.Download::token, StreamCodecs.ofVarLong())
		);
		builder.add(CrdtRequest.Handshake.class, StructuredStreamCodec.create(CrdtRequest.Handshake::new,
				CrdtRequest.Handshake::version, VERSION_CODEC)
		);
		builder.add(CrdtRequest.Ping.class, StreamCodecs.singleton(new CrdtRequest.Ping()));
		builder.add(CrdtRequest.Take.class, StreamCodecs.singleton(new CrdtRequest.Take()));
		builder.add(CrdtRequest.TakeAck.class, StreamCodecs.singleton(new CrdtRequest.TakeAck()));
		builder.add(CrdtRequest.Upload.class, StreamCodecs.singleton(new CrdtRequest.Upload()));

		return builder.build();
	}

	private static StreamCodec<CrdtResponse> createCrdtResponseStreamCodec() {
		SubtypeBuilder<CrdtResponse> builder = new SubtypeBuilder<>();

		builder.add(CrdtResponse.DownloadStarted.class, StreamCodecs.singleton(new CrdtResponse.DownloadStarted()));
		builder.add(CrdtResponse.Handshake.class, StructuredStreamCodec.create(CrdtResponse.Handshake::new,
				CrdtResponse.Handshake::handshakeFailure, StreamCodecs.ofNullable(
						StructuredStreamCodec.create(CrdtResponse.HandshakeFailure::new,
								CrdtResponse.HandshakeFailure::minimalVersion, VERSION_CODEC,
								CrdtResponse.HandshakeFailure::message, StreamCodecs.ofString())
				))
		);
		builder.add(CrdtResponse.Pong.class, StreamCodecs.singleton(new CrdtResponse.Pong()));
		builder.add(CrdtResponse.RemoveAck.class, StreamCodecs.singleton(new CrdtResponse.RemoveAck()));
		builder.add(CrdtResponse.ServerError.class, StructuredStreamCodec.create(CrdtResponse.ServerError::new,
				CrdtResponse.ServerError::message, StreamCodecs.ofString()
				)
		);
		builder.add(CrdtResponse.TakeStarted.class, StreamCodecs.singleton(new CrdtResponse.TakeStarted()));
		builder.add(CrdtResponse.UploadAck.class, StreamCodecs.singleton(new CrdtResponse.UploadAck()));

		return builder.build();
	}
}
