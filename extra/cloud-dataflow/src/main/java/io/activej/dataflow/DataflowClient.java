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

package io.activej.dataflow;

import io.activej.async.process.AsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.exception.UnknownFormatException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.dsl.ChannelTransformer;
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.dataflow.exception.DataflowException;
import io.activej.dataflow.exception.DataflowStacklessException;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse.Handshake.NotOk;
import io.activej.dataflow.proto.serializer.CustomNodeSerializer;
import io.activej.dataflow.proto.serializer.CustomStreamSchemaSerializer;
import io.activej.dataflow.proto.serializer.FunctionSerializer;
import io.activej.datastream.AbstractStreamConsumer;
import io.activej.datastream.AbstractStreamSupplier;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.processor.StreamSupplierTransformer;
import io.activej.eventloop.net.SocketSettings;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

import static io.activej.dataflow.proto.serializer.ProtobufUtils.convert;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Client for datagraph server.
 * Sends JSON commands for performing certain actions on server.
 */
public final class DataflowClient {
	private static final Logger logger = getLogger(DataflowClient.class);

	private final SocketSettings socketSettings = SocketSettings.createDefault();

	private final ByteBufsCodec<DataflowResponse, DataflowRequest> codec;
	private final BinarySerializerLocator serializers;
	private final FunctionSerializer functionSerializer;

	private @Nullable CustomNodeSerializer customNodeSerializer;
	private @Nullable CustomStreamSchemaSerializer customStreamSchemaSerializer;

	private DataflowClient(ByteBufsCodec<DataflowResponse, DataflowRequest> codec, BinarySerializerLocator serializers, FunctionSerializer functionSerializer) {
		this.codec = codec;
		this.serializers = serializers;
		this.functionSerializer = functionSerializer;
	}

	public static DataflowClient create(ByteBufsCodec<DataflowResponse, DataflowRequest> codec, BinarySerializerLocator serializers, FunctionSerializer functionSerializer) {
		return new DataflowClient(codec, serializers, functionSerializer);
	}

	public DataflowClient withCustomNodeSerializer(CustomNodeSerializer customNodeSerializer) {
		this.customNodeSerializer = customNodeSerializer;
		return this;
	}

	public DataflowClient withCustomStreamSchemaSerializer(CustomStreamSchemaSerializer customStreamSchemaSerializer) {
		this.customStreamSchemaSerializer = customStreamSchemaSerializer;
		return this;
	}

	public <T> StreamSupplier<T> download(InetSocketAddress address, StreamId streamId, StreamSchema<T> streamSchema, ChannelTransformer<ByteBuf, ByteBuf> transformer) {
		return StreamSupplier.ofPromise(AsyncTcpSocketNio.connect(address, 0, socketSettings)
				.mapException(IOException.class, e -> new DataflowStacklessException("Failed to connect to " + address, e))
				.then(socket -> {
					Messaging<DataflowResponse, DataflowRequest> messaging = MessagingWithBinaryStreaming.create(socket, codec);
					return performHandshake(messaging)
							.then(() -> messaging.send(downloadRequest(streamId))
									.mapException(IOException.class, e -> new DataflowException("Failed to download from " + address, e)))
							.map($ -> messaging.receiveBinaryStream()
									.transformWith(transformer)
									.transformWith(ChannelDeserializer.create(streamSchema.createSerializer(serializers))
											.withExplicitEndOfStream())
									.transformWith(new StreamTraceCounter<>(streamId, address))
									.withEndOfStream(eos -> eos
											.mapException(e -> (e instanceof IOException ||
													e instanceof UnknownFormatException ||
													e instanceof TruncatedDataException) ?
													new DataflowStacklessException("Error when downloading from " + address, e) :
													new DataflowException("Error when downloading from " + address, e))
											.whenComplete(messaging::close)));
				}));
	}

	public <T> StreamSupplier<T> download(InetSocketAddress address, StreamId streamId, StreamSchema<T> streamSchema) {
		return download(address, streamId, streamSchema, ChannelTransformer.identity());
	}

	private static class StreamTraceCounter<T> implements StreamSupplierTransformer<T, StreamSupplier<T>> {
		private final StreamId streamId;
		private final InetSocketAddress address;
		private int count = 0;
		private final Input input;
		private final Output output;

		private StreamTraceCounter(StreamId streamId, InetSocketAddress address) {
			this.streamId = streamId;
			this.address = address;
			this.input = new Input();
			this.output = new Output();

			input.getAcknowledgement()
					.whenException(output::closeEx);
			output.getAcknowledgement()
					.whenResult(input::acknowledge)
					.whenException(input::closeEx);
		}

		@Override
		public StreamSupplier<T> transform(StreamSupplier<T> supplier) {
			supplier.streamTo(input);
			return output;
		}

		private final class Input extends AbstractStreamConsumer<T> {
			@Override
			protected void onEndOfStream() {
				output.sendEndOfStream();
			}

			@Override
			protected void onComplete() {
				logger.info("Received {} items total from stream {}({})", count, streamId, address);
			}
		}

		private final class Output extends AbstractStreamSupplier<T> {
			@Override
			protected void onResumed() {
				StreamDataAcceptor<T> dataAcceptor = getDataAcceptor();
				assert dataAcceptor != null;
				input.resume(item -> {
					if (++count == 1 || count % 1_000 == 0) {
						logger.info("Received {} items from stream {}({}): {}", count, streamId, address, item);
					}
					dataAcceptor.accept(item);
				});
			}

			@Override
			protected void onSuspended() {
				input.suspend();
			}
		}
	}

	public class Session implements AsyncCloseable {
		private final InetSocketAddress address;
		private final Messaging<DataflowResponse, DataflowRequest> messaging;

		private Session(InetSocketAddress address, AsyncTcpSocketNio socket) {
			this.address = address;
			this.messaging = MessagingWithBinaryStreaming.create(socket, codec);
		}

		public Promise<Void> execute(long taskId, Collection<Node> nodes) {
			return performHandshake(messaging)
					.then(() -> messaging.send(executeRequest(taskId, nodes))
							.mapException(IOException.class, e -> new DataflowStacklessException("Failed to send command to " + address, e)))
					.then(() -> messaging.receive()
							.mapException(IOException.class, e -> new DataflowStacklessException("Failed to receive response from " + address, e)))
					.whenResult(response -> {
						messaging.close();
						switch (response.getResponseCase()) {
							case RESULT -> {
								DataflowResponse.Error error = response.getResult().getError();
								if (!error.getErrorIsNull()) {
									throw new DataflowException("Error on remote server " + address + ": " + error.getError());
								}
							}
							case RESPONSE_NOT_SET -> throw new DataflowException("Server did not set a response");
							default -> throw new DataflowException("Bad response from server");
						}
					})
					.toVoid();
		}

		@Override
		public void closeEx(@NotNull Exception e) {
			messaging.closeEx(e);
		}
	}

	public Promise<Session> connect(InetSocketAddress address) {
		return AsyncTcpSocketNio.connect(address, 0, socketSettings)
				.map(socket -> new Session(address, socket))
				.mapException(e -> new DataflowException("Could not connect to " + address, e));
	}

	private static DataflowRequest downloadRequest(StreamId streamId) {
		return DataflowRequest.newBuilder()
				.setDownload(DataflowRequest.Download.newBuilder()
						.setStreamId(convert(streamId)))
				.build();
	}

	private DataflowRequest executeRequest(long taskId, Collection<Node> nodes) {
		return DataflowRequest.newBuilder()
				.setExecute(DataflowRequest.Execute.newBuilder()
						.setTaskId(taskId)
						.addAllNodes(convert(nodes, functionSerializer, customNodeSerializer, customStreamSchemaSerializer)))
				.build();
	}

	public static Promise<Void> performHandshake(Messaging<DataflowResponse, DataflowRequest> messaging) {
		return messaging.send(DataflowRequest.newBuilder().setHandshake(DataflowRequest.Handshake.newBuilder().setVersion(DataflowServer.VERSION)).build())
				.then(messaging::receive)
				.mapException(IOException.class, DataflowStacklessException::new)
				.whenResult(handshakeResponse -> {
					if (!handshakeResponse.hasHandshake()) {
						throw new DataflowException("Handshake response expected, got " + handshakeResponse.getResponseCase());
					}
					DataflowResponse.Handshake handshake = handshakeResponse.getHandshake();
					if (handshake.hasNotOk()) {
						NotOk notOk = handshake.getNotOk();
						throw new DataflowException(String.format("Handshake failed: %s. Minimal allowed version: %s",
								notOk.getMessage(), notOk.hasMinimalVersion() ? notOk.getMinimalVersion() : "unspecified"));
					}
				})
				.toVoid();
	}
}
