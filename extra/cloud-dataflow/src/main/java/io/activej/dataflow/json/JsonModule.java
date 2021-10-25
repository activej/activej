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

package io.activej.dataflow.json;

import com.dslplatform.json.*;
import com.dslplatform.json.JsonReader.ReadObject;
import io.activej.dataflow.command.*;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.http.LocalTaskData;
import io.activej.dataflow.http.ReducedTaskData;
import io.activej.dataflow.node.*;
import io.activej.dataflow.stats.BinaryNodeStat;
import io.activej.dataflow.stats.NodeStat;
import io.activej.dataflow.stats.TestNodeStat;
import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.DeduplicateReducer;
import io.activej.datastream.processor.StreamReducers.MergeReducer;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToOutput;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.Dependency;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.dslplatform.json.JsonWriter.*;
import static com.dslplatform.json.NumberConverter.*;
import static io.activej.dataflow.json.JsonUtils.*;
import static io.activej.types.Types.parameterizedType;
import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@SuppressWarnings({"unchecked", "rawtypes", "ConstantConditions"})
public final class JsonModule extends AbstractModule {

	private JsonModule() {
	}

	public static Module create() {
		return new JsonModule();
	}

	private static final Comparator<?> NATURAL_ORDER = Comparator.naturalOrder();
	private static final Class<?> NATURAL_ORDER_CLASS = NATURAL_ORDER.getClass();

	@QualifierAnnotation
	@Target({FIELD, PARAMETER, METHOD, ElementType.TYPE})
	@Retention(RUNTIME)
	public @interface Subtypes {
	}

	@FunctionalInterface
	public interface SubtypeNameFactory {
		@Nullable String getName(Class<?> subtype);
	}

	@Override
	protected void configure() {
		bind(codec(DataflowCommand.class));
		bind(codec(DataflowResponse.class));

		bind(codec(NodeConsumerOfId.class));
		bind(codec(NodeSupplierOfId.class));
		bind(codec(NodeUnion.class));
		bind(codec(DataflowCommandDownload.class));
		bind(codec(DataflowResponsePartitionData.class));
		bind(codec(TestNodeStat.class));
		bind(codec(BinaryNodeStat.class));

		bind(Key.ofType(parameterizedType(JsonCodec.class, NATURAL_ORDER_CLASS)))
				.toInstance(ofObject(() -> NATURAL_ORDER));

		generate(JsonCodec.class, (bindings, scope, key) -> {
			Class<Object> type = key.getTypeParameter(0).getRawType();
			if (type.isEnum()) {
				return Binding.to(() -> JsonCodec.of(
						reader -> {
							if (reader.wasNull()) return null;
							return valueOf((Class) type, reader.readString());
						},
						(writer, value) -> {
							if (value == null) {
								writer.writeNull();
							} else {
								writer.writeString(value.name());
							}
						}));
			}
			if (type == (Class<?>) Map.class || type == (Class<?>) List.class) {
				return null;
			}
			if (key.getQualifier() != Subtypes.class && !type.isAnnotationPresent(Subtypes.class)) {
				return Binding.to(() -> {
							WriteObject<Object> writer = DSL_JSON.tryFindWriter(type);
							if (writer == null) {
								throw new IllegalStateException("Cannot find serializer for " + type);
							}
							ReadObject<Object> reader = DSL_JSON.tryFindReader(type);
							if (writer == null) {
								throw new IllegalStateException("Cannot find deserializer for " + type);
							}
							return JsonCodec.of(reader, writer);
						}
				);
			}
			return Binding.to(args -> {
						Injector injector = (Injector) args[0];
						OptionalDependency<SubtypeNameFactory> maybeNames = (OptionalDependency<SubtypeNameFactory>) args[1];
						SubtypeNameFactory names = maybeNames.isPresent() ? maybeNames.get() : $ -> null;

						Set<Class<?>> subtypes = new HashSet<>();

						Injector i = injector;
						while (i != null) {
							for (Key<?> k : i.getBindings().keySet()) {
								if (k.getRawType() != JsonCodec.class) {
									continue;
								}
								Class<?> subtype = k.getTypeParameter(0).getRawType();
								if (type != subtype && type.isAssignableFrom(subtype)) {
									subtypes.add(subtype);
								}
							}
							i = i.getParent();
						}

						JsonCodecSubtype<Object> combined = JsonCodecSubtype.create();
						for (Class<?> subtype : subtypes) {
							JsonCodec<?> codec = injector.getInstance(Key.ofType(parameterizedType(JsonCodec.class, subtype)));
							String name = names.getName(subtype);
							if (name != null) {
								combined.setSubtypeCodec(subtype, name, codec);
							} else {
								combined.setSubtypeCodec(subtype, codec);
							}
						}
						return combined;
					}, new Dependency[]{
							Dependency.toKey(Key.of(Injector.class)),
							Dependency.toKey(new Key<OptionalDependency<SubtypeNameFactory>>() {})}
			);
		});
	}

	@Provides
	JsonCodec<Class<?>> classCodec() {
		return JsonCodec.of(
				reader -> {
					if (reader.wasNull()) return null;
					try {
						return Class.forName(reader.readString());
					} catch (ClassNotFoundException e) {
						throw ParsingException.create("No such class", e, true);
					}
				},
				(writer, value) -> {
					if (value == null) {
						writer.writeNull();
					} else {
						writer.writeString(value.getName());
					}
				});
	}

	@Provides
	JsonCodec<StreamId> streamId() {
		return JsonCodec.of(
				reader -> new StreamId(NumberConverter.deserializeLong(reader)),
				(writer, value) -> {
					if (value == null) {
						writer.writeNull();
					} else {
						NumberConverter.serialize(value.getId(), writer);
					}
				}
		);
	}

	@Provides
	JsonCodec<InetSocketAddress> address() {
		return JsonCodec.of(
				reader -> {
					String str = reader.readString();
					String[] split = str.split(":");
					if (split.length != 2) {
						throw ParsingException.create("Address should be split with a single ':'", true);
					}
					try {
						return new InetSocketAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1]));
					} catch (UnknownHostException e) {
						throw ParsingException.create("Failed to create InetSocketAddress", e, true);
					}
				},
				(writer, addr) -> writer.writeString(addr.getAddress().getHostAddress() + ':' + addr.getPort())
		);
	}

	private static final String TASK_ID = "taskId";
	private static final String NODE_STATS = "nodeStats";

	@Provides
	JsonCodec<DataflowCommandExecute> dataflowCommandExecute(JsonCodec<List<Node>> nodesCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					long taskId = readValue(reader, TASK_ID, LONG_READER);
					reader.comma();

					List<Node> nodes = readValue(reader, NODE_STATS, nodesCodec);
					reader.endObject();
					return new DataflowCommandExecute(taskId, nodes);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);
					writeValue(writer, TASK_ID, LONG_WRITER, value.getTaskId());
					writer.writeByte(COMMA);

					writeValue(writer, NODE_STATS, nodesCodec, value.getNodes());
					writer.writeByte(OBJECT_END);
				});
	}

	@Provides
	JsonCodec<DataflowCommandGetTasks> dataflowCommandGetTasks() {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
					Long taskId = readValue(reader, TASK_ID, LONG_READER);
					reader.endObject();
					return new DataflowCommandGetTasks(taskId);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);
					writeValue(writer, TASK_ID, LONG_WRITER, value.getTaskId());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String ERROR = "error";

	@Provides
	JsonCodec<DataflowResponseResult> dataflowResponseResult() {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
					String error = readValue(reader, ERROR, StringConverter.READER);
					reader.endObject();
					return new DataflowResponseResult(error);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);
					writeValue(writer, ERROR, StringConverter.WRITER, value.getError());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String INDEX = "index";
	private static final String TYPE = "type";
	private static final String STREAM_ID = "streamId";

	@Provides
	JsonCodec<NodeUpload> nodeUpload(JsonCodec<Class<?>> classCodec, JsonCodec<StreamId> streamIdCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, INT_READER);
					reader.comma();

					Class type = readValue(reader, TYPE, classCodec);
					reader.comma();

					StreamId streamId = readValue(reader, STREAM_ID, streamIdCodec);
					reader.endObject();

					return new NodeUpload<>(index, type, streamId);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, TYPE, classCodec, value.getType());
					writer.writeByte(COMMA);

					writeValue(writer, STREAM_ID, streamIdCodec, value.getStreamId());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String ADDRESS = "address";

	@Provides
	JsonCodec<NodeDownload> nodeDownload(JsonCodec<Class<?>> classCodec, JsonCodec<InetSocketAddress> addressCodec, JsonCodec<StreamId> streamIdCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, INT_READER);
					reader.comma();

					Class<?> type = readValue(reader, TYPE, classCodec);
					reader.comma();

					InetSocketAddress address = readValue(reader, ADDRESS, addressCodec);
					reader.comma();

					StreamId streamId = readValue(reader, STREAM_ID, streamIdCodec);
					reader.comma();

					StreamId output = readValue(reader, OUTPUT, streamIdCodec);
					reader.endObject();

					return new NodeDownload(index, type, address, streamId, output);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, TYPE, classCodec, value.getType());
					writer.writeByte(COMMA);

					writeValue(writer, ADDRESS, addressCodec, value.getAddress());
					writer.writeByte(COMMA);

					writeValue(writer, STREAM_ID, streamIdCodec, value.getStreamId());
					writer.writeByte(COMMA);

					writeValue(writer, OUTPUT, streamIdCodec, value.getOutput());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String FUNCTION = "function";

	@Provides
	JsonCodec<NodeMap> nodeMap(@Subtypes JsonCodec<Function> functionCodec, JsonCodec<StreamId> streamIdCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, NumberConverter::deserializeInt);
					reader.comma();

					Function<?, ?> function = readValue(reader, FUNCTION, functionCodec);
					reader.comma();

					StreamId input = readValue(reader, INPUT, streamIdCodec);
					reader.comma();

					StreamId output = readValue(reader, OUTPUT, streamIdCodec);
					reader.endObject();

					return new NodeMap<>(index, function, input, output);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, FUNCTION, functionCodec, value.getFunction());
					writer.writeByte(COMMA);

					writeValue(writer, INPUT, streamIdCodec, value.getInput());
					writer.writeByte(COMMA);

					writeValue(writer, OUTPUT, streamIdCodec, value.getOutput());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String REDUCER = "reducer";

	@Provides
	JsonCodec<NodeReduce.Input> nodeReduceInput(@Subtypes JsonCodec<Reducer> reducerCodec, @Subtypes JsonCodec<Function> functionCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					Reducer reducer = readValue(reader, REDUCER, reducerCodec);
					reader.comma();

					Function<?, ?> function = readValue(reader, KEY_FUNCTION, functionCodec);
					reader.endObject();

					return new NodeReduce.Input<>(reducer, function);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, REDUCER, reducerCodec, value.getReducer());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_FUNCTION, functionCodec, value.getKeyFunction());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String REDUCER_TO_RESULT = "reducerToResult";

	@Provides
	JsonCodec<InputToAccumulator> inputToAccumulator(@Subtypes JsonCodec<ReducerToResult> reducerToResultCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
					ReducerToResult reducerToResult = readValue(reader, REDUCER_TO_RESULT, reducerToResultCodec);
					reader.endObject();
					return new InputToAccumulator(reducerToResult);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);
					writeValue(writer, REDUCER_TO_RESULT, reducerToResultCodec, value.getReducerToResult());
					writer.writeByte(OBJECT_END);
				});
	}

	@Provides
	JsonCodec<InputToOutput> inputToOutput(@Subtypes JsonCodec<ReducerToResult> reducerToResultCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
					ReducerToResult reducerToResult = readValue(reader, REDUCER_TO_RESULT, reducerToResultCodec);
					reader.endObject();
					return new InputToOutput<>(reducerToResult);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);
					writeValue(writer, REDUCER_TO_RESULT, reducerToResultCodec, value.getReducerToResult());
					writer.writeByte(OBJECT_END);
				});
	}

	@Provides
	JsonCodec<AccumulatorToAccumulator> accumulatorToAccumulator(@Subtypes JsonCodec<ReducerToResult> reducerToResultCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
					ReducerToResult reducerToResult = readValue(reader, REDUCER_TO_RESULT, reducerToResultCodec);
					reader.endObject();
					return new AccumulatorToAccumulator<>(reducerToResult);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);
					writeValue(writer, REDUCER_TO_RESULT, reducerToResultCodec, value.getReducerToResult());
					writer.writeByte(OBJECT_END);
				});
	}


	@Provides
	JsonCodec<AccumulatorToOutput> accumulatorToOutput(@Subtypes JsonCodec<ReducerToResult> reducerToResultCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
					ReducerToResult reducerToResult = readValue(reader, REDUCER_TO_RESULT, reducerToResultCodec);
					reader.endObject();
					return new AccumulatorToOutput<>(reducerToResult);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);
					writeValue(writer, REDUCER_TO_RESULT, reducerToResultCodec, value.getReducerToResult());
					writer.writeByte(OBJECT_END);
				});
	}


	@Provides
	JsonCodec<DeduplicateReducer> mergeDistinctReducer() {
		return ofObject(DeduplicateReducer::new);
	}

	@Provides
	JsonCodec<MergeReducer> mergeSortReducer() {
		return ofObject(MergeReducer::new);
	}


	private static final String PREDICATE = "predicate";

	@Provides
	JsonCodec<NodeFilter> nodeFilter(@Subtypes JsonCodec<Predicate> predicateCodec, JsonCodec<StreamId> streamIdCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, NumberConverter::deserializeInt);
					reader.comma();

					Predicate<?> predicate = readValue(reader, PREDICATE, predicateCodec);
					reader.comma();

					StreamId input = readValue(reader, INPUT, streamIdCodec);
					reader.comma();

					StreamId output = readValue(reader, OUTPUT, streamIdCodec);
					reader.endObject();

					return new NodeFilter(index, predicate, input, output);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, PREDICATE, predicateCodec, value.getPredicate());
					writer.writeByte(COMMA);

					writeValue(writer, INPUT, streamIdCodec, value.getInput());
					writer.writeByte(COMMA);

					writeValue(writer, OUTPUT, streamIdCodec, value.getOutput());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String OUTPUTS = "outputs";
	private static final String NONCE = "nonce";

	@Provides
	JsonCodec<NodeShard> nodeShard(@Subtypes JsonCodec<Function> functionCodec, JsonCodec<StreamId> streamIdCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, NumberConverter::deserializeInt);
					reader.comma();

					Function<?, ?> function = readValue(reader, KEY_FUNCTION, functionCodec);
					reader.comma();

					StreamId input = readValue(reader, INPUT, streamIdCodec);
					reader.comma();

					List<StreamId> outputs = readValue(reader, OUTPUTS, $ -> reader.readCollection(streamIdCodec));
					reader.comma();

					int nonce = readValue(reader, NONCE, INT_READER);
					reader.endObject();

					return new NodeShard(index, function, input, outputs, nonce);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_FUNCTION, functionCodec, value.getKeyFunction());
					writer.writeByte(COMMA);

					writeValue(writer, INPUT, streamIdCodec, value.getInput());
					writer.writeByte(COMMA);

					writeValue(writer, OUTPUTS, ($, outputs) -> writer.serialize(outputs, streamIdCodec), value.getOutputs());
					writer.writeByte(COMMA);

					writeValue(writer, NONCE, INT_WRITER, value.getNonce());
					writer.writeByte(OBJECT_END);
				});
	}

	@Provides
	JsonCodec<NodeMerge> nodeMerge(
			@Subtypes JsonCodec<Function> functionCodec,
			@Subtypes JsonCodec<Comparator> comparatorCodec,
			JsonCodec<StreamId> streamIdCodec
	) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, NumberConverter::deserializeInt);
					reader.comma();

					Function function = readValue(reader, KEY_FUNCTION, functionCodec);
					reader.comma();

					Comparator comparator = readValue(reader, KEY_COMPARATOR, comparatorCodec);
					reader.comma();

					boolean deduplicate = readValue(reader, DEDUPLICATE, BoolConverter.READER);
					reader.comma();

					List<StreamId> inputs = readValue(reader, INPUTS, $ -> reader.readCollection(streamIdCodec));
					reader.comma();

					StreamId output = readValue(reader, OUTPUT, streamIdCodec);
					reader.endObject();

					return new NodeMerge<>(index, function, comparator, deduplicate, inputs, output);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_FUNCTION, functionCodec, value.getKeyFunction());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_COMPARATOR, comparatorCodec, value.getKeyComparator());
					writer.writeByte(COMMA);

					writeValue(writer, DEDUPLICATE, BoolConverter.WRITER, value.isDeduplicate());
					writer.writeByte(COMMA);

					writeValue(writer, INPUTS, ($, inputs) -> writer.serialize(inputs, streamIdCodec), value.getInputs());
					writer.writeByte(COMMA);

					writeValue(writer, OUTPUT, streamIdCodec, value.getOutput());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String INPUTS = "inputs";

	@Provides
	JsonCodec<NodeReduce> nodeReduce(@Subtypes JsonCodec<Comparator> comparatorCodec, JsonCodec<StreamId> streamIdCodec,
			JsonCodec<Map<StreamId, NodeReduce.Input>> inputsCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, NumberConverter::deserializeInt);
					reader.comma();

					Comparator<?> comparator = readValue(reader, KEY_COMPARATOR, comparatorCodec);
					reader.comma();

					Map<StreamId, NodeReduce.Input> inputs = readValue(reader, INPUTS, inputsCodec);
					reader.comma();

					StreamId output = readValue(reader, OUTPUT, streamIdCodec);
					reader.endObject();

					return new NodeReduce(index, comparator, inputs, output);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_COMPARATOR, comparatorCodec, value.getKeyComparator());
					writer.writeByte(COMMA);

					writeValue(writer, INPUTS, inputsCodec, value.getInputMap());
					writer.writeByte(COMMA);

					writeValue(writer, OUTPUT, streamIdCodec, value.getOutput());
					writer.writeByte(OBJECT_END);
				});
	}

	@Provides
	JsonCodec<NodeReduceSimple> nodeReduceSimple(
			@Subtypes JsonCodec<Function> functionCodec,
			@Subtypes JsonCodec<Comparator> comparatorCodec,
			@Subtypes JsonCodec<Reducer> reducerCodec,
			JsonCodec<StreamId> streamIdCodec
	) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, NumberConverter::deserializeInt);
					reader.comma();

					Function function = readValue(reader, KEY_FUNCTION, functionCodec);
					reader.comma();

					Comparator comparator = readValue(reader, KEY_COMPARATOR, comparatorCodec);
					reader.comma();

					Reducer reducer = readValue(reader, REDUCER, reducerCodec);
					reader.comma();

					List<StreamId> inputs = readValue(reader, INPUTS, $ -> reader.readCollection(streamIdCodec));
					reader.comma();

					StreamId output = readValue(reader, OUTPUT, streamIdCodec);
					reader.endObject();

					return new NodeReduceSimple(index, function, comparator, reducer, inputs, output);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_FUNCTION, functionCodec, value.getKeyFunction());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_COMPARATOR, comparatorCodec, value.getKeyComparator());
					writer.writeByte(COMMA);

					writeValue(writer, REDUCER, reducerCodec, value.getReducer());
					writer.writeByte(COMMA);

					writeValue(writer, INPUTS, ($, inputs) -> writer.serialize(inputs, streamIdCodec), value.getInputs());
					writer.writeByte(COMMA);

					writeValue(writer, OUTPUT, streamIdCodec, value.getOutput());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String KEY_FUNCTION = "keyFunction";
	private static final String KEY_COMPARATOR = "keyComparator";
	private static final String DEDUPLICATE = "deduplicate";
	private static final String ITEMS_IN_MEMORY_SIZE = "itemsInMemorySize";

	@Provides
	JsonCodec<NodeSort> nodeSort(JsonCodec<Class<?>> typeCodec, @Subtypes JsonCodec<Comparator> comparatorCodec, @Subtypes JsonCodec<Function> functionCodec, JsonCodec<StreamId> streamIdCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, NumberConverter::deserializeInt);
					reader.comma();

					Class<?> type = readValue(reader, TYPE, typeCodec);
					reader.comma();

					Function function = readValue(reader, KEY_FUNCTION, functionCodec);
					reader.comma();

					Comparator comparator = readValue(reader, KEY_COMPARATOR, comparatorCodec);
					reader.comma();

					boolean deduplicate = readValue(reader, DEDUPLICATE, BoolConverter.READER);
					reader.comma();

					int itemsInMemorySize = readValue(reader, ITEMS_IN_MEMORY_SIZE, INT_READER);
					reader.comma();

					StreamId input = readValue(reader, INPUT, streamIdCodec);
					reader.comma();

					StreamId output = readValue(reader, OUTPUT, streamIdCodec);
					reader.endObject();

					return new NodeSort<>(index, type, function, comparator, deduplicate, itemsInMemorySize, input, output);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, TYPE, typeCodec, value.getType());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_FUNCTION, functionCodec, value.getKeyFunction());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_COMPARATOR, comparatorCodec, value.getKeyComparator());
					writer.writeByte(COMMA);

					writeValue(writer, DEDUPLICATE, BoolConverter.WRITER, value.isDeduplicate());
					writer.writeByte(COMMA);

					writeValue(writer, ITEMS_IN_MEMORY_SIZE, INT_WRITER, value.getItemsInMemorySize());
					writer.writeByte(COMMA);

					writeValue(writer, INPUT, streamIdCodec, value.getInput());
					writer.writeByte(COMMA);

					writeValue(writer, OUTPUT, streamIdCodec, value.getOutput());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String LEFT = "left";
	private static final String RIGHT = "right";
	private static final String LEFT_KEY_FUNCTION = "leftKeyFunction";
	private static final String RIGHT_KEY_FUNCTION = "rightKeyFunction";
	private static final String JOINER = "joiner";

	@Provides
	JsonCodec<NodeJoin> nodeJoin(
			@Subtypes JsonCodec<Joiner> joinerCodec,
			@Subtypes JsonCodec<Comparator> comparatorCodec,
			@Subtypes JsonCodec<Function> functionCodec,
			JsonCodec<StreamId> streamIdCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					int index = readValue(reader, INDEX, NumberConverter::deserializeInt);
					reader.comma();

					StreamId left = readValue(reader, LEFT, streamIdCodec);
					reader.comma();

					StreamId right = readValue(reader, RIGHT, streamIdCodec);
					reader.comma();

					StreamId output = readValue(reader, OUTPUT, streamIdCodec);
					reader.comma();

					Comparator comparator = readValue(reader, KEY_COMPARATOR, comparatorCodec);
					reader.comma();

					Function leftKeyFunction = readValue(reader, LEFT_KEY_FUNCTION, functionCodec);
					reader.comma();

					Function rightKeyFunction = readValue(reader, RIGHT_KEY_FUNCTION, functionCodec);
					reader.comma();

					Joiner joiner = readValue(reader, JOINER, joinerCodec);
					reader.endObject();

					return new NodeJoin<>(index, left, right, output, comparator, leftKeyFunction, rightKeyFunction, joiner);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, INDEX, INT_WRITER, value.getIndex());
					writer.writeByte(COMMA);

					writeValue(writer, LEFT, streamIdCodec, value.getLeft());
					writer.writeByte(COMMA);

					writeValue(writer, RIGHT, streamIdCodec, value.getRight());
					writer.writeByte(COMMA);

					writeValue(writer, OUTPUT, streamIdCodec, value.getOutput());
					writer.writeByte(COMMA);

					writeValue(writer, KEY_COMPARATOR, comparatorCodec, value.getKeyComparator());
					writer.writeByte(COMMA);

					writeValue(writer, LEFT_KEY_FUNCTION, functionCodec, value.getLeftKeyFunction());
					writer.writeByte(COMMA);

					writeValue(writer, RIGHT_KEY_FUNCTION, functionCodec, value.getRightKeyFunction());
					writer.writeByte(COMMA);

					writeValue(writer, JOINER, joinerCodec, value.getJoiner());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String STATUS = "status";
	private static final String START = "start";
	private static final String FINISH = "finish";
	private static final String GRAPHVIZ = "graphviz";

	@Provides
	JsonCodec<DataflowResponseTaskData> localTaskStat(
			JsonCodec<TaskStatus> statusCodec,
			JsonCodec<Map<Integer, NodeStat>> nodesCodec,
			JsonCodec<Instant> instantCodec
	) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					TaskStatus taskStatus = readValue(reader, STATUS, statusCodec);
					reader.comma();

					Instant start = readValue(reader, START, instantCodec);
					reader.comma();

					Instant finish = readValue(reader, FINISH, instantCodec);
					reader.comma();

					String error = readValue(reader, ERROR, StringConverter.READER);
					reader.comma();

					Map<Integer, NodeStat> nodes = readValue(reader, NODE_STATS, nodesCodec);
					reader.comma();

					String graphviz = readValue(reader, GRAPHVIZ, StringConverter.READER);
					reader.endObject();

					return new DataflowResponseTaskData(taskStatus, start, finish, error, nodes, graphviz);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, STATUS, statusCodec, value.getStatus());
					writer.writeByte(COMMA);

					writeValue(writer, START, instantCodec, value.getStartTime());
					writer.writeByte(COMMA);

					writeValue(writer, FINISH, instantCodec, value.getFinishTime());
					writer.writeByte(COMMA);

					writeValue(writer, ERROR, StringConverter.WRITER, value.getErrorString());
					writer.writeByte(COMMA);

					writeValue(writer, NODE_STATS, nodesCodec, value.getNodes());
					writer.writeByte(COMMA);

					writeValue(writer, GRAPHVIZ, StringConverter.WRITER, value.getGraphViz());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String STARTED = "started";
	private static final String FINISHED = "finished";
	private static final String GRAPH = "graph";

	@Provides
	JsonCodec<LocalTaskData> localTaskData(JsonCodec<TaskStatus> statusCodec, JsonCodec<Map<Integer, NodeStat>> nodesCodec, JsonCodec<Instant> instantCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					TaskStatus taskStatus = readValue(reader, STATUS, statusCodec);
					reader.comma();

					String graph = readValue(reader, GRAPH, StringConverter.READER);
					reader.comma();

					Map<Integer, NodeStat> nodes = readValue(reader, NODE_STATS, nodesCodec);
					reader.comma();

					Instant started = readValue(reader, STARTED, instantCodec);
					reader.comma();

					Instant finished = readValue(reader, FINISHED, instantCodec);
					reader.comma();

					String error = readValue(reader, ERROR, StringConverter.READER);
					reader.endObject();

					return new LocalTaskData(taskStatus, graph, nodes, started, finished, error);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, STATUS, statusCodec, value.getStatus());
					writer.writeByte(COMMA);

					writeValue(writer, GRAPH, StringConverter.WRITER, value.getGraph());
					writer.writeByte(COMMA);

					writeValue(writer, NODE_STATS, nodesCodec, value.getNodeStats());
					writer.writeByte(COMMA);

					writeValue(writer, STARTED, instantCodec, value.getStarted());
					writer.writeByte(COMMA);

					writeValue(writer, FINISHED, instantCodec, value.getFinished());
					writer.writeByte(COMMA);

					writeValue(writer, ERROR, StringConverter.WRITER, value.getError());
					writer.writeByte(OBJECT_END);
				});
	}

	private static final String STATUSES = "statuses";

	@Provides
	JsonCodec<ReducedTaskData> reducedTaskData(JsonCodec<List<TaskStatus>> statusesCodec, JsonCodec<Map<Integer, NodeStat>> nodeStatsCodec) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

					List<TaskStatus> taskStatuses = readValue(reader, STATUSES, statusesCodec);
					reader.comma();

					String graph = readValue(reader, GRAPH, StringConverter.READER);
					reader.comma();

					Map<Integer, NodeStat> nodes = readValue(reader, NODE_STATS, nodeStatsCodec);
					reader.endObject();
					return new ReducedTaskData(taskStatuses, graph, nodes);
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);

					writeValue(writer, STATUSES, statusesCodec, value.getStatuses());
					writer.writeByte(COMMA);

					writeValue(writer, GRAPH, StringConverter.WRITER, value.getGraph());
					writer.writeByte(COMMA);

					writeValue(writer, NODE_STATS, nodeStatsCodec, value.getReducedNodeStats());
					writer.writeByte(OBJECT_END);
				});
	}

	@Provides
	SubtypeNameFactory subtypeNames() {
		return subtype -> subtype == NATURAL_ORDER_CLASS ? "Comparator.naturalOrder" : null;
	}

	@Provides
	<K, V> JsonCodec<Map<K, V>> map(JsonCodec<K> keyCodec, JsonCodec<V> valueCodec) {
		return JsonCodec.of(
				reader -> ((List<Map.Entry<K, V>>) reader.readCollection(
						$ -> {
							if (reader.last() != '[')
								throw reader.newParseError("Expecting '[' as collection start");
							reader.getNextToken();
							K key = keyCodec.read(reader);
							reader.comma();

							reader.getNextToken();
							V value = valueCodec.read(reader);
							reader.endArray();
							return new AbstractMap.SimpleEntry(key, value);
						}))
						.stream()
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
				(writer, value) ->
						writer.serialize(value.entrySet(), ($, entry) -> {
							writer.writeByte(ARRAY_START);
							keyCodec.write(writer, entry.getKey());
							writer.writeByte(COMMA);
							valueCodec.write(writer, entry.getValue());
							writer.writeByte(ARRAY_END);
						})
		);
	}

	@Provides
	<T> JsonCodec<List<T>> list(JsonCodec<T> elementCodec) {
		return JsonCodec.of(
				reader -> reader.readCollection(elementCodec),
				(writer, value) -> writer.serialize(value, elementCodec)
		);
	}

	private static <T> T readValue(JsonReader<?> reader, String key, ReadObject<? extends T> readObject) throws IOException {
		reader.getNextToken();
		String readKey = reader.readKey();
		if (!readKey.equals(key)) throw reader.newParseError("Expected key '" + key + '\'');
		return readObject.read(reader);
	}

	private static <T> void writeValue(JsonWriter writer, String key, WriteObject<T> writeObject, T value) {
		writer.writeString(key);
		writer.writeByte(SEMI);
		writeObject.write(writer, value);
	}
}
