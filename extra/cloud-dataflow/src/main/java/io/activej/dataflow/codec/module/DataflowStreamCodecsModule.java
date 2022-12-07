package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.StreamCodecSubtype;
import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.codec.SubtypeImpl;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.KeyPattern;
import io.activej.inject.binding.Binding;
import io.activej.inject.module.AbstractModule;
import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StreamCodecs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.activej.common.Checks.checkState;
import static io.activej.types.Types.parameterizedType;

public final class DataflowStreamCodecsModule extends AbstractModule {
	private static final ThreadLocal<Set<Key<StreamCodec<?>>>> GENERATING = ThreadLocal.withInitial(HashSet::new);

	private DataflowStreamCodecsModule() {
	}

	public static DataflowStreamCodecsModule create() {
		return new DataflowStreamCodecsModule();
	}

	@Override
	protected void configure() {
		install(new DataflowRequestCodecsModule());
		install(new DataflowResponseCodecsModule());

		generate(new KeyPattern<StreamCodec<?>>() {}, (bindings, scope, key) ->
				Binding.to(injector -> {
							Set<Key<StreamCodec<?>>> generating = GENERATING.get();
							try {
								if (!generating.add(key)) {
									return StreamCodecs.lazy(() -> doGenerate(key, injector));
								}
								return doGenerate(key, injector);
							} finally {
								generating.remove(key);
							}

						},
						Injector.class));
	}

	@SuppressWarnings("unchecked")
	private static StreamCodec<?> doGenerate(Key<StreamCodec<?>> key, Injector injector) {
		Map<Integer, Class<?>> subtypes = new HashMap<>();
		Class<Object> type = key.getTypeParameter(0).getRawType();

		Injector i = injector;
		while (i != null) {
			for (Key<?> k : i.getBindings().keySet()) {
				if (k.getRawType() != StreamCodec.class) {
					continue;
				}
				if (!(k.getQualifier() instanceof Subtype subtypeAnnotation)) {
					continue;
				}
				Class<?> subtype = k.getTypeParameter(0).getRawType();

				if (!(type.isAssignableFrom(subtype))) {
					continue;
				}

				checkState(!subtypes.containsValue(subtype), "Duplicate subtypes of type: " + subtype);
				Class<?> prev = subtypes.put(subtypeAnnotation.value(), subtype);
				checkState(prev == null, "Duplicate subtypes with index: " + subtypeAnnotation.value());
			}
			i = i.getParent();
		}

		StreamCodecSubtype<Object> codecSubtype = new StreamCodecSubtype<>();

		for (Map.Entry<Integer, Class<?>> entry : subtypes.entrySet()) {
			StreamCodec<?> codec = injector.getInstance(Key.ofType(
					parameterizedType(StreamCodec.class, entry.getValue()),
					SubtypeImpl.subtype(entry.getKey())));

			//noinspection rawtypes
			codecSubtype.addSubtype(entry.getKey(), ((Class) entry.getValue()), codec);
		}

		return codecSubtype;
	}
}
