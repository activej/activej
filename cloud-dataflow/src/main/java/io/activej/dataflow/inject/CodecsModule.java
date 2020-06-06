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

package io.activej.dataflow.inject;

import io.activej.codec.CodecSubtype;
import io.activej.codec.StructuredCodec;
import io.activej.common.tuple.*;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.Dependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.util.Types;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.*;

import static io.activej.codec.StructuredCodecs.*;
import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public final class CodecsModule extends AbstractModule {

	private CodecsModule() {
	}

	public static Module create() {
		return new CodecsModule();
	}

	@QualifierAnnotation
	@Target({FIELD, PARAMETER, METHOD})
	@Retention(RUNTIME)
	public @interface Subtypes {
	}

	@FunctionalInterface
	public interface SubtypeNameFactory {
		@Nullable
		String getName(Class<?> subtype);
	}

	@Override
	protected void configure() {
		bindPrimitive(void.class, VOID_CODEC);
		bindPrimitive(boolean.class, BOOLEAN_CODEC);
		bindPrimitive(char.class, CHARACTER_CODEC);
		bindPrimitive(byte.class, BYTE_CODEC);
		bindPrimitive(int.class, INT_CODEC);
		bindPrimitive(long.class, LONG_CODEC);
		bindPrimitive(float.class, FLOAT_CODEC);
		bindPrimitive(double.class, DOUBLE_CODEC);

		bindPrimitive(Void.class, VOID_CODEC);
		bindPrimitive(Boolean.class, BOOLEAN_CODEC);
		bindPrimitive(Character.class, CHARACTER_CODEC);
		bindPrimitive(Byte.class, BYTE_CODEC);
		bindPrimitive(Integer.class, INT_CODEC);
		bindPrimitive(Long.class, LONG_CODEC);
		bindPrimitive(Float.class, FLOAT_CODEC);
		bindPrimitive(Double.class, DOUBLE_CODEC);

		bindPrimitive(String.class, STRING_CODEC);

		bindPrimitive(byte[].class, BYTES_CODEC);

		bind(new Key<StructuredCodec<Class<?>>>() {}).toInstance(CLASS_CODEC);

		generate(StructuredCodec.class, (bindings, scope, key) -> {
			if (key.getQualifier() != Subtypes.class) {
				return null;
			}
			Class<?> type = key.getTypeParameter(0).getRawType();
			return Binding.to(args -> {
				Injector injector = (Injector) args[0];
				SubtypeNameFactory names = (SubtypeNameFactory) args[1];
				if (names == null) {
					names = $ -> null;
				}

				Set<Class<?>> subtypes = new HashSet<>();

				Injector i = injector;
				while (i != null) {
					for (Key<?> k : i.getBindings().keySet()) {
						if (k.getRawType() != StructuredCodec.class) {
							continue;
						}
						Class<?> subtype = k.getTypeParameter(0).getRawType();
						if (type != subtype && type.isAssignableFrom(subtype)) {
							subtypes.add(subtype);
						}
					}
					i = i.getParent();
				}

				CodecSubtype<Object> combined = CodecSubtype.create();
				for (Class<?> subtype : subtypes) {
					StructuredCodec<?> codec = injector.getInstance(Key.ofType(Types.parameterized(StructuredCodec.class, subtype)));
					String name = names.getName(subtype);
					if (name != null) {
						combined.with(subtype, name, codec);
					} else {
						combined.with(subtype, codec);
					}
				}
				return combined;
			}, new Dependency[]{Dependency.toKey(Key.of(Injector.class)), Dependency.toOptionalKey(Key.of(SubtypeNameFactory.class))});
		});
	}

	private <T> void bindPrimitive(Class<T> cls, StructuredCodec<T> codec) {
		bind(Key.ofType(Types.parameterized(StructuredCodec.class, cls))).toInstance(codec);
	}

	@Provides
	<T> StructuredCodec<Optional<T>> optional(StructuredCodec<T> itemCodec) {
		return ofOptional(itemCodec);
	}

	@Provides
	<T> StructuredCodec<Set<T>> set(StructuredCodec<T> itemCodec) {
		return ofSet(itemCodec);
	}

	@Provides
	<T> StructuredCodec<List<T>> list(StructuredCodec<T> itemCodec) {
		return ofList(itemCodec);
	}

	@Provides
	<K, V> StructuredCodec<Map<K, V>> map(StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		return ofMap(keyCodec, valueCodec);
	}

	@Provides
	<T1> StructuredCodec<Tuple1<T1>> tuple1(StructuredCodec<T1> codec1) {
		return tuple(Tuple1::new,
				Tuple1::getValue1, codec1);
	}

	@Provides
	<T1, T2> StructuredCodec<Tuple2<T1, T2>> tuple2(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2) {
		return tuple(Tuple2::new,
				Tuple2::getValue1, codec1,
				Tuple2::getValue2, codec2);
	}

	@Provides
	<T1, T2, T3> StructuredCodec<Tuple3<T1, T2, T3>> tuple3(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2, StructuredCodec<T3> codec3) {
		return tuple(Tuple3::new,
				Tuple3::getValue1, codec1,
				Tuple3::getValue2, codec2,
				Tuple3::getValue3, codec3);
	}

	@Provides
	<T1, T2, T3, T4> StructuredCodec<Tuple4<T1, T2, T3, T4>> tuple4(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2, StructuredCodec<T3> codec3, StructuredCodec<T4> codec4) {
		return tuple(Tuple4::new,
				Tuple4::getValue1, codec1,
				Tuple4::getValue2, codec2,
				Tuple4::getValue3, codec3,
				Tuple4::getValue4, codec4);
	}

	@Provides
	<T1, T2, T3, T4, T5> StructuredCodec<Tuple5<T1, T2, T3, T4, T5>> tuple5(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2, StructuredCodec<T3> codec3, StructuredCodec<T4> codec4, StructuredCodec<T5> codec5) {
		return tuple(Tuple5::new,
				Tuple5::getValue1, codec1,
				Tuple5::getValue2, codec2,
				Tuple5::getValue3, codec3,
				Tuple5::getValue4, codec4,
				Tuple5::getValue5, codec5);
	}

	@Provides
	<T1, T2, T3, T4, T5, T6> StructuredCodec<Tuple6<T1, T2, T3, T4, T5, T6>> tuple6(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2, StructuredCodec<T3> codec3, StructuredCodec<T4> codec4, StructuredCodec<T5> codec5, StructuredCodec<T6> codec6) {
		return tuple(Tuple6::new,
				Tuple6::getValue1, codec1,
				Tuple6::getValue2, codec2,
				Tuple6::getValue3, codec3,
				Tuple6::getValue4, codec4,
				Tuple6::getValue5, codec5,
				Tuple6::getValue6, codec6);
	}
}
