package io.activej.dataflow.codec;

import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamInput;
import io.activej.serializer.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.activej.common.Checks.checkArgument;

public final class StreamCodecSubtype<T> implements StreamCodec<T> {
	private final Map<Class<? extends T>, Integer> classToIndexMap = new HashMap<>();
	private final Map<Integer, StreamCodec<? extends T>> indexToCodecMap = new HashMap<>();

	public <S extends T> void addSubtype(int index, Class<S> subtypeClass, StreamCodec<S> subtypeCodec) {
		for (Class<? extends T> aClass : classToIndexMap.keySet()) {
			if (aClass.isAssignableFrom(subtypeClass) || subtypeClass.isAssignableFrom(aClass)) {
				throw new IllegalArgumentException("Conflicting subtypes: " + subtypeClass.getName() + " and " + aClass.getName());
			}
		}

		Integer prevIndex = classToIndexMap.put(subtypeClass, index);
		checkArgument(prevIndex == null, () -> "Subtype " + subtypeClass.getName() + " already added");

		StreamCodec<? extends T> prevCodec = indexToCodecMap.put(index, subtypeCodec);
		checkArgument(prevCodec == null, () -> "Subtype with index " + index + " already added");
	}

	@Override
	public T decode(StreamInput input) throws IOException {
		int index = input.readVarInt();

		StreamCodec<? extends T> streamCodec = indexToCodecMap.get(index);
		if (streamCodec == null) {
			throw new CorruptedDataException("Cannot decode subtype of index: " + index);
		}

		return streamCodec.decode(input);
	}

	@Override
	public void encode(StreamOutput output, T item) throws IOException {
		Integer index = findIndex(item.getClass());
		if (index == null) {
			throw new IllegalArgumentException("Cannot encode subtype: " + item.getClass());
		}

		//noinspection unchecked
		StreamCodec<T> streamCodec = (StreamCodec<T>) indexToCodecMap.get(index);
		assert streamCodec != null;

		output.writeVarInt(index);
		streamCodec.encode(output, item);
	}

	private @Nullable Integer findIndex(Class<?> cls) {
		Integer index = classToIndexMap.get(cls);

		if (index != null) return index;

		for (Map.Entry<Class<? extends T>, Integer> entry : classToIndexMap.entrySet()) {
			if (entry.getKey().isAssignableFrom(cls)) {
				if (index != null) {
					throw new IllegalArgumentException("Ambiguous codecs found for type " + cls.getName());
				}
				index = entry.getValue();
			}
		}

		return index;
	}
}
