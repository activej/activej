package io.activej.dataflow.proto.calcite.serializer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.proto.calcite.RecordSchemeProto;
import io.activej.record.RecordScheme;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import static io.activej.serializer.BinarySerializers.BYTES_SERIALIZER;

public final class RecordSchemeSerializer implements BinarySerializer<RecordScheme> {
	private final DefiningClassLoader classLoader;

	public RecordSchemeSerializer(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public RecordSchemeSerializer() {
		this(DefiningClassLoader.create());
	}

	@Override
	public void encode(BinaryOutput out, RecordScheme scheme) {
		BYTES_SERIALIZER.encode(out, RecordSchemeConverter.convert(scheme).toByteArray());
	}

	@Override
	public RecordScheme decode(BinaryInput in) throws CorruptedDataException {
		byte[] serialized = BYTES_SERIALIZER.decode(in);

		RecordSchemeProto.RecordScheme scheme;
		try {
			scheme = RecordSchemeProto.RecordScheme.parseFrom(serialized);
		} catch (InvalidProtocolBufferException e) {
			throw new CorruptedDataException(e.getMessage());
		}

		return RecordSchemeConverter.convert(classLoader, scheme);
	}
}
