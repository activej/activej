package io.activej.serializer.annotations;

import io.activej.serializer.SerializerDef;
import io.activej.serializer.StringFormat;

import java.lang.annotation.Annotation;

/**
 * A class that holds implementations of serializer annotations
 *
 * @see SerializeNullable
 * @see SerializeClass
 * @see SerializeFixedSize
 * @see SerializeStringFormat
 * @see SerializeVarLength
 */
@SuppressWarnings({"ClassExplicitlyAnnotation", "unused"})
public final class CustomAnnotations {

	public static SerializeNullable serializeNullable() {
		return new SerializerNullableImpl();
	}

	public static SerializeClass serializeClass(Class<? extends SerializerDef> value) {
		return new SerializeClassImpl(value);
	}

	public static SerializeClass serializeClass(Class<?>[] subclasses, String subclassesId, int subclassesIdx) {
		return new SerializeClassImpl(subclasses, subclassesId, subclassesIdx);
	}

	public static SerializeFixedSize serializeFixedSize(int value) {
		return new SerializeFixedSizeImpl(value);
	}

	public static SerializeStringFormat serializeStringFormat(StringFormat stringFormat) {
		return new SerializerStringFormatImpl(stringFormat);
	}

	public static SerializeVarLength serializeVarLength() {
		return new SerializerVarLengthImpl();
	}

	abstract static class AbstractSerializeAnnotation implements Annotation {
		@Override
		public final Class<? extends Annotation> annotationType() {
			throw new UnsupportedOperationException();
		}

		public final int[] path() {
			return new int[0];
		}
	}

	static final class SerializerNullableImpl extends AbstractSerializeAnnotation implements SerializeNullable {
	}

	static final class SerializeFixedSizeImpl extends AbstractSerializeAnnotation implements SerializeFixedSize {
		final int value;

		SerializeFixedSizeImpl(int value) {
			this.value = value;
		}

		@Override
		public int value() {
			return value;
		}
	}

	static final class SerializeClassImpl extends AbstractSerializeAnnotation implements SerializeClass {
		final Class<? extends SerializerDef> value;
		final Class<?>[] subclasses;
		final String subclassesId;
		final int subclassesIdx;

		SerializeClassImpl(Class<? extends SerializerDef> value) {
			this.value = value;
			this.subclasses = new Class[0];
			this.subclassesId = "";
			this.subclassesIdx = 0;
		}

		SerializeClassImpl(Class<?>[] subclasses, String subclassesId, int subclassesIdx) {
			this.value = SerializerDef.class;
			this.subclasses = subclasses;
			this.subclassesId = subclassesId;
			this.subclassesIdx = subclassesIdx;
		}

		@Override
		public Class<? extends SerializerDef> value() {
			return value;
		}

		@Override
		public Class<?>[] subclasses() {
			return subclasses;
		}

		@Override
		public String subclassesId() {
			return subclassesId;
		}

		@Override
		public int subclassesIdx() {
			return subclassesIdx;
		}
	}

	static final class SerializerStringFormatImpl extends AbstractSerializeAnnotation implements SerializeStringFormat {
		final StringFormat stringFormat;

		SerializerStringFormatImpl(StringFormat format) {
			stringFormat = format;
		}

		@Override
		public StringFormat value() {
			return stringFormat;
		}
	}

	static final class SerializerVarLengthImpl extends AbstractSerializeAnnotation implements SerializeVarLength {
	}

}
