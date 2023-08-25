package io.activej.json;

public interface JsonKeyCodec<T> extends JsonKeyEncoder<T>, JsonKeyDecoder<T> {
	@Override
	String encode(T value);

	@Override
	T decode(String string) throws JsonValidationException;

	static <T> JsonKeyCodec<T> of(JsonKeyEncoder<T> encoder, JsonKeyDecoder<T> decoder) {
		return new JsonKeyCodec<>() {
			@Override
			public String encode(T value) {
				return encoder.encode(value);
			}

			@Override
			public T decode(String string) throws JsonValidationException {
				return decoder.decode(string);
			}
		};
	}

	static JsonKeyCodec<String> ofStringKey() {
		return new JsonKeyCodec<>() {
			@Override
			public String encode(String value) {
				return value;
			}

			@Override
			public String decode(String string) throws JsonValidationException {
				return string;
			}
		};
	}

	static <T extends Number> JsonKeyCodec<T> ofNumberKey(Class<T> type) {
		return new JsonKeyCodec<>() {
			private interface NumberParser<T extends Number> {
				T parse(String string) throws NumberFormatException;
			}

			private final NumberParser<?> parser;

			{
				if (type == Byte.class) {
					this.parser = Byte::parseByte;
				} else if (type == Short.class) {
					this.parser = Short::parseShort;
				} else if (type == Integer.class) {
					this.parser = Integer::parseInt;
				} else if (type == Long.class) {
					this.parser = Long::parseLong;
				} else if (type == Float.class) {
					this.parser = Float::parseFloat;
				} else if (type == Double.class) {
					this.parser = Double::parseDouble;
				} else
					throw new IllegalArgumentException();
			}

			@Override
			public String encode(Number value) {
				return value.toString();
			}

			@Override
			public T decode(String string) throws JsonValidationException {
				try {
					//noinspection unchecked
					return (T) parser.parse(string);
				} catch (NumberFormatException e) {
					throw new JsonValidationException("TODO", e);
				}
			}
		};
	}

}
