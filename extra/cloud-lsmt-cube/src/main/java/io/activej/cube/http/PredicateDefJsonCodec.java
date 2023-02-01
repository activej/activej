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

package io.activej.cube.http;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;
import io.activej.aggregation.predicate.AggregationPredicate;
import io.activej.aggregation.predicate.impl.*;
import io.activej.aggregation.util.JsonCodec;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.dslplatform.json.JsonWriter.*;
import static io.activej.cube.Utils.getJsonCodec;

@SuppressWarnings("unchecked")
public final class PredicateDefJsonCodec implements JsonCodec<AggregationPredicate> {
	public static final String EMPTY_STRING = "";
	public static final String SPACES = "\\s+";
	public static final String EQ = "eq";
	public static final String NOT_EQ = "notEq";
	public static final String HAS = "has";
	public static final String GE = "ge";
	public static final String GT = "gt";
	public static final String LE = "le";
	public static final String LT = "lt";
	public static final String IN = "in";
	public static final String BETWEEN = "between";
	public static final String REGEXP = "regexp";
	public static final String AND = "and";
	public static final String OR = "or";
	public static final String NOT = "not";
	public static final String TRUE = "true";
	public static final String FALSE = "false";
	public static final String EQ_SIGN = "=";
	public static final String NOT_EQ_SIGN = "<>";
	public static final String GE_SIGN = ">=";
	public static final String GT_SIGN = ">";
	public static final String LE_SIGN = "<=";
	public static final String LT_SIGN = "<";
	public static final String IN_SIGN = "IN";
	private final Map<String, JsonCodec<Object>> attributeFormats;

	private PredicateDefJsonCodec(Map<String, JsonCodec<Object>> attributeFormats) {
		this.attributeFormats = attributeFormats;
	}

	public static PredicateDefJsonCodec create(Map<String, Type> attributeTypes, Map<String, Type> measureTypes) {
		Map<String, JsonCodec<Object>> attributeCodecs = new LinkedHashMap<>();
		for (Map.Entry<String, Type> entry : attributeTypes.entrySet()) {
			attributeCodecs.put(entry.getKey(), getJsonCodec(entry.getValue()).nullable());
		}
		for (Map.Entry<String, Type> entry : measureTypes.entrySet()) {
			attributeCodecs.put(entry.getKey(), getJsonCodec(entry.getValue()));
		}
		return new PredicateDefJsonCodec(attributeCodecs);
	}

	private void writeEq(JsonWriter writer, Eq predicate) {
		writer.writeString(predicate.key);
		writer.writeByte(SEMI);
		attributeFormats.get(predicate.key).write(writer, predicate.value);
	}

	private void writeNotEq(JsonWriter writer, NotEq predicate) {
		writer.writeString(predicate.key);
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.key).write(writer, predicate.value);
	}

	private void writeGe(JsonWriter writer, Ge predicate) {
		writer.writeString(predicate.key);
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.key).write(writer, predicate.value);
	}

	private void writeGt(JsonWriter writer, Gt predicate) {
		writer.writeString(predicate.key);
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.key).write(writer, predicate.value);
	}

	private void writeLe(JsonWriter writer, Le predicate) {
		writer.writeString(predicate.key);
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.key).write(writer, predicate.value);
	}

	private void writeLt(JsonWriter writer, Lt predicate) {
		writer.writeString(predicate.key);
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.key).write(writer, predicate.value);
	}

	private void writeIn(JsonWriter writer, In predicate) {
		writer.writeString(predicate.key);
		JsonCodec<Object> codec = attributeFormats.get(predicate.key);
		for (Object o : predicate.values) {
			writer.writeByte(COMMA);
			codec.write(writer, o);
		}
	}

	private void writeBetween(JsonWriter writer, Between predicate) {
		writer.writeString(predicate.key);
		writer.writeByte(COMMA);
		JsonCodec<Object> codec = attributeFormats.get(predicate.key);
		codec.write(writer, predicate.from);
		writer.writeByte(COMMA);
		codec.write(writer, predicate.to);
	}

	private void writeRegexp(JsonWriter writer, RegExp predicate) {
		writer.writeString(predicate.key);
		writer.writeByte(COMMA);
		writer.writeString(predicate.regexp.pattern());
	}

	private void write(JsonWriter writer, List<AggregationPredicate> predicates) {
		for (int i = 0; i < predicates.size(); i++) {
			AggregationPredicate p = predicates.get(i);
			write(writer, p);
			if (i != predicates.size() - 1) {
				writer.writeByte(COMMA);
			}
		}
	}

	private void writeNot(JsonWriter writer, Not predicate) {
		write(writer, predicate.predicate);
	}

	@SuppressWarnings("NullableProblems")
	@Override
	public void write(JsonWriter writer, AggregationPredicate predicate) {
		if (predicate instanceof Eq predicateEq) {
			writer.writeByte(OBJECT_START);
			writeEq(writer, predicateEq);
			writer.writeByte(OBJECT_END);
		} else {
			writer.writeByte(ARRAY_START);
			if (predicate instanceof NotEq predicateNotEq) {
				writer.writeString(NOT_EQ);
				writer.writeByte(COMMA);
				writeNotEq(writer, predicateNotEq);
			} else if (predicate instanceof Ge predicateGe) {
				writer.writeString(GE);
				writer.writeByte(COMMA);
				writeGe(writer, predicateGe);
			} else if (predicate instanceof Has predicateHas) {
				writer.writeString(HAS);
				writer.writeByte(COMMA);
				writer.writeString(predicateHas.key);
			} else if (predicate instanceof Gt predicateGt) {
				writer.writeString(GT);
				writer.writeByte(COMMA);
				writeGt(writer, predicateGt);
			} else if (predicate instanceof Le predicateLe) {
				writer.writeString(LE);
				writer.writeByte(COMMA);
				writeLe(writer, predicateLe);
			} else if (predicate instanceof Lt predicateLt) {
				writer.writeString(LT);
				writer.writeByte(COMMA);
				writeLt(writer, predicateLt);
			} else if (predicate instanceof In predicateIn) {
				writer.writeString(IN);
				writer.writeByte(COMMA);
				writeIn(writer, predicateIn);
			} else if (predicate instanceof Between predicateBetween) {
				writer.writeString(BETWEEN);
				writer.writeByte(COMMA);
				writeBetween(writer, predicateBetween);
			} else if (predicate instanceof RegExp predicateRegexp) {
				writer.writeString(REGEXP);
				writer.writeByte(COMMA);
				writeRegexp(writer, predicateRegexp);
			} else if (predicate instanceof And predicateAnd) {
				writer.writeString(AND);
				writer.writeByte(COMMA);
				write(writer, predicateAnd.predicates);
			} else if (predicate instanceof Or predicateOr) {
				writer.writeString(OR);
				writer.writeByte(COMMA);
				write(writer, predicateOr.predicates);
			} else if (predicate instanceof Not predicateNot) {
				writer.writeString(NOT);
				writer.writeByte(COMMA);
				writeNot(writer, predicateNot);
			} else if (predicate instanceof AlwaysTrue) {
				writer.writeString(TRUE);
			} else if (predicate instanceof AlwaysFalse) {
				writer.writeString(FALSE);
			} else {
				throw new IllegalArgumentException("Unknown predicate type");
			}
			writer.writeByte(ARRAY_END);
		}
	}

	@SuppressWarnings("unchecked")
	private AggregationPredicate readObjectWithAlgebraOfSetsOperator(JsonReader<?> reader) throws IOException {
		if (reader.last() == OBJECT_END) return new And(List.of());
		List<AggregationPredicate> predicates = new ArrayList<>();

		while (true) {
			String[] fieldWithOperator = reader.readKey().split(SPACES);
			String field = fieldWithOperator[0];
			String operator = (fieldWithOperator.length == 1) ? EMPTY_STRING : fieldWithOperator[1];
			JsonCodec<Object> codec = attributeFormats.get(field);
			if (codec == null) throw ParsingException.create("Could not decode: " + field, true);
			Object value = codec.read(reader);
			AggregationPredicate comparisonPredicate;
			switch (operator) {
				case EMPTY_STRING, EQ_SIGN -> comparisonPredicate = new Eq(field, value);
				case NOT_EQ_SIGN -> comparisonPredicate = new NotEq(field, value);
				case GE_SIGN -> comparisonPredicate = new Ge(field, (Comparable<Object>) value);
				case GT_SIGN -> comparisonPredicate = new Gt(field, (Comparable<Object>) value);
				case LE_SIGN -> comparisonPredicate = new Le(field, (Comparable<Object>) value);
				case LT_SIGN -> comparisonPredicate = new Lt(field, (Comparable<Object>) value);
				case IN_SIGN -> {
					if (value == null) {
						throw ParsingException.create("Arguments of " + IN_SIGN + " cannot be null", true);
					}
					comparisonPredicate = new In(field, (SortedSet<Object>) value);
				}
				default -> throw ParsingException.create("Could not read predicate", true);
			}
			predicates.add(comparisonPredicate);
			byte nextToken = reader.getNextToken();
			if (nextToken == OBJECT_END) {
				return predicates.size() == 1 ? predicates.get(0) : new And(predicates);
			} else if (nextToken != COMMA) {
				throw reader.newParseError("Unexpected symbol");
			}
		}
	}

	@Override
	public AggregationPredicate read(JsonReader reader) throws IOException {
		if (reader.last() == OBJECT_START) {
			reader.getNextToken();
			return readObjectWithAlgebraOfSetsOperator(reader);
		} else if (reader.last() == ARRAY_START) {
			reader.getNextToken();
			String type = reader.readString();
			AggregationPredicate result;
			byte next = reader.getNextToken();
			if (next != COMMA) {
				result = switch (type) {
					case TRUE -> AlwaysTrue.INSTANCE;
					case FALSE -> AlwaysFalse.INSTANCE;
					default -> throw reader.newParseError("Unknown predicate type " + type);
				};
			} else {
				reader.getNextToken();
				result = switch (type) {
					case EQ -> readEq(reader);
					case NOT_EQ -> readNotEq(reader);
					case GE -> readGe(reader);
					case GT -> readGt(reader);
					case LE -> readLe(reader);
					case LT -> readLt(reader);
					case IN -> readIn(reader);
					case BETWEEN -> readBetween(reader);
					case REGEXP -> readRegexp(reader);
					case AND -> readAnd(reader);
					case OR -> readOr(reader);
					case NOT -> readNot(reader);
					case HAS -> readHas(reader);
					default -> throw reader.newParseError("Unknown predicate type " + type);
				};
			}
			reader.checkArrayEnd();
			return result;
		}
		throw reader.newParseError("Either [ or { is expected");
	}

	private ReadObject<Object> getAttributeReadObject(String attribute) throws ParsingException {
		JsonCodec<Object> codec = attributeFormats.get(attribute);
		if (codec == null) {
			throw ParsingException.create("Unknown attribute: " + attribute, true);
		}
		return codec;
	}

	private AggregationPredicate readEq(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		ReadObject<Object> readObject = getAttributeReadObject(attribute);
		reader.comma();
		reader.getNextToken();
		Object value = readObject.read(reader);
		reader.getNextToken();
		return new Eq(attribute, value);
	}

	private AggregationPredicate readNotEq(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		ReadObject<Object> readObject = getAttributeReadObject(attribute);
		reader.comma();
		reader.getNextToken();
		Object value = readObject.read(reader);
		reader.getNextToken();
		return new NotEq(attribute, value);
	}

	private AggregationPredicate readGe(JsonReader<?> reader) throws IOException {
		AttributeAndValue attributeAndValue = readAttributeAndValue(reader);
		return new Ge(attributeAndValue.attribute, attributeAndValue.value);
	}

	private AggregationPredicate readGt(JsonReader<?> reader) throws IOException {
		AttributeAndValue attributeAndValue = readAttributeAndValue(reader);
		return new Gt(attributeAndValue.attribute, attributeAndValue.value);
	}

	private AggregationPredicate readLe(JsonReader<?> reader) throws IOException {
		AttributeAndValue attributeAndValue = readAttributeAndValue(reader);
		return new Le(attributeAndValue.attribute, attributeAndValue.value);
	}

	private AggregationPredicate readLt(JsonReader<?> reader) throws IOException {
		AttributeAndValue attributeAndValue = readAttributeAndValue(reader);
		return new Lt(attributeAndValue.attribute, attributeAndValue.value);
	}

	private AttributeAndValue readAttributeAndValue(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		ReadObject<Object> readObject = getAttributeReadObject(attribute);
		reader.comma();
		reader.getNextToken();
		Comparable<Object> value = (Comparable<Object>) readObject.read(reader);
		reader.getNextToken();
		return new AttributeAndValue(attribute, value);
	}

	private AggregationPredicate readIn(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		ReadObject<Object> readObject = getAttributeReadObject(attribute);
		SortedSet<Object> result = new TreeSet<>();
		byte token;
		while ((token = reader.getNextToken()) != ARRAY_END) {
			if (token != COMMA) {
				throw reader.newParseError("Comma expected");
			}
			reader.getNextToken();
			result.add(readObject.read(reader));
		}
		return new In(attribute, result);
	}

	private AggregationPredicate readBetween(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		ReadObject<Object> readObject = getAttributeReadObject(attribute);
		reader.comma();
		reader.getNextToken();
		Comparable<Object> from = (Comparable<Object>) readObject.read(reader);
		reader.comma();
		reader.getNextToken();
		Comparable<Object> to = (Comparable<Object>) readObject.read(reader);
		reader.getNextToken();
		return new Between(attribute, from, to);
	}

	private AggregationPredicate readRegexp(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		reader.comma();
		reader.getNextToken();
		String regexp = reader.readString();
		Pattern pattern;
		try {
			pattern = Pattern.compile(regexp);
		} catch (PatternSyntaxException e) {
			throw ParsingException.create("Malformed regexp", e, true);
		}
		reader.getNextToken();
		return new RegExp(attribute, pattern);
	}

	private AggregationPredicate readAnd(JsonReader<?> reader) throws IOException {
		List<AggregationPredicate> result = new ArrayList<>();
		result.add(read(reader));
		while (reader.getNextToken() == ',') {
			reader.getNextToken();
			result.add(read(reader));
		}
		return new And(result);
	}

	private AggregationPredicate readOr(JsonReader<?> reader) throws IOException {
		List<AggregationPredicate> result = new ArrayList<>();
		result.add(read(reader));
		while (reader.getNextToken() == ',') {
			reader.getNextToken();
			result.add(read(reader));
		}
		return new Or(result);
	}

	private AggregationPredicate readNot(JsonReader<?> reader) throws IOException {
		AggregationPredicate predicate = read(reader);
		reader.getNextToken();
		return new Not(predicate);
	}

	private AggregationPredicate readHas(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		reader.getNextToken();
		return new Has(attribute);
	}

	public record AttributeAndValue(String attribute, Comparable<Object> value) {
	}
}
