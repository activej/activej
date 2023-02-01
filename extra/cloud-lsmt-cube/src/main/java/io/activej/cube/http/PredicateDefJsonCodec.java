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
import io.activej.aggregation.predicate.*;
import io.activej.aggregation.util.JsonCodec;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.dslplatform.json.JsonWriter.*;
import static io.activej.cube.Utils.getJsonCodec;

@SuppressWarnings("unchecked")
public final class PredicateDefJsonCodec implements JsonCodec<PredicateDef> {
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

	private void writeEq(JsonWriter writer, PredicateDef_Eq predicate) {
		writer.writeString(predicate.getKey());
		writer.writeByte(SEMI);
		attributeFormats.get(predicate.getKey()).write(writer, predicate.getValue());
	}

	private void writeNotEq(JsonWriter writer, PredicateDef_NotEq predicate) {
		writer.writeString(predicate.getKey());
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.getKey()).write(writer, predicate.getValue());
	}

	private void writeGe(JsonWriter writer, PredicateDef_Ge predicate) {
		writer.writeString(predicate.getKey());
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.getKey()).write(writer, predicate.getValue());
	}

	private void writeGt(JsonWriter writer, PredicateDef_Gt predicate) {
		writer.writeString(predicate.getKey());
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.getKey()).write(writer, predicate.getValue());
	}

	private void writeLe(JsonWriter writer, PredicateDef_Le predicate) {
		writer.writeString(predicate.getKey());
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.getKey()).write(writer, predicate.getValue());
	}

	private void writeLt(JsonWriter writer, PredicateDef_Lt predicate) {
		writer.writeString(predicate.getKey());
		writer.writeByte(COMMA);
		attributeFormats.get(predicate.getKey()).write(writer, predicate.getValue());
	}

	private void writeIn(JsonWriter writer, PredicateDef_In predicate) {
		writer.writeString(predicate.getKey());
		JsonCodec<Object> codec = attributeFormats.get(predicate.getKey());
		for (Object o : predicate.getValues()) {
			writer.writeByte(COMMA);
			codec.write(writer, o);
		}
	}

	private void writeBetween(JsonWriter writer, PredicateDef_Between predicate) {
		writer.writeString(predicate.getKey());
		writer.writeByte(COMMA);
		JsonCodec<Object> codec = attributeFormats.get(predicate.getKey());
		codec.write(writer, predicate.getFrom());
		writer.writeByte(COMMA);
		codec.write(writer, predicate.getTo());
	}

	private void writeRegexp(JsonWriter writer, PredicateDef_RegExp predicate) {
		writer.writeString(predicate.getKey());
		writer.writeByte(COMMA);
		writer.writeString(predicate.getRegexp());
	}

	private void write(JsonWriter writer, List<PredicateDef> predicates) {
		for (int i = 0; i < predicates.size(); i++) {
			PredicateDef p = predicates.get(i);
			write(writer, p);
			if (i != predicates.size() - 1) {
				writer.writeByte(COMMA);
			}
		}
	}

	private void writeNot(JsonWriter writer, PredicateDef_Not predicate) {
		write(writer, predicate.getPredicate());
	}

	@SuppressWarnings("NullableProblems")
	@Override
	public void write(JsonWriter writer, PredicateDef predicate) {
		if (predicate instanceof PredicateDef_Eq predicateEq) {
			writer.writeByte(OBJECT_START);
			writeEq(writer, predicateEq);
			writer.writeByte(OBJECT_END);
		} else {
			writer.writeByte(ARRAY_START);
			if (predicate instanceof PredicateDef_NotEq predicateNotEq) {
				writer.writeString(NOT_EQ);
				writer.writeByte(COMMA);
				writeNotEq(writer, predicateNotEq);
			} else if (predicate instanceof PredicateDef_Ge predicateGe) {
				writer.writeString(GE);
				writer.writeByte(COMMA);
				writeGe(writer, predicateGe);
			} else if (predicate instanceof PredicateDef_Has predicateHas) {
				writer.writeString(HAS);
				writer.writeByte(COMMA);
				writer.writeString(predicateHas.getKey());
			} else if (predicate instanceof PredicateDef_Gt predicateGt) {
				writer.writeString(GT);
				writer.writeByte(COMMA);
				writeGt(writer, predicateGt);
			} else if (predicate instanceof PredicateDef_Le predicateLe) {
				writer.writeString(LE);
				writer.writeByte(COMMA);
				writeLe(writer, predicateLe);
			} else if (predicate instanceof PredicateDef_Lt predicateLt) {
				writer.writeString(LT);
				writer.writeByte(COMMA);
				writeLt(writer, predicateLt);
			} else if (predicate instanceof PredicateDef_In predicateIn) {
				writer.writeString(IN);
				writer.writeByte(COMMA);
				writeIn(writer, predicateIn);
			} else if (predicate instanceof PredicateDef_Between predicateBetween) {
				writer.writeString(BETWEEN);
				writer.writeByte(COMMA);
				writeBetween(writer, predicateBetween);
			} else if (predicate instanceof PredicateDef_RegExp predicateRegexp) {
				writer.writeString(REGEXP);
				writer.writeByte(COMMA);
				writeRegexp(writer, predicateRegexp);
			} else if (predicate instanceof PredicateDef_And predicateAnd) {
				writer.writeString(AND);
				writer.writeByte(COMMA);
				write(writer, predicateAnd.getPredicates());
			} else if (predicate instanceof PredicateDef_Or predicateOr) {
				writer.writeString(OR);
				writer.writeByte(COMMA);
				write(writer, predicateOr.getPredicates());
			} else if (predicate instanceof PredicateDef_Not predicateNot) {
				writer.writeString(NOT);
				writer.writeByte(COMMA);
				writeNot(writer, predicateNot);
			} else if (predicate instanceof PredicateDef_AlwaysTrue) {
				writer.writeString(TRUE);
			} else if (predicate instanceof PredicateDef_AlwaysFalse) {
				writer.writeString(FALSE);
			} else {
				throw new IllegalArgumentException("Unknown predicate type");
			}
			writer.writeByte(ARRAY_END);
		}
	}

	@SuppressWarnings("unchecked")
	private PredicateDef readObjectWithAlgebraOfSetsOperator(JsonReader<?> reader) throws IOException {
		if (reader.last() == OBJECT_END) return new PredicateDef_And(List.of());
		List<PredicateDef> predicates = new ArrayList<>();

		while (true) {
			String[] fieldWithOperator = reader.readKey().split(SPACES);
			String field = fieldWithOperator[0];
			String operator = (fieldWithOperator.length == 1) ? EMPTY_STRING : fieldWithOperator[1];
			JsonCodec<Object> codec = attributeFormats.get(field);
			if (codec == null) throw ParsingException.create("Could not decode: " + field, true);
			Object value = codec.read(reader);
			PredicateDef comparisonPredicate;
			switch (operator) {
				case EMPTY_STRING, EQ_SIGN -> comparisonPredicate = new PredicateDef_Eq(field, value);
				case NOT_EQ_SIGN -> comparisonPredicate = new PredicateDef_NotEq(field, value);
				case GE_SIGN -> comparisonPredicate = new PredicateDef_Ge(field, (Comparable<Object>) value);
				case GT_SIGN -> comparisonPredicate = new PredicateDef_Gt(field, (Comparable<Object>) value);
				case LE_SIGN -> comparisonPredicate = new PredicateDef_Le(field, (Comparable<Object>) value);
				case LT_SIGN -> comparisonPredicate = new PredicateDef_Lt(field, (Comparable<Object>) value);
				case IN_SIGN -> {
					if (value == null) {
						throw ParsingException.create("Arguments of " + IN_SIGN + " cannot be null", true);
					}
					comparisonPredicate = new PredicateDef_In(field, (SortedSet<Object>) value);
				}
				default -> throw ParsingException.create("Could not read predicate", true);
			}
			predicates.add(comparisonPredicate);
			byte nextToken = reader.getNextToken();
			if (nextToken == OBJECT_END) {
				return predicates.size() == 1 ? predicates.get(0) : new PredicateDef_And(predicates);
			} else if (nextToken != COMMA) {
				throw reader.newParseError("Unexpected symbol");
			}
		}
	}

	@Override
	public PredicateDef read(JsonReader reader) throws IOException {
		if (reader.last() == OBJECT_START) {
			reader.getNextToken();
			return readObjectWithAlgebraOfSetsOperator(reader);
		} else if (reader.last() == ARRAY_START) {
			reader.getNextToken();
			String type = reader.readString();
			PredicateDef result;
			byte next = reader.getNextToken();
			if (next != COMMA) {
				result = switch (type) {
					case TRUE -> PredicateDef_AlwaysTrue.INSTANCE;
					case FALSE -> PredicateDef_AlwaysFalse.INSTANCE;
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

	private PredicateDef readEq(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		ReadObject<Object> readObject = getAttributeReadObject(attribute);
		reader.comma();
		reader.getNextToken();
		Object value = readObject.read(reader);
		reader.getNextToken();
		return new PredicateDef_Eq(attribute, value);
	}

	private PredicateDef readNotEq(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		ReadObject<Object> readObject = getAttributeReadObject(attribute);
		reader.comma();
		reader.getNextToken();
		Object value = readObject.read(reader);
		reader.getNextToken();
		return new PredicateDef_NotEq(attribute, value);
	}

	private PredicateDef readGe(JsonReader<?> reader) throws IOException {
		AttributeAndValue attributeAndValue = readAttributeAndValue(reader);
		return new PredicateDef_Ge(attributeAndValue.attribute, attributeAndValue.value);
	}

	private PredicateDef readGt(JsonReader<?> reader) throws IOException {
		AttributeAndValue attributeAndValue = readAttributeAndValue(reader);
		return new PredicateDef_Gt(attributeAndValue.attribute, attributeAndValue.value);
	}

	private PredicateDef readLe(JsonReader<?> reader) throws IOException {
		AttributeAndValue attributeAndValue = readAttributeAndValue(reader);
		return new PredicateDef_Le(attributeAndValue.attribute, attributeAndValue.value);
	}

	private PredicateDef readLt(JsonReader<?> reader) throws IOException {
		AttributeAndValue attributeAndValue = readAttributeAndValue(reader);
		return new PredicateDef_Lt(attributeAndValue.attribute, attributeAndValue.value);
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

	private PredicateDef readIn(JsonReader<?> reader) throws IOException {
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
		return new PredicateDef_In(attribute, result);
	}

	private PredicateDef readBetween(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		ReadObject<Object> readObject = getAttributeReadObject(attribute);
		reader.comma();
		reader.getNextToken();
		Comparable<Object> from = (Comparable<Object>) readObject.read(reader);
		reader.comma();
		reader.getNextToken();
		Comparable<Object> to = (Comparable<Object>) readObject.read(reader);
		reader.getNextToken();
		return new PredicateDef_Between(attribute, from, to);
	}

	private PredicateDef readRegexp(JsonReader<?> reader) throws IOException {
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
		return new PredicateDef_RegExp(attribute, pattern);
	}

	private PredicateDef readAnd(JsonReader<?> reader) throws IOException {
		List<PredicateDef> result = new ArrayList<>();
		result.add(read(reader));
		while (reader.getNextToken() == ',') {
			reader.getNextToken();
			result.add(read(reader));
		}
		return new PredicateDef_And(result);
	}

	private PredicateDef readOr(JsonReader<?> reader) throws IOException {
		List<PredicateDef> result = new ArrayList<>();
		result.add(read(reader));
		while (reader.getNextToken() == ',') {
			reader.getNextToken();
			result.add(read(reader));
		}
		return new PredicateDef_Or(result);
	}

	private PredicateDef readNot(JsonReader<?> reader) throws IOException {
		PredicateDef predicate = read(reader);
		reader.getNextToken();
		return new PredicateDef_Not(predicate);
	}

	private PredicateDef readHas(JsonReader<?> reader) throws IOException {
		String attribute = reader.readString();
		reader.getNextToken();
		return new PredicateDef_Has(attribute);
	}

	public record AttributeAndValue(String attribute, Comparable<Object> value) {
	}
}
