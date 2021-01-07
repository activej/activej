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

import io.activej.aggregation.AggregationPredicate;
import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredInput;
import io.activej.codec.StructuredOutput;
import io.activej.codec.registry.CodecFactory;
import io.activej.common.exception.MalformedDataException;

import java.lang.reflect.Type;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static io.activej.aggregation.AggregationPredicates.*;
import static io.activej.codec.StructuredInput.Token.OBJECT;

final class AggregationPredicateCodec implements StructuredCodec<AggregationPredicate> {
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
	private final Map<String, StructuredCodec<?>> attributeCodecs;

	private AggregationPredicateCodec(Map<String, StructuredCodec<?>> attributeCodecs) {
		this.attributeCodecs = attributeCodecs;
	}

	public static AggregationPredicateCodec create(CodecFactory mapping,
			Map<String, Type> attributeTypes, Map<String, Type> measureTypes) {
		Map<String, StructuredCodec<?>> attributeCodecs = new LinkedHashMap<>();
		for (Map.Entry<String, Type> entry : attributeTypes.entrySet()) {
			attributeCodecs.put(entry.getKey(), mapping.get(entry.getValue()).nullable());
		}
		for (Map.Entry<String, Type> entry : measureTypes.entrySet()) {
			attributeCodecs.put(entry.getKey(), mapping.get(entry.getValue()));
		}
		return new AggregationPredicateCodec(attributeCodecs);
	}

	@SuppressWarnings("unchecked")
	private void writeEq(StructuredOutput writer, PredicateEq predicate) {
		writer.writeKey(predicate.getKey());
		StructuredCodec<Object> codec = (StructuredCodec<Object>) attributeCodecs.get(predicate.getKey());
		codec.encode(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeNotEq(StructuredOutput writer, PredicateNotEq predicate) {
		writer.writeString(predicate.getKey());
		StructuredCodec<Object> codec = (StructuredCodec<Object>) attributeCodecs.get(predicate.getKey());
		codec.encode(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeGe(StructuredOutput writer, PredicateGe predicate) {
		writer.writeString(predicate.getKey());
		StructuredCodec<Object> codec = (StructuredCodec<Object>) attributeCodecs.get(predicate.getKey());
		codec.encode(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeGt(StructuredOutput writer, PredicateGt predicate) {
		writer.writeString(predicate.getKey());
		StructuredCodec<Object> codec = (StructuredCodec<Object>) attributeCodecs.get(predicate.getKey());
		codec.encode(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeLe(StructuredOutput writer, PredicateLe predicate) {
		writer.writeString(predicate.getKey());
		StructuredCodec<Object> codec = (StructuredCodec<Object>) attributeCodecs.get(predicate.getKey());
		codec.encode(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeLt(StructuredOutput writer, PredicateLt predicate) {
		writer.writeString(predicate.getKey());
		StructuredCodec<Object> codec = (StructuredCodec<Object>) attributeCodecs.get(predicate.getKey());
		codec.encode(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeIn(StructuredOutput writer, PredicateIn predicate) {
		writer.writeString(predicate.getKey());
		StructuredCodec<Object> codec = (StructuredCodec<Object>) attributeCodecs.get(predicate.getKey());
		for (Object value : predicate.getValues()) {
			codec.encode(writer, value);
		}
	}

	@SuppressWarnings("unchecked")
	private void writeBetween(StructuredOutput writer, PredicateBetween predicate) {
		writer.writeString(predicate.getKey());
		StructuredCodec<Object> codec = (StructuredCodec<Object>) attributeCodecs.get(predicate.getKey());
		codec.encode(writer, predicate.getFrom());
		codec.encode(writer, predicate.getTo());
	}

	private void writeRegexp(StructuredOutput writer, PredicateRegexp predicate) {
		writer.writeString(predicate.getKey());
		writer.writeString(predicate.getRegexp());
	}

	private void writeAnd(StructuredOutput writer, PredicateAnd predicate) {
		for (AggregationPredicate p : predicate.getPredicates()) {
			encode(writer, p);
		}
	}

	private void writeOr(StructuredOutput writer, PredicateOr predicate) {
		for (AggregationPredicate p : predicate.getPredicates()) {
			encode(writer, p);
		}
	}

	private void writeNot(StructuredOutput writer, PredicateNot predicate) {
		encode(writer, predicate.getPredicate());
	}

	@Override
	public void encode(StructuredOutput writer, AggregationPredicate predicate) {
		if (predicate instanceof PredicateEq) {
			writer.writeObject(() -> writeEq(writer, (PredicateEq) predicate));
		} else {
			writer.writeTuple(() -> {
				if (predicate instanceof PredicateNotEq) {
					writer.writeString(NOT_EQ);
					writeNotEq(writer, (PredicateNotEq) predicate);
				} else if (predicate instanceof PredicateGe) {
					writer.writeString(GE);
					writeGe(writer, (PredicateGe) predicate);
				} else if (predicate instanceof PredicateHas) {
					writer.writeString(HAS);
					writer.writeString(((PredicateHas) predicate).getKey());
				} else if (predicate instanceof PredicateGt) {
					writer.writeString(GT);
					writeGt(writer, (PredicateGt) predicate);
				} else if (predicate instanceof PredicateLe) {
					writer.writeString(LE);
					writeLe(writer, (PredicateLe) predicate);
				} else if (predicate instanceof PredicateLt) {
					writer.writeString(LT);
					writeLt(writer, (PredicateLt) predicate);
				} else if (predicate instanceof PredicateIn) {
					writer.writeString(IN);
					writeIn(writer, (PredicateIn) predicate);
				} else if (predicate instanceof PredicateBetween) {
					writer.writeString(BETWEEN);
					writeBetween(writer, (PredicateBetween) predicate);
				} else if (predicate instanceof PredicateRegexp) {
					writer.writeString(REGEXP);
					writeRegexp(writer, (PredicateRegexp) predicate);
				} else if (predicate instanceof PredicateAnd) {
					writer.writeString(AND);
					writeAnd(writer, (PredicateAnd) predicate);
				} else if (predicate instanceof PredicateOr) {
					writer.writeString(OR);
					writeOr(writer, (PredicateOr) predicate);
				} else if (predicate instanceof PredicateNot) {
					writer.writeString(NOT);
					writeNot(writer, (PredicateNot) predicate);
				} else if (predicate instanceof PredicateAlwaysTrue) {
					writer.writeString(TRUE);
				} else if (predicate instanceof PredicateAlwaysFalse) {
					writer.writeString(FALSE);
				} else {
					throw new IllegalArgumentException("Unknown predicate type");
				}
			});
		}
	}

	private AggregationPredicate readObjectWithAlgebraOfSetsOperator(StructuredInput reader) throws MalformedDataException {
		List<AggregationPredicate> predicates = new ArrayList<>();
		while (reader.hasNext()) {
			String[] fieldWithOperator = reader.readKey().split(SPACES);
			String field = fieldWithOperator[0];
			String operator = (fieldWithOperator.length == 1) ? EMPTY_STRING : fieldWithOperator[1];
			StructuredCodec<?> codec = attributeCodecs.get(field);
			if (codec == null) throw new MalformedDataException("Could not decode: " + field);
			Object value = codec.decode(reader);
			AggregationPredicate comparisonPredicate;
			switch (operator) {
				case EMPTY_STRING:
				case EQ_SIGN:
					comparisonPredicate = eq(field, value);
					break;
				case NOT_EQ_SIGN:
					comparisonPredicate = notEq(field, value);
					break;
				case GE_SIGN:
					comparisonPredicate = ge(field, (Comparable<?>) value);
					break;
				case GT_SIGN:
					comparisonPredicate = gt(field, (Comparable<?>) value);
					break;
				case LE_SIGN:
					comparisonPredicate = le(field, (Comparable<?>) value);
					break;
				case LT_SIGN:
					comparisonPredicate = lt(field, (Comparable<?>) value);
					break;
				case IN_SIGN:
					comparisonPredicate = in(field, (Set<?>) value);
					break;
				default:
					throw new MalformedDataException("Could not read predicate");
			}
			predicates.add(comparisonPredicate);
		}
		return predicates.size() == 1 ? predicates.get(0) : and(predicates);
	}

	@SuppressWarnings("unchecked")
	private <T> StructuredCodec<T> getAttributeCodec(String attribute) throws MalformedDataException {
		StructuredCodec<T> codec = (StructuredCodec<T>) attributeCodecs.get(attribute);
		if (codec == null) {
			throw new MalformedDataException("Unknown attribute: " + attribute);
		}
		return codec;
	}

	private AggregationPredicate readEq(StructuredInput reader) throws MalformedDataException {
		String attribute = reader.readString();
		StructuredCodec<?> codec = getAttributeCodec(attribute);
		Object value = codec.decode(reader);
		return eq(attribute, value);
	}

	private AggregationPredicate readNotEq(StructuredInput reader) throws MalformedDataException {
		String attribute = reader.readString();
		StructuredCodec<?> codec = getAttributeCodec(attribute);
		Object value = codec.decode(reader);
		return notEq(attribute, value);
	}

	private AggregationPredicate readGe(StructuredInput reader) throws MalformedDataException {
		String attribute = reader.readString();
		StructuredCodec<?> codec = getAttributeCodec(attribute);
		Comparable<?> value = (Comparable<?>) codec.decode(reader);
		return ge(attribute, value);
	}

	private AggregationPredicate readGt(StructuredInput reader) throws MalformedDataException {
		String attribute = reader.readString();
		StructuredCodec<?> codec = getAttributeCodec(attribute);
		Comparable<?> value = (Comparable<?>) codec.decode(reader);
		return gt(attribute, value);
	}

	private AggregationPredicate readLe(StructuredInput reader) throws MalformedDataException {
		String attribute = reader.readString();
		StructuredCodec<?> codec = getAttributeCodec(attribute);
		Comparable<?> value = (Comparable<?>) codec.decode(reader);
		return le(attribute, value);
	}

	private AggregationPredicate readLt(StructuredInput reader) throws MalformedDataException {
		String attribute = reader.readString();
		StructuredCodec<?> codec = getAttributeCodec(attribute);
		Comparable<?> value = (Comparable<?>) codec.decode(reader);
		return lt(attribute, value);
	}

	private AggregationPredicate readIn(StructuredInput reader) throws MalformedDataException {
		String attribute = reader.readString();
		StructuredCodec<?> codec = getAttributeCodec(attribute);
		Set<Object> values = new LinkedHashSet<>();
		while (reader.hasNext()) {
			Object value = codec.decode(reader);
			values.add(value);
		}
		return in(attribute, values);
	}

	private AggregationPredicate readBetween(StructuredInput reader) throws MalformedDataException {
		String attribute = reader.readString();
		StructuredCodec<?> codec = getAttributeCodec(attribute);
		Comparable<?> from = (Comparable<?>) codec.decode(reader);
		Comparable<?> to = (Comparable<?>) codec.decode(reader);
		return between(attribute, from, to);
	}

	private AggregationPredicate readRegexp(StructuredInput reader) throws MalformedDataException {
		String attribute = reader.readString();
		String regexp = reader.readString();
		Pattern pattern;
		try {
			pattern = Pattern.compile(regexp);
		} catch (PatternSyntaxException e) {
			throw new MalformedDataException("Malformed regexp", e);
		}
		return regexp(attribute, pattern);
	}

	private AggregationPredicate readAnd(StructuredInput reader) throws MalformedDataException {
		List<AggregationPredicate> predicates = new ArrayList<>();
		while (reader.hasNext()) {
			AggregationPredicate predicate = decode(reader);
			predicates.add(predicate);
		}
		return and(predicates);
	}

	private AggregationPredicate readOr(StructuredInput reader) throws MalformedDataException {
		List<AggregationPredicate> predicates = new ArrayList<>();
		while (reader.hasNext()) {
			AggregationPredicate predicate = decode(reader);
			predicates.add(predicate);
		}
		return or(predicates);
	}

	private AggregationPredicate readNot(StructuredInput reader) throws MalformedDataException {
		AggregationPredicate predicate = decode(reader);
		return not(predicate);
	}

	@Override
	public AggregationPredicate decode(StructuredInput reader) throws MalformedDataException {
		if (reader.getNext().contains(OBJECT)) {
			return reader.readObject(this::readObjectWithAlgebraOfSetsOperator);
		} else {
			return reader.readTuple(in -> {
				String type = in.readString();
				switch (type) {
					case EQ:
						return readEq(in);
					case NOT_EQ:
						return readNotEq(in);
					case GE:
						return readGe(in);
					case GT:
						return readGt(in);
					case LE:
						return readLe(in);
					case LT:
						return readLt(in);
					case IN:
						return readIn(in);
					case BETWEEN:
						return readBetween(in);
					case REGEXP:
						return readRegexp(in);
					case AND:
						return readAnd(in);
					case OR:
						return readOr(in);
					case NOT:
						return readNot(in);
					case TRUE:
						return alwaysTrue();
					case FALSE:
						return alwaysFalse();
					default:
						throw new MalformedDataException("Unknown predicate type " + type);
				}
			});
		}
	}
}
