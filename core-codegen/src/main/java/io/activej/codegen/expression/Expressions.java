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

package io.activej.codegen.expression;

import io.activej.codegen.operation.ArithmeticOperation;
import io.activej.codegen.operation.CompareOperation;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.activej.codegen.expression.ExpressionCast.SELF_TYPE;
import static io.activej.codegen.expression.ExpressionComparator.*;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.objectweb.asm.Type.getType;

/**
 * Defines list of possibilities for creating dynamic objects
 */
public class Expressions {
	/**
	 * Returns a new constant for the value
	 *
	 * @param value value which will be created as constant
	 * @return new instance of the ExpressionConstant
	 */
	public static Expression value(Object value) {
		return new ExpressionConstant(value);
	}

	/**
	 * Returns a new constant for the value of a given type
	 *
	 * @param value value which will be created as constant
	 * @param type  actual type of value
	 * @return new instance of the ExpressionConstant
	 */
	public static Expression value(Object value, Class<?> type) {
		return new ExpressionConstant(value, type);
	}

	/**
	 * @see #sequence(List)
	 */
	public static Expression sequence(Expression... parts) {
		return new ExpressionSequence(asList(parts));
	}

	/**
	 * Returns a sequence of operations which will be processed one after the other
	 *
	 * @param parts list of operations
	 * @return new instance of the ExpressionSequence
	 */
	public static Expression sequence(List<Expression> parts) {
		List<Expression> list = new ArrayList<>(parts.size());
		for (Expression part : parts) {
			if (part instanceof ExpressionSequence) {
				list.addAll(((ExpressionSequence) part).expressions);
			} else {
				list.add(part);
			}
		}
		return new ExpressionSequence(list);
	}

	/**
	 * Returns a sequence of operations which will be processed one after the other.
	 * Operations should be added to a list that is consumed by the given consumer
	 *
	 * @param consumer consumer of a list of operations. Operations added to the list
	 *                 will be processed in a sequence
	 * @return new instance of the ExpressionSequence
	 */
	public static Expression sequence(Consumer<List<Expression>> consumer) {
		List<Expression> seq = new ArrayList<>();
		consumer.accept(seq);
		return new ExpressionSequence(seq);
	}

	/**
	 * Returns a sequence of operations which will be processed one after the other.
	 * Operations should be added to a list that is mapped by the given function. The last
	 * operation should be returned as a result of a given function
	 *
	 * @param fn function that transforms a list of operations to an expression
	 * @return new instance of the ExpressionSequence
	 */
	public static Expression sequence(Function<List<Expression>, Expression> fn) {
		List<Expression> seq = new ArrayList<>();
		Expression result = fn.apply(seq);
		seq.add(result);
		return new ExpressionSequence(seq);
	}

	/**
	 * Returns an expression that represents a new local variables with some action applied to it
	 *
	 * @param expression new local variable
	 * @param fn         function applied to a new local variable
	 * @return an expression that represents a new local variable with some action applied to it
	 */
	public static Expression let(Expression expression, Function<Variable, Expression> fn) {
		Variable variable = new ExpressionLet(expression);
		return sequence(variable, fn.apply(variable));
	}

	/**
	 * Returns an expression that represents new local variables with some action applied to them
	 *
	 * @param expressions list of new local variables
	 * @param fn          function applied to a list of new local variables
	 * @return an expression that represents new local variables with some action applied to them
	 */
	public static Expression let(List<Expression> expressions, Function<List<Variable>, Expression> fn) {
		List<Variable> variables = expressions.stream().map(ExpressionLet::new).collect(toList());
		List<Expression> sequence = new ArrayList<>(expressions.size() + 1);
		sequence.addAll(variables);
		sequence.add(fn.apply(variables));
		return sequence(sequence);
	}

	/**
	 * @see #let(List, Function)
	 */
	public static Expression let(Expression[] expressions, Function<Variable[], Expression> fn) {
		return let(asList(expressions), variables -> fn.apply(variables.toArray(new Variable[0])));
	}

	/**
	 * Sets the value from the argument 'from' to the argument 'to'
	 *
	 * @param to   variable which will be changed
	 * @param from variable which changes
	 * @return new instance of the Expression
	 */
	public static Expression set(StoreDef to, Expression from) {
		return new ExpressionSet(to, from);
	}

	/**
	 * Casts expression to the type
	 *
	 * @param expression expressions which will be cast
	 * @param type       expression will be cast to the 'type'
	 * @return new instance of the Expression which is cast to the type
	 */
	public static Expression cast(Expression expression, Class<?> type) {
		return new ExpressionCast(expression, getType(type));
	}

	/**
	 * Casts a given to a self type (a type that is being generated)
	 *
	 * @param expression an original expression
	 * @return expression cast to a self type
	 */
	public static Expression castIntoSelf(Expression expression) {
		return new ExpressionCast(expression, SELF_TYPE);
	}

	/**
	 * Returns the property from {@code owner}
	 *
	 * @param owner    owner of the property
	 * @param property name of the property which will be returned
	 * @return new instance of the Property
	 */
	public static Variable property(Expression owner, String property) {
		return new Property(owner, property);
	}

	/**
	 * Returns the static field from {@code owner} class
	 *
	 * @param owner a class that is the owner of the field
	 * @param field name of the static field which will be returned
	 * @return new instance of the ExpressionStaticField
	 */
	public static Variable staticField(Class<?> owner, String field) {
		return new ExpressionStaticField(owner, field);
	}

	/**
	 * Returns the static field from self type (type that is being generated)
	 *
	 * @param field name of the static field which will be returned
	 * @return new instance of the ExpressionStaticField
	 */
	public static Variable staticField(String field) {
		return new ExpressionStaticField(null, field);
	}

	/**
	 * Returns current instance
	 *
	 * @return current instance of the Expression
	 */
	public static Expression self() {
		return new VarThis();
	}

	/**
	 * Returns value which ordinal number is 'argument'
	 *
	 * @param argument ordinal number in list of arguments
	 * @return new instance of the VarArg
	 */
	public static Variable arg(int argument) {
		return new VarArg(argument);
	}

	/**
	 * An expression that represents boolean {@code false} value
	 */
	public static Expression alwaysFalse() {
		return value(false);
	}

	/**
	 * An expression that represents boolean {@code true} value
	 */
	public static Expression alwaysTrue() {
		return value(true);
	}

	/**
	 * An expression that represents boolean negation
	 *
	 * @param expression an expression that is being negated
	 */
	public static Expression not(Expression expression) {
		return new ExpressionBooleanNot(expression);
	}

	/**
	 * Compares arguments using given compare operation
	 *
	 * @param eq    compare operation which will be used for the arguments
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	public static Expression cmp(CompareOperation eq, Expression left, Expression right) {
		return new ExpressionCmp(eq, left, right);
	}

	/**
	 * Compares to arguments for equality
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	public static Expression cmpEq(Expression left, Expression right) {
		return cmp(CompareOperation.EQ, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is greater than  or equal to the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	public static Expression cmpGe(Expression left, Expression right) {
		return cmp(CompareOperation.GE, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is less than or equal to the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	public static Expression cmpLe(Expression left, Expression right) {
		return cmp(CompareOperation.LE, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is less than the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	public static Expression cmpLt(Expression left, Expression right) {
		return cmp(CompareOperation.LT, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is greater than the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	public static Expression cmpGt(Expression left, Expression right) {
		return cmp(CompareOperation.GT, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is not equal to the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	public static Expression cmpNe(Expression left, Expression right) {
		return cmp(CompareOperation.NE, left, right);
	}

	/**
	 * Returns a result of logical 'AND' for the list of predicates
	 *
	 * @param predicates list of predicated
	 * @return an expression that represents a result of logical 'AND'
	 */
	public static Expression and(List<Expression> predicates) {
		return new ExpressionBooleanAnd(predicates);
	}

	/**
	 * @see #and(List)
	 */
	public static Expression and(Stream<Expression> predicates) {
		return and(predicates.collect(toList()));
	}

	/**
	 * @see #and(List)
	 */
	public static Expression and(Expression... predicates) {
		return and(asList(predicates));
	}

	/**
	 * Returns a result of logical 'AND' for two predicates
	 *
	 * @param predicate1 the first predicate
	 * @param predicate2 the second predicate
	 * @return an expression that represents a result of logical 'AND'
	 */
	public static Expression and(Expression predicate1, Expression predicate2) {
		return and(asList(predicate1, predicate2));
	}

	/**
	 * Returns a result of logical 'OR' for the list of predicates
	 *
	 * @param predicates list of predicates
	 * @return an expression that represents a result of logical 'OR'
	 */
	public static ExpressionBooleanOr or(List<Expression> predicates) {
		return new ExpressionBooleanOr(predicates);
	}

	/**
	 * @see #or(List)
	 */
	public static ExpressionBooleanOr or(Stream<Expression> predicates) {
		return or(predicates.collect(toList()));
	}

	/**
	 * @see #or(List)
	 */
	public static ExpressionBooleanOr or(Expression... predicates) {
		return or(asList(predicates));
	}

	/**
	 * Returns a result of logical 'OR' for two predicates
	 *
	 * @param predicate1 the first predicate
	 * @param predicate2 the second predicate
	 * @return an expression that represents a result of logical 'OR'
	 */
	public static ExpressionBooleanOr or(Expression predicate1, Expression predicate2) {
		return or(asList(predicate1, predicate2));
	}

	/**
	 * An expression that represents implementation of {@link #equals(Object)} method that uses
	 * given properties to check the equality
	 *
	 * @param properties list of properties
	 * @return an `equals()` implementation expression
	 */
	public static Expression equalsImpl(List<String> properties) {
		return and(properties.stream()
				.map(property -> cmpEq(
						property(self(), property),
						property(castIntoSelf(arg(0)), property))));
	}

	/**
	 * @see #equalsImpl(List)
	 */
	public static Expression equalsImpl(String... properties) {
		return equalsImpl(asList(properties));
	}

	/**
	 * An expression that represents implementation of {@link #toString()} method that uses
	 * given properties to build a `toString()` result
	 *
	 * @param properties list of properties
	 * @return a `toString()` implementation expression
	 */
	public static Expression toStringImpl(List<String> properties) {
		ExpressionToString toString = ExpressionToString.create();
		for (String property : properties) {
			toString.with(property + "=", property(self(), property));
		}
		return toString;
	}

	/**
	 * @see #toStringImpl(List)
	 */
	public static Expression toStringImpl(String... properties) {
		return toStringImpl(asList(properties));
	}

	/**
	 * Returns the string which was constructed by concatenation of all the arguments
	 *
	 * @param arguments list of arguments to be concatenated
	 * @return new instance of the ExpressionConcat
	 */
	public static Expression concat(List<Expression> arguments) {
		if (arguments.isEmpty()) return value("");
		return ExpressionConcat.create(arguments);
	}

	/**
	 * @see #concat(List)
	 */
	public static Expression concat(Expression... arguments) {
		return concat(asList(arguments));
	}

	/**
	 * An expression that represents implementation of {@link Comparator#compare(Object, Object)} method
	 * that uses given properties for comparison
	 *
	 * @param properties properties which will be compared
	 * @return a `compare(Object, Object)` implementation expression
	 */
	public static Expression compare(Class<?> type, List<String> properties) {
		ExpressionComparator comparator = ExpressionComparator.create();
		for (String property : properties) {
			comparator.with(leftProperty(type, property), rightProperty(type, property), true);
		}
		return comparator;
	}

	/**
	 * @see #compare(Class, List)
	 */
	public static Expression compare(Class<?> type, String... properties) {
		return compare(type, asList(properties));
	}

	/**
	 * An expression that represents implementation of {@link Comparable#compareTo(Object)} method
	 * that uses given properties for comparison
	 *
	 * @param properties properties which will be compared
	 * @return a `compareTo(Object)` implementation expression
	 */
	public static Expression compareToImpl(List<String> properties) {
		ExpressionComparator comparator = ExpressionComparator.create();
		for (String property : properties) {
			comparator.with(thisProperty(property), thatProperty(property), true);
		}
		return comparator;
	}

	/**
	 * @see #compareToImpl(List)
	 */
	public static Expression compareToImpl(String... properties) {
		return compareToImpl(asList(properties));
	}

	/**
	 * An exception that represents a hash code which was calculated using given {@code properties}
	 *
	 * @param properties list of properties which will be used to calculate hash code
	 * @return new instance of the ExpressionHash
	 */
	public static Expression hash(List<Expression> properties) {
		return new ExpressionHash(properties);
	}

	/**
	 * @see #hash(List)
	 */
	public static Expression hash(Expression... properties) {
		return hash(asList(properties));
	}

	/**
	 * @see #hash(List)
	 */
	public static Expression hashCodeImpl(String... properties) {
		return hashCodeImpl(asList(properties));
	}

	/**
	 * @see #hash(List)
	 */
	public static Expression hashCodeImpl(List<String> properties) {
		return new ExpressionHash(properties
				.stream()
				.map(property -> property(self(), property))
				.collect(toList()));
	}

	/**
	 * Unifies passed arithmetic types as one. Arithmetic types are all primitive types, excluding {@code boolean.class}
	 * A unified type represents a result type after applying arithmetic operations on given types
	 * as per <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-4.html">Java Language Specification</a>
	 * <p>
	 * Rules are:
	 * <ul>
	 *     <li>If there is {@code double.class} among types, unified type is  {@code double.class}</li>
	 *     <li>Else, if there is {@code float.class} among types, unified type is  {@code float.class}</li>
	 *     <li>Else, if there is {@code long.class} among types, unified type is  {@code long.class}</li>
	 *     <li>Else, unified type is  {@code int.class}</li>
	 * </ul>
	 *
	 * @param types arithmetic types to be unified
	 * @return a unified arithmetic type
	 * @throws IllegalArgumentException if a non-arithmetic type is passed to the method
	 */
	public static Class<?> unifyArithmeticTypes(Class<?>... types) {
		return ExpressionArithmeticOp.unifyArithmeticTypes(types);
	}

	/**
	 * @see #unifyArithmeticTypes(Class[])
	 */
	public static Class<?> unifyArithmeticTypes(List<Class<?>> types) {
		return ExpressionArithmeticOp.unifyArithmeticTypes(types.toArray(new Class<?>[0]));
	}

	/**
	 * An expression that represents a result of an arithmetic operation on two given operands
	 *
	 * @param op    an arithmetic operation
	 * @param left  left operand
	 * @param right right operand
	 * @see ArithmeticOperation
	 */
	public static Expression arithmeticOp(ArithmeticOperation op, Expression left, Expression right) {
		return new ExpressionArithmeticOp(op, left, right);
	}

	/**
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression arithmeticOp(String op, Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.operation(op), left, right);
	}

	/**
	 * An expression that represents a sum of two arguments
	 *
	 * @param left  first argument which will be added
	 * @param right second argument which will be added
	 * @return new instance of the ExpressionArithmeticOp
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression add(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.ADD, left, right);
	}

	/**
	 * An expression that represents an increment of an argument
	 *
	 * @param value value to be incremented
	 * @return an expression that represents an incremented value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression inc(Expression value) {
		return add(value, value(1));
	}

	/**
	 * An expression that represents a subtraction of one argument from the other
	 *
	 * @param left  first argument which represents a minuend
	 * @param right second argument which represents a subtrahend
	 * @return an expression that represents a difference between two arguments
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression sub(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.SUB, left, right);
	}

	/**
	 * An expression that represents a decrement of an argument
	 *
	 * @param value value to be decremented
	 * @return an expression that represents a decremented value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression dec(Expression value) {
		return sub(value, value(1));
	}

	/**
	 * An expression that represents a multiplication of two arguments
	 *
	 * @param left  first argument which will be multiplied
	 * @param right second argument which will be multiplied
	 * @return new instance of the ExpressionArithmeticOp
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression mul(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.MUL, left, right);
	}

	/**
	 * An expression that represents a division of one argument by the other
	 *
	 * @param left  first argument which represents a dividend
	 * @param right second argument which represents a divisor
	 * @return an expression that represents a division of two arguments
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression div(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.DIV, left, right);
	}

	/**
	 * An expression that represents a remainder of division of first argument
	 * by the other
	 *
	 * @param left  first argument which represents a dividend
	 * @param right second argument which represents a divisor
	 * @return an expression that represents a remainder of division
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression rem(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.REM, left, right);
	}

	public static Expression neg(Expression arg) {
		return new ExpressionNeg(arg);
	}

	/**
	 * An expression that represents a bitwise AND operation
	 *
	 * @param left  expression that represents the first operand
	 * @param right expression that represents the second operand
	 * @return expression that represents a result of bitwise AND operation
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression bitAnd(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.AND, left, right);
	}

	/**
	 * An expression that represents a bitwise OR operation
	 *
	 * @param left  expression that represents the first operand
	 * @param right expression that represents the second operand
	 * @return expression that represents a result of bitwise OR operation
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression bitOr(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.OR, left, right);
	}

	/**
	 * An expression that represents a bitwise XOR operation
	 *
	 * @param left  expression that represents the first operand
	 * @param right expression that represents the second operand
	 * @return expression that represents a result of bitwise XOR operation
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression bitXor(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.XOR, left, right);
	}

	/**
	 * An expression that represents an arithmetic left shift operation
	 *
	 * @param left  expression that represents a value that will be shifted
	 * @param right expression that represents a number of bits to be shifted
	 * @return expression that represents a left-shifted value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression shl(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.SHL, left, right);
	}

	/**
	 * An expression that represents an arithmetic right shift operation
	 *
	 * @param left  expression that represents a value that will be shifted
	 * @param right expression that represents a number of bits to be shifted
	 * @return expression that represents a right-shifted value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression shr(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.SHR, left, right);
	}

	/**
	 * An expression that represents a logical right shift operation
	 *
	 * @param left  expression that represents a value that will be shifted
	 * @param right expression that represents a number of bits to be shifted
	 * @return expression that represents a right-shifted value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	public static Expression ushr(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.USHR, left, right);
	}

	/**
	 * Returns new instance of class
	 *
	 * @param type   type of the constructor
	 * @param fields fields for constructor
	 * @return new instance of the ExpressionConstructor
	 */
	public static Expression constructor(Class<?> type, Expression... fields) {
		return new ExpressionConstructor(type, asList(fields));
	}

	public static Expression superConstructor(Expression... fields) {
		return new ExpressionSuperConstructor(asList(fields));
	}

	public static Expression callSuper(String methodName, Expression... arguments) {
		return new ExpressionCallSuper(methodName, arguments);
	}

	/**
	 * Returns a new {@link ExpressionCall expression call}
	 * which allows using static methods from other classes
	 *
	 * @param owner      owner of the method
	 * @param methodName name of the method in the class
	 * @param arguments  list of the arguments for the method
	 * @return new instance of the ExpressionCall
	 */
	public static Expression call(Expression owner, String methodName, Expression... arguments) {
		return new ExpressionCall(owner, methodName, arguments);
	}

	public static Expression ifThenElse(Expression condition, Expression left, Expression right) {
		return new ExpressionIf(condition, left, right);
	}

	public static Expression length(Expression field) {
		return new ExpressionLength(field);
	}

	public static Expression arrayNew(Class<?> type, Expression length) {
		return new ExpressionArrayNew(type, length);
	}

	public static Expression staticCall(Class<?> owner, String method, Expression... arguments) {
		return new ExpressionStaticCall(owner, method, asList(arguments));
	}

	public static Expression staticCallSelf(String method, Expression... arguments) {
		return new ExpressionStaticCallSelf(method, asList(arguments));
	}

	public static Expression arrayGet(Expression array, Expression index) {
		return new ExpressionArrayGet(array, index);
	}

	public static Expression isNull(Expression field) {
		return new ExpressionIsNull(field);
	}

	public static Expression isNotNull(Expression field) {
		return new ExpressionIsNotNull(field);
	}

	public static Expression nullRef(Class<?> type) {
		return new ExpressionNull(type);
	}

	public static Expression nullRef(Type type) {
		return new ExpressionNull(type);
	}

	public static Expression voidExp() {
		return ExpressionVoid.INSTANCE;
	}

	public static Expression throwException(Expression exception) {
		return new ExpressionThrow(exception);
	}

	public static Expression throwException(Class<? extends Throwable> exceptionClass) {
		return new ExpressionThrow(constructor(exceptionClass));
	}

	public static Expression throwException(Class<? extends Throwable> exceptionClass, Expression message) {
		return new ExpressionThrow(constructor(exceptionClass, message));
	}

	public static Expression throwException(Class<? extends Throwable> exceptionClass, String message) {
		return new ExpressionThrow(constructor(exceptionClass, value(message)));
	}

	public static Expression throwException(Throwable exception) {
		return new ExpressionThrow(value(exception));
	}

	public static Expression switchByIndex(Expression index, Expression... expressions) {
		return switchByIndex(index, asList(expressions), ExpressionSwitch.DEFAULT_EXPRESSION);
	}

	public static Expression switchByIndex(Expression index, List<Expression> expressions) {
		return switchByIndex(index, expressions, ExpressionSwitch.DEFAULT_EXPRESSION);
	}

	public static Expression switchByIndex(Expression index, List<Expression> expressions, Expression defaultExpression) {
		return new ExpressionSwitch(index, IntStream.range(0, expressions.size()).mapToObj(Expressions::value).collect(toList()), expressions, defaultExpression);
	}

	public static Expression switchByKey(Expression key, List<Expression> matchCases, List<Expression> matchExpressions) {
		return new ExpressionSwitch(key, matchCases, matchExpressions, ExpressionSwitch.DEFAULT_EXPRESSION);
	}

	public static Expression switchByKey(Expression key, List<Expression> matchCases, List<Expression> matchExpressions, Expression defaultExpression) {
		return new ExpressionSwitch(key, matchCases, matchExpressions, defaultExpression);
	}

	public static Expression switchByKey(Expression key, Map<Expression, Expression> cases) {
		return switchByKey(key, cases, ExpressionSwitch.DEFAULT_EXPRESSION);
	}

	public static Expression switchByKey(Expression key, Map<Expression, Expression> cases, Expression defaultExpression) {
		List<Expression> matchCases = new ArrayList<>();
		List<Expression> matchExpressions = new ArrayList<>();
		for (Map.Entry<Expression, Expression> entry : cases.entrySet()) {
			matchCases.add(entry.getKey());
			matchExpressions.add(entry.getValue());
		}
		return new ExpressionSwitch(key, matchCases, matchExpressions, defaultExpression);
	}

	public static Expression arraySet(Expression array, Expression position, Expression newElement) {
		return new ExpressionArraySet(array, position, newElement);
	}

	public static Expression loop(Expression body) {
		return loop(body, voidExp());
	}

	public static Expression loop(Expression condition, Expression body) {
		return new ExpressionLoop(condition, body);
//		return loop(ifThenElse(condition, sequence(body, value(true)), value(false)));
	}

	public static Expression iterate(Expression from, Expression to, UnaryOperator<Expression> action) {
		return new ExpressionIterate(from, to, action);
//		return let(new Expression[]{from, to},
//				vars -> loop(cmpLt(vars[0], vars[1]),
//						sequence(action.apply(vars[0]), set(vars[0], inc(vars[0])))));
	}

	public static Expression iterateArray(Variable array, UnaryOperator<Expression> action) {
		return iterate(value(0), length(array),
				i -> action.apply(arrayGet(array, i)));
	}

	public static Expression iterateList(Variable list, UnaryOperator<Expression> action) {
		return iterate(value(0), length(list),
				i -> action.apply(call(list, "get", i)));
	}

	public static Expression iterateIterable(Expression iterable, UnaryOperator<Expression> action) {
		return let(call(iterable, "iterator"),
				it -> iterateIterator(it, action));
	}

	public static Expression iterateIterator(Variable iterator, UnaryOperator<Expression> action) {
		return loop(call(iterator, "hasNext"), let(call(iterator, "next"), action::apply));
	}

	public static Expression iterateMap(Expression map, UnaryOperator<Expression> keyAction, UnaryOperator<Expression> valueAction) {
		return iterateMap(map, (key, value) -> sequence(keyAction.apply(key), valueAction.apply(value)));
	}

	public static Expression iterateMap(Expression map, BinaryOperator<Expression> action) {
		return iterateIterable(call(map, "entrySet"),
				it -> action.apply(
						call(cast(it, Map.Entry.class), "getKey"),
						call(cast(it, Map.Entry.class), "getValue")));
	}

	public static Expression iterateMapKeys(Expression map, UnaryOperator<Expression> action) {
		return iterateIterable(call(map, "keySet"), action);
	}

	public static Expression iterateMapValues(Expression map, UnaryOperator<Expression> action) {
		return iterateIterable(call(map, "values"), action);
	}

}
