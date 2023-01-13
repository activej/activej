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

import io.activej.codegen.Context;
import io.activej.codegen.operation.ArithmeticOperation;
import io.activej.codegen.operation.CompareOperation;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static io.activej.codegen.expression.Expression_Cast.SELF_TYPE;
import static io.activej.codegen.expression.Expression_Compare.*;
import static io.activej.codegen.operation.CompareOperation.*;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toList;
import static org.objectweb.asm.Type.getType;

/**
 * These are basic methods for functions that allow to read the state and to process data
 */
public interface Expression {
	/**
	 * Processes data and returns its type
	 *
	 * @param ctx information about a dynamic class
	 * @return type of the processes data or {@code null} if expression throws an exception
	 */
	@Nullable Type load(Context ctx);

	/**
	 * Returns a new constant for the value
	 *
	 * @param value value which will be created as constant
	 * @return new instance of the ExpressionConstant
	 */
	static Expression value(Object value) {
		return new Expression_Constant(value);
	}

	/**
	 * Returns a new constant for the value of a given type
	 *
	 * @param value value which will be created as constant
	 * @param type  actual type of value
	 * @return new instance of the ExpressionConstant
	 */
	static Expression value(Object value, Class<?> type) {
		return new Expression_Constant(value, type);
	}

	/**
	 * @see #sequence(List)
	 */
	static Expression sequence(Expression... parts) {
		return new Expression_Sequence(List.of(parts));
	}

	/**
	 * Returns a sequence of operations which will be processed one after the other
	 *
	 * @param parts list of operations
	 * @return new instance of the ExpressionSequence
	 */
	static Expression sequence(List<Expression> parts) {
		List<Expression> list = new ArrayList<>(parts.size());
		for (Expression part : parts) {
			if (part instanceof Expression_Sequence) {
				list.addAll(((Expression_Sequence) part).expressions);
			} else {
				list.add(part);
			}
		}
		return new Expression_Sequence(list);
	}

	/**
	 * Returns a sequence of operations which will be processed one after the other.
	 * Operations should be added to a list that is consumed by the given consumer
	 *
	 * @param consumer consumer of a list of operations. Operations added to the list
	 *                 will be processed in a sequence
	 * @return new instance of the ExpressionSequence
	 */
	static Expression sequence(Consumer<List<Expression>> consumer) {
		List<Expression> seq = new ArrayList<>();
		consumer.accept(seq);
		return new Expression_Sequence(seq);
	}

	/**
	 * Returns a sequence of operations which will be processed one after the other.
	 * Operations should be added to a list that is mapped by the given function. The last
	 * operation should be returned as a result of a given function
	 *
	 * @param fn function that transforms a list of operations to an expression
	 * @return new instance of the ExpressionSequence
	 */
	static Expression sequence(Function<List<Expression>, Expression> fn) {
		List<Expression> seq = new ArrayList<>();
		Expression result = fn.apply(seq);
		seq.add(result);
		return new Expression_Sequence(seq);
	}

	/**
	 * Returns an expression that represents a new local variables with some action applied to it
	 *
	 * @param expression new local variable
	 * @param fn         function applied to a new local variable
	 * @return an expression that represents a new local variable with some action applied to it
	 */
	static Expression let(Expression expression, Function<Variable, Expression> fn) {
		Variable variable = new Expression_Let(expression);
		return sequence(variable, fn.apply(variable));
	}

	/**
	 * Returns an expression that represents new local variables with some action applied to them
	 *
	 * @param expressions list of new local variables
	 * @param fn          function applied to a list of new local variables
	 * @return an expression that represents new local variables with some action applied to them
	 */
	static Expression let(List<Expression> expressions, Function<List<Variable>, Expression> fn) {
		List<Variable> variables = expressions.stream().map(Expression_Let::new).collect(toList());
		List<Expression> sequence = new ArrayList<>(expressions.size() + 1);
		sequence.addAll(variables);
		sequence.add(fn.apply(variables));
		return sequence(sequence);
	}

	/**
	 * @see #let(List, Function)
	 */
	static Expression let(Expression[] expressions, Function<Variable[], Expression> fn) {
		return let(List.of(expressions), variables -> fn.apply(variables.toArray(new Variable[0])));
	}

	/**
	 * Sets the value from the argument 'from' to the argument 'to'
	 *
	 * @param to   variable which will be changed
	 * @param from variable which changes
	 * @return new instance of the Expression
	 */
	static Expression set(StoreDef to, Expression from) {
		return new Expression_Set(to, from);
	}

	/**
	 * Casts expression to the type
	 *
	 * @param expression expressions which will be cast
	 * @param type       expression will be cast to the 'type'
	 * @return new instance of the Expression which is cast to the type
	 */
	static Expression cast(Expression expression, Class<?> type) {
		return new Expression_Cast(expression, getType(type));
	}

	/**
	 * Casts a given to a self type (a type that is being generated)
	 *
	 * @param expression an original expression
	 * @return expression cast to a self type
	 */
	static Expression castIntoSelf(Expression expression) {
		return new Expression_Cast(expression, SELF_TYPE);
	}

	/**
	 * Returns the property from {@code owner}
	 *
	 * @param owner    owner of the property
	 * @param property name of the property which will be returned
	 * @return new instance of the Property
	 */
	static Variable property(Expression owner, String property) {
		return new Property(owner, property);
	}

	/**
	 * Returns the static field from {@code owner} class
	 *
	 * @param owner a class that is the owner of the field
	 * @param field name of the static field which will be returned
	 * @return new instance of the ExpressionStaticField
	 */
	static Variable staticField(Class<?> owner, String field) {
		return new Expression_StaticField(owner, field);
	}

	/**
	 * Returns the static field from self type (type that is being generated)
	 *
	 * @param field name of the static field which will be returned
	 * @return new instance of the ExpressionStaticField
	 */
	static Variable staticField(String field) {
		return new Expression_StaticField(null, field);
	}

	/**
	 * Returns current instance
	 *
	 * @return current instance of the Expression
	 */
	static Expression self() {
		return new Expression_VarThis();
	}

	/**
	 * Returns value which ordinal number is 'argument'
	 *
	 * @param argument ordinal number in list of arguments
	 * @return new instance of the VarArg
	 */
	static Variable arg(int argument) {
		return new VarArg(argument);
	}

	/**
	 * An expression that represents boolean negation
	 *
	 * @param expression an expression that is being negated
	 */
	static Expression not(Expression expression) {
		return ifElse(expression, value(false), value(true));
	}

	/**
	 * Compares to arguments for equality
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	static Expression isEq(Expression left, Expression right) {
		return isCmp(EQ, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is not equal to the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	static Expression isNe(Expression left, Expression right) {
		return isCmp(NE, left, right);
	}

	static Expression isRefEq(Expression left, Expression right) {
		return isCmp(REF_EQ, left, right);
	}

	static Expression isRefNe(Expression left, Expression right) {
		return isCmp(REF_NE, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is greater than  or equal to the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	static Expression isGe(Expression left, Expression right) {
		return isCmp(GE, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is less than or equal to the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	static Expression isLe(Expression left, Expression right) {
		return isCmp(LE, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is less than the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	static Expression isLt(Expression left, Expression right) {
		return isCmp(LT, left, right);
	}

	/**
	 * Compares two arguments for whether the first argument is greater than the second argument
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return an expression that represents comparison
	 */
	static Expression isGt(Expression left, Expression right) {
		return isCmp(GT, left, right);
	}

	private static Expression isCmp(CompareOperation op, Expression left, Expression right) {
		return ifObjCmp(op, left, right, value(true), value(false));
	}

	/**
	 * Returns a result of logical 'AND' for the list of predicates
	 *
	 * @param predicates list of predicated
	 * @return an expression that represents a result of logical 'AND'
	 */
	static Expression and(List<Expression> predicates) {
		return new Expression_BooleanAnd(predicates);
	}

	/**
	 * @see #and(List)
	 */
	static Expression and(Stream<Expression> predicates) {
		return and(predicates.collect(toList()));
	}

	/**
	 * @see #and(List)
	 */
	static Expression and(Expression... predicates) {
		return and(List.of(predicates));
	}

	/**
	 * Returns a result of logical 'AND' for two predicates
	 *
	 * @param predicate1 the first predicate
	 * @param predicate2 the second predicate
	 * @return an expression that represents a result of logical 'AND'
	 */
	static Expression and(Expression predicate1, Expression predicate2) {
		return and(List.of(predicate1, predicate2));
	}

	/**
	 * Returns a result of logical 'OR' for the list of predicates
	 *
	 * @param predicates list of predicates
	 * @return an expression that represents a result of logical 'OR'
	 */
	static Expression_BooleanOr or(List<Expression> predicates) {
		return new Expression_BooleanOr(predicates);
	}

	/**
	 * @see #or(List)
	 */
	static Expression_BooleanOr or(Stream<Expression> predicates) {
		return or(predicates.collect(toList()));
	}

	/**
	 * @see #or(List)
	 */
	static Expression_BooleanOr or(Expression... predicates) {
		return or(List.of(predicates));
	}

	/**
	 * Returns a result of logical 'OR' for two predicates
	 *
	 * @param predicate1 the first predicate
	 * @param predicate2 the second predicate
	 * @return an expression that represents a result of logical 'OR'
	 */
	static Expression_BooleanOr or(Expression predicate1, Expression predicate2) {
		return or(List.of(predicate1, predicate2));
	}

	static Expression tableSwitch(Expression key, Map<Integer, Expression> cases, Expression defaultExpression) {
		List<Map.Entry<Integer, Expression>> list = new ArrayList<>(cases.entrySet());
		list.sort(comparingInt(Map.Entry::getKey));
		int[] keys = new int[list.size()];
		Expression[] expressionsArray = new Expression[list.size()];
		for (int i = 0; i < list.size(); i++) {
			keys[i] = list.get(i).getKey();
			expressionsArray[i] = list.get(i).getValue();
		}
		return new Expression_TableSwitch(key, keys, expressionsArray, defaultExpression);
	}

	/**
	 * Unifies passed arithmetic types as one. Arithmetic types are all primitive types, excluding {@code boolean.class}
	 * A unified type represents a result type after applying arithmetic operations on given types
	 * as per <a href="https://docs.oracle.com/javase/specs/jls/se17/html/jls-4.html">Java Language Specification</a>
	 * <p>
	 * Rules are:
	 * <ul>
	 *     <li>If there is {@code double.class} among types, unified type is  {@code double.class}</li>
	 *     <li>Else, if there is {@code float.class} among types, unified type is  {@code float.class}</li>
	 *     <li>Else, if there is {@code long.class} among types, unified type is  {@code long.class}</li>
	 *     <li>Else, unified type is {@code int.class}</li>
	 * </ul>
	 *
	 * @param types arithmetic types to be unified
	 * @return a unified arithmetic type
	 * @throws IllegalArgumentException if a non-arithmetic type is passed to the method
	 */
	static Class<?> unifyArithmeticTypes(Class<?>... types) {
		return Expression_ArithmeticOp.unifyArithmeticTypes(types);
	}

	/**
	 * @see #unifyArithmeticTypes(Class[])
	 */
	static Class<?> unifyArithmeticTypes(List<Class<?>> types) {
		return Expression_ArithmeticOp.unifyArithmeticTypes(types.toArray(new Class<?>[0]));
	}

	/**
	 * An expression that represents a result of an arithmetic operation on two given operands
	 *
	 * @param op    an arithmetic operation
	 * @param left  left operand
	 * @param right right operand
	 * @see ArithmeticOperation
	 */
	static Expression arithmeticOp(ArithmeticOperation op, Expression left, Expression right) {
		return new Expression_ArithmeticOp(op, left, right);
	}

	/**
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression arithmeticOp(String op, Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.operation(op), left, right);
	}

	/**
	 * An expression that represents a sum of two arguments
	 *
	 * @param left  first argument which will be added
	 * @param right second argument which will be added
	 * @return new instance of the ExpressionArithmeticOp
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression add(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.ADD, left, right);
	}

	/**
	 * An expression that represents an increment of an argument
	 *
	 * @param value value to be incremented
	 * @return an expression that represents an incremented value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression inc(Expression value) {
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
	static Expression sub(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.SUB, left, right);
	}

	/**
	 * An expression that represents a decrement of an argument
	 *
	 * @param value value to be decremented
	 * @return an expression that represents a decremented value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression dec(Expression value) {
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
	static Expression mul(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.MUL, left, right);
	}

	/**
	 * An expression that represents a division of one argument by the other
	 *
	 * @param left  first argument which represents a dividend
	 * @param right second argument which represents a divisor
	 * @return an expression that represents a division of two arguments
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression div(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.DIV, left, right);
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
	static Expression rem(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.REM, left, right);
	}

	static Expression neg(Expression arg) {
		return new Expression_Neg(arg);
	}

	/**
	 * An expression that represents a bitwise AND operation
	 *
	 * @param left  expression that represents the first operand
	 * @param right expression that represents the second operand
	 * @return expression that represents a result of bitwise AND operation
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression bitAnd(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.AND, left, right);
	}

	/**
	 * An expression that represents a bitwise OR operation
	 *
	 * @param left  expression that represents the first operand
	 * @param right expression that represents the second operand
	 * @return expression that represents a result of bitwise OR operation
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression bitOr(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.OR, left, right);
	}

	/**
	 * An expression that represents a bitwise XOR operation
	 *
	 * @param left  expression that represents the first operand
	 * @param right expression that represents the second operand
	 * @return expression that represents a result of bitwise XOR operation
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression bitXor(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.XOR, left, right);
	}

	/**
	 * An expression that represents an arithmetic left shift operation
	 *
	 * @param left  expression that represents a value that will be shifted
	 * @param right expression that represents a number of bits to be shifted
	 * @return expression that represents a left-shifted value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression shl(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.SHL, left, right);
	}

	/**
	 * An expression that represents an arithmetic right shift operation
	 *
	 * @param left  expression that represents a value that will be shifted
	 * @param right expression that represents a number of bits to be shifted
	 * @return expression that represents a right-shifted value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression shr(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.SHR, left, right);
	}

	/**
	 * An expression that represents a logical right shift operation
	 *
	 * @param left  expression that represents a value that will be shifted
	 * @param right expression that represents a number of bits to be shifted
	 * @return expression that represents a right-shifted value
	 * @see #arithmeticOp(ArithmeticOperation, Expression, Expression)
	 */
	static Expression ushr(Expression left, Expression right) {
		return new Expression_ArithmeticOp(ArithmeticOperation.USHR, left, right);
	}

	/**
	 * Returns new instance of class
	 *
	 * @param type   type of the constructor
	 * @param fields fields for constructor
	 * @return new instance of the ExpressionConstructor
	 */
	static Expression constructor(Class<?> type, Expression... fields) {
		return new Expression_Constructor(type, List.of(fields));
	}

	static Expression superConstructor(Expression... fields) {
		return new Expression_SuperConstructor(List.of(fields));
	}

	static Expression callSuper(String methodName, Expression... arguments) {
		return new Expression_CallSuper(methodName, arguments);
	}

	/**
	 * Returns a new {@link Expression_Call expression call}
	 * which allows using static methods from other classes
	 *
	 * @param owner      owner of the method
	 * @param methodName name of the method in the class
	 * @param arguments  list of the arguments for the method
	 * @return new instance of the ExpressionCall
	 */
	static Expression call(Expression owner, String methodName, Expression... arguments) {
		return new Expression_Call(owner, methodName, arguments);
	}

	static Expression ifNull(Expression value, Expression expressionTrue, Expression expressionFalse) {
		return new Expression_IfNull(value, expressionTrue, expressionFalse);
	}

	static Expression isNull(Expression value) {
		return ifNull(value, value(true), value(false));
	}

	static Expression ifNonNull(Expression value, Expression expressionTrue, Expression expressionFalse) {
		return new Expression_IfNonNull(value, expressionTrue, expressionFalse);
	}

	static Expression isNotNull(Expression value) {
		return ifNonNull(value, value(true), value(false));
	}

	static Expression ifElse(Expression value, Expression expressionTrue, Expression expressionFalse) {
		return new Expression_IfZCmp(value, NE, expressionTrue, expressionFalse);
	}

	static Expression ifEq(Expression value1, Expression value2, Expression expressionTrue, Expression expressionFalse) {
		return ifObjCmp(EQ, value1, value2, expressionTrue, expressionFalse);
	}

	static Expression ifNe(Expression value1, Expression value2, Expression expressionTrue, Expression expressionFalse) {
		return ifObjCmp(NE, value1, value2, expressionTrue, expressionFalse);
	}

	static Expression ifRefEq(Expression value1, Expression value2, Expression expressionTrue, Expression expressionFalse) {
		return ifObjCmp(REF_EQ, value1, value2, expressionTrue, expressionFalse);
	}

	static Expression ifRefNe(Expression value1, Expression value2, Expression expressionTrue, Expression expressionFalse) {
		return ifObjCmp(REF_NE, value1, value2, expressionTrue, expressionFalse);
	}

	static Expression ifLt(Expression value1, Expression value2, Expression expressionTrue, Expression expressionFalse) {
		return ifObjCmp(LT, value1, value2, expressionTrue, expressionFalse);
	}

	static Expression ifGt(Expression value1, Expression value2, Expression expressionTrue, Expression expressionFalse) {
		return ifObjCmp(GT, value1, value2, expressionTrue, expressionFalse);
	}

	static Expression ifLe(Expression value1, Expression value2, Expression expressionTrue, Expression expressionFalse) {
		return ifObjCmp(LE, value1, value2, expressionTrue, expressionFalse);
	}

	static Expression ifGe(Expression value1, Expression value2, Expression expressionTrue, Expression expressionFalse) {
		return ifObjCmp(GE, value1, value2, expressionTrue, expressionFalse);
	}

	private static Expression ifObjCmp(CompareOperation op, Expression value1, Expression value2, Expression expressionTrue, Expression expressionFalse) {
		return new Expression_IfObjCmp(op, value1, value2, expressionTrue, expressionFalse);
	}

	static Expression length(Expression field) {
		return new Expression_Length(field);
	}

	static Expression arrayNew(Class<?> type, Expression length) {
		return new Expression_ArrayNew(type, length);
	}

	static Expression staticCall(Class<?> owner, String method, Expression... arguments) {
		return new Expression_StaticCall(owner, method, List.of(arguments));
	}

	static Expression staticCallSelf(String method, Expression... arguments) {
		return new Expression_StaticCallSelf(method, List.of(arguments));
	}

	static Expression arrayGet(Expression array, Expression index) {
		return new Expression_ArrayGet(array, index);
	}

	static Expression nullRef(Class<?> type) {
		return new Expression_Null(type);
	}

	static Expression nullRef(Type type) {
		return new Expression_Null(type);
	}

	static Expression voidExp() {
		return Expression_Void.INSTANCE;
	}

	static Expression throwException(Expression exception) {
		return new Expression_Throw(exception);
	}

	static Expression throwException(Class<? extends Throwable> exceptionClass) {
		return new Expression_Throw(constructor(exceptionClass));
	}

	static Expression throwException(Class<? extends Throwable> exceptionClass, Expression message) {
		return new Expression_Throw(constructor(exceptionClass, message));
	}

	static Expression throwException(Class<? extends Throwable> exceptionClass, String message) {
		return new Expression_Throw(constructor(exceptionClass, value(message)));
	}

	static Expression throwException(Throwable exception) {
		return new Expression_Throw(value(exception));
	}

	static Expression arraySet(Expression array, Expression position, Expression newElement) {
		return new Expression_ArraySet(array, position, newElement);
	}

	static Expression loop(Expression body) {
		return loop(body, voidExp());
	}

	static Expression loop(Expression condition, Expression body) {
		return new Expression_Loop(condition, body);
//		return loop(ifThenElse(condition, sequence(body, value(true)), value(false)));
	}

	static Expression iterate(Expression from, Expression to, UnaryOperator<Expression> action) {
		return new Expression_Iterate(from, to, action);
//		return let(new Expression[]{from, to},
//				vars -> loop(cmpLt(vars[0], vars[1]),
//						sequence(action.apply(vars[0]), set(vars[0], inc(vars[0])))));
	}

	static Expression iterateArray(Variable array, UnaryOperator<Expression> action) {
		return iterate(value(0), length(array),
				i -> action.apply(arrayGet(array, i)));
	}

	static Expression iterateList(Variable list, UnaryOperator<Expression> action) {
		return iterate(value(0), length(list),
				i -> action.apply(call(list, "get", i)));
	}

	static Expression iterateIterable(Expression iterable, UnaryOperator<Expression> action) {
		return let(call(iterable, "iterator"),
				it -> iterateIterator(it, action));
	}

	static Expression iterateIterator(Variable iterator, UnaryOperator<Expression> action) {
		return loop(call(iterator, "hasNext"), let(call(iterator, "next"), action::apply));
	}

	static Expression iterateMap(Expression map, UnaryOperator<Expression> keyAction, UnaryOperator<Expression> valueAction) {
		return iterateMap(map, (key, value) -> sequence(keyAction.apply(key), valueAction.apply(value)));
	}

	static Expression iterateMap(Expression map, BinaryOperator<Expression> action) {
		return iterateIterable(call(map, "entrySet"),
				it -> action.apply(
						call(cast(it, Map.Entry.class), "getKey"),
						call(cast(it, Map.Entry.class), "getValue")));
	}

	static Expression iterateMapKeys(Expression map, UnaryOperator<Expression> action) {
		return iterateIterable(call(map, "keySet"), action);
	}

	static Expression iterateMapValues(Expression map, UnaryOperator<Expression> action) {
		return iterateIterable(call(map, "values"), action);
	}

	/**
	 * Returns the string which was constructed by concatenation of all the arguments
	 *
	 * @param arguments list of arguments to be concatenated
	 * @return new instance of the ExpressionConcat
	 */
	static Expression concat(List<Expression> arguments) {
		if (arguments.isEmpty()) return value("");
		return new Expression_Concat(arguments);
	}

	/**
	 * @see #concat(List)
	 */
	static Expression concat(Expression... arguments) {
		return concat(List.of(arguments));
	}

	static Expression hashCode(Expression value) {
		return call(value, "hashCode");
	}

	static Expression compare(Expression left, Expression right) {
		return call(cast(left, Comparable.class), "compareTo", cast(right, Comparable.class));
	}

	static Expression_HashCode hashCodeImpl() {
		return new Expression_HashCode();
	}

	static Expression hashCodeImpl(List<String> fields) {
		return hashCodeImpl().withFields(fields);
	}

	static Expression hashCodeImpl(String... fields) {
		return hashCodeImpl().withFields(fields);
	}

	static Expression_Equals equalsImpl() {
		return new Expression_Equals();
	}

	static Expression equalsImpl(List<String> fields) {
		return ifNull(arg(0),
				value(false),
				let(castIntoSelf(arg(0)), that -> and(fields.stream()
						.map(field -> let(
								new Expression[]{
										property(self(), field),
										property(that, field)
								},
								vars ->
										ifNull(vars[0],
												isNull(vars[1]),
												ifNull(vars[1], value(false), isEq(vars[0], vars[1]))))))));
	}

	static Expression equalsImpl(String... fields) {
		return equalsImpl(List.of(fields));
	}

	static Expression_ToString toStringImpl() {
		return new Expression_ToString();
	}

	static Expression_ToString toStringImpl(String begin, String end, @Nullable String nameSeparator, String valueSeparator) {
		return new Expression_ToString(begin, end, nameSeparator, valueSeparator);
	}

	static Expression toStringImpl(List<String> fields) {
		Expression_ToString toString = toStringImpl();
		for (String field : fields) {
			toString.with(field, property(self(), field));
		}
		return toString;
	}

	static Expression toStringImpl(String... fields) {
		return toStringImpl(List.of(fields));
	}

	static Expression_Compare compare() {
		return new Expression_Compare();
	}

	static Expression comparableImpl(List<String> fields) {
		Expression_Compare comparator = Expression.compare();
		for (String field : fields) {
			comparator.with(thisProperty(field), thatProperty(field), true);
		}
		return comparator;
	}

	static Expression comparableImpl(String... fields) {
		return comparableImpl(List.of(fields));
	}

	static Expression comparatorImpl(Class<?> type, List<String> fields) {
		Expression_Compare comparator = Expression.compare();
		for (String field : fields) {
			comparator.with(leftProperty(type, field), rightProperty(type, field), true);
		}
		return comparator;
	}

}
