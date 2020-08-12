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
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.Iterator;
import java.util.function.Function;

import static org.objectweb.asm.Type.*;

public abstract class AbstractExpressionIteratorForEach implements Expression {
	protected final Expression collection;
	protected final Class<?> type;
	protected final Function<Expression, Expression> forEach;

	protected AbstractExpressionIteratorForEach(Expression collection, Class<?> type, Function<Expression, Expression> forEach) {
		this.collection = collection;
		this.type = type;
		this.forEach = forEach;
	}

	protected abstract Expression getValue(VarLocal varIt);

	@Override
	public final Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		Type collectionType = collection.load(ctx);
		if (collectionType.getSort() == ARRAY) {
			return arrayForEach(ctx, g, labelLoop, labelExit);
		}

		VarLocal varIter = ctx.newLocal(getType(Iterator.class));

		Class<?> t = ctx.toJavaType(collectionType);
		//noinspection StatementWithEmptyBody
		if (Iterator.class.isAssignableFrom(t)) {
			// do nothing
		} else {
			ctx.invoke(collectionType, "iterator");
		}
		varIter.store(ctx);

		g.mark(labelLoop);

		ctx.invoke(varIter, "hasNext");
		g.push(false);
		g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.EQ, labelExit);

		ctx.cast(ctx.invoke(varIter, "next"), getType(type));
		VarLocal it = ctx.newLocal(getType(type));
		it.store(ctx);

		Type forEachType = forEach.apply(getValue(it)).load(ctx);
		if (forEachType.getSize() == 1)
			g.pop();
		if (forEachType.getSize() == 2)
			g.pop2();

		g.goTo(labelLoop);
		g.mark(labelExit);
		return VOID_TYPE;

	}

	public Type arrayForEach(Context ctx, GeneratorAdapter g, Label labelLoop, Label labelExit) {
		VarLocal len = ctx.newLocal(INT_TYPE);
		g.arrayLength();
		len.store(ctx);

		g.push(0);
		VarLocal varPosition = ctx.newLocal(INT_TYPE);
		varPosition.store(ctx);

		g.mark(labelLoop);

		varPosition.load(ctx);
		len.load(ctx);

		g.ifCmp(INT_TYPE, GeneratorAdapter.GE, labelExit);

		collection.load(ctx);
		varPosition.load(ctx);
		g.arrayLoad(getType(type));

		VarLocal it = ctx.newLocal(getType(type));
		it.store(ctx);

		forEach.apply(getValue(it)).load(ctx);

		varPosition.load(ctx);
		g.push(1);
		g.math(GeneratorAdapter.ADD, INT_TYPE);
		varPosition.store(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);

		return VOID_TYPE;
	}
}
