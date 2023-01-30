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

package io.activej.test;

import io.activej.inject.Injector;
import io.activej.inject.InstanceInjector;
import io.activej.inject.Key;
import io.activej.inject.binding.Binding;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.Modules;
import io.activej.inject.util.ReflectionUtils;
import io.activej.test.rules.LambdaStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.lang.annotation.Annotation;
import java.util.*;

import static io.activej.common.Utils.union;
import static io.activej.types.Types.parameterizedType;
import static java.util.stream.Collectors.toSet;

public class ActiveJRunner extends BlockJUnit4ClassRunner {
	private final Set<FrameworkMethod> surroundings = new HashSet<>();
	private final Set<Key<?>> staticDependencies;

	private Module currentModule;
	private Set<Key<?>> currentDependencies;

	protected Injector currentInjector;

	public ActiveJRunner(Class<?> cls) throws InitializationError {
		super(cls);

		surroundings.addAll(getTestClass().getAnnotatedMethods(Before.class));
		surroundings.addAll(getTestClass().getAnnotatedMethods(After.class));

		staticDependencies = surroundings.stream()
				.flatMap(m -> Arrays.stream(ReflectionUtils.toDependencies(cls, m.getMethod())))
				.collect(toSet());
	}

	// runChild is always called before createTest
	@Override
	protected void runChild(FrameworkMethod method, RunNotifier notifier) {
		Description description = describeChild(method);
		if (isIgnored(method)) {
			notifier.fireTestIgnored(description);
			return;
		}
		try {
			Class<?> cls = getTestClass().getJavaClass();

			Set<Module> modules = new HashSet<>();

			addClassModules(modules, cls); // add modules from class annotation
			addMethodModules(modules, method); // add modules from current test method
			for (FrameworkMethod m : surroundings) { // add modules from befores and afters
				addMethodModules(modules, m);
			}
			currentModule = Modules.combine(modules);

			currentDependencies =
					Arrays.stream(ReflectionUtils.toDependencies(cls, method.getMethod()))
							.collect(toSet());

		} catch (ExceptionInInitializerError e) {
			Throwable cause = e.getCause();
			notifier.fireTestFailure(new Failure(description, cause != null ? cause : e));
			return;
		} catch (Exception e) {
			notifier.fireTestFailure(new Failure(description, e));
			return;
		}

		runLeaf(methodBlock(method), description, notifier);
	}

	private static void addClassModules(Set<Module> modules, Class<?> cls) throws ExceptionInInitializerError, ReflectiveOperationException {
		while (cls != null) {
			UseModules useModules = cls.getAnnotation(UseModules.class);
			if (useModules != null) {
				for (Class<? extends Module> moduleClass : useModules.value()) {
					modules.add(moduleClass.getDeclaredConstructor().newInstance());
				}
			}
			cls = cls.getSuperclass();
		}
	}

	private static void addMethodModules(Set<Module> modules, FrameworkMethod method) throws ExceptionInInitializerError, ReflectiveOperationException {
		UseModules useModules = method.getMethod().getAnnotation(UseModules.class);
		if (useModules == null) {
			return;
		}
		for (Class<? extends Module> moduleClass : useModules.value()) {
			modules.add(moduleClass.getDeclaredConstructor().newInstance());
		}
	}

	public static final class DependencyToken {}

	// createTest is always called after runChild
	@Override
	protected Object createTest() throws Exception {
		Object instance = super.createTest();

		Key<Object> self = Key.ofType(getTestClass().getJavaClass());

		Key<InstanceInjector<Object>> instanceInjectorKey = Key.ofType(parameterizedType(InstanceInjector.class, getTestClass().getJavaClass()));

		currentInjector = Injector.of(currentModule, ModuleBuilder.create()
				// scan the test class for @Provide's
				.scan(instance)

				// bind unusable private type with all the extra dependencies so that injector knows about them
				.bind(DependencyToken.class).to(Binding.<DependencyToken>to(() -> {
					throw new AssertionError("should never be instantiated");
				}).addDependencies(union(currentDependencies, staticDependencies)))

				// bind test class to existing instance if whoever needs it (e.g. for implicit parameter of non-static inner classes)
				.bind(self).toInstance(instance)

				// and generate one of those to handle @Inject's
				.bind(instanceInjectorKey)
				.build());

		// creating eager stuff right away
		currentInjector.createEagerInstances();

		// and also actually handle the @Inject's
		currentInjector.getInstance(instanceInjectorKey).injectInto(instance);

		return instance;
	}

	// allow test methods to have any arguments
	@Override
	protected void validatePublicVoidNoArgMethods(Class<? extends Annotation> annotation, boolean isStatic, List<Throwable> errors) {
		for (FrameworkMethod testMethod : getTestClass().getAnnotatedMethods(annotation)) {
			testMethod.validatePublicVoid(isStatic, errors);
		}
	}

	// invoke methods with args fetched from current injector
	@Override
	protected Statement methodInvoker(FrameworkMethod method, Object test) {
		return new LambdaStatement(() -> method.invokeExplosively(test, getArgs(method)));
	}

	protected Object[] getArgs(FrameworkMethod method) {
		return Arrays.stream(ReflectionUtils.toDependencies(getTestClass().getJavaClass(), method.getMethod()))
				.map(dependency -> currentInjector.getInstance(dependency))
				.toArray(Object[]::new);
	}

	// same as original except that methods are called like in methodInvoker method
	@Override
	protected Statement withBefores(FrameworkMethod method, Object target, Statement test) {
		List<FrameworkMethod> methods = getTestClass().getAnnotatedMethods(Before.class);
		if (methods.isEmpty()) {
			return test;
		}
		return new LambdaStatement(() -> {
			for (FrameworkMethod m : methods) {
				m.invokeExplosively(target, getArgs(m));
			}
			test.evaluate();
		});
	}

	// same as above
	@Override
	protected Statement withAfters(FrameworkMethod method, Object target, Statement test) {
		List<FrameworkMethod> methods = getTestClass().getAnnotatedMethods(After.class);
		if (methods.isEmpty()) {
			return test;
		}
		return new LambdaStatement(() -> {
			List<Throwable> errors = new ArrayList<>();
			try {
				test.evaluate();
			} catch (Throwable e) {
				errors.add(e);
			} finally {
				for (FrameworkMethod m : methods) {
					try {
						m.invokeExplosively(target, getArgs(m));
					} catch (Throwable e) {
						errors.add(e);
					}
				}
			}
			MultipleFailureException.assertEmpty(errors);
		});
	}
}
