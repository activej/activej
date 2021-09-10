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

package io.activej.jmx;

import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.module.UniqueQualifierImpl;
import io.activej.jmx.DynamicMBeanFactory.JmxCustomTypeAdapter;
import io.activej.worker.WorkerPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.StringFormatUtils.formatDuration;
import static io.activej.common.StringFormatUtils.parseDuration;
import static io.activej.common.reflection.ReflectionUtils.getAnnotationString;
import static io.activej.jmx.Utils.*;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public final class JmxRegistry implements JmxRegistryMXBean {
	private static final Logger logger = LoggerFactory.getLogger(JmxRegistry.class);

	private static final String GENERIC_PARAM_NAME_FORMAT = "T%d=%s";

	private final MBeanServer mbs;
	private final DynamicMBeanFactory mbeanFactory;
	private final Map<Type, JmxCustomTypeAdapter<?>> customTypes;
	private final Map<WorkerPool, Key<?>> workerPoolKeys = new HashMap<>();
	private final Set<ObjectName> registeredObjectNames = new HashSet<>();
	private ProtoObjectNameMapper objectNameMapper = ProtoObjectNameMapper.identity();
	private boolean withScopes = true;

	// jmx
	private int registeredSingletons;
	private int registeredPools;
	private int totallyRegisteredMBeans;

	private JmxRegistry(@NotNull MBeanServer mbs,
			DynamicMBeanFactory mbeanFactory,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		this.mbs = mbs;
		this.mbeanFactory = mbeanFactory;
		this.customTypes = customTypes;
	}

	public static JmxRegistry create(MBeanServer mbs, DynamicMBeanFactory mbeanFactory) {
		return new JmxRegistry(mbs, mbeanFactory, Collections.emptyMap());
	}

	public static JmxRegistry create(MBeanServer mbs,
			DynamicMBeanFactory mbeanFactory,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		return new JmxRegistry(mbs, mbeanFactory, customTypes);
	}

	public JmxRegistry withScopes(boolean withScopes) {
		this.withScopes = withScopes;
		return this;
	}

	public JmxRegistry withObjectNameMapping(ProtoObjectNameMapper objectNameMapper) {
		this.objectNameMapper = objectNameMapper;
		return this;
	}

	public void addWorkerPoolKey(WorkerPool workerPool, Key<?> workerPoolKey) {
		checkArgument(!workerPoolKeys.containsKey(workerPool), "Key already added");
		workerPoolKeys.put(workerPool, workerPoolKey);
	}

	public void registerSingleton(@NotNull Key<?> key, @NotNull Object singletonInstance, @NotNull JmxBeanSettings settings) {
		Class<?> instanceClass = singletonInstance.getClass();
		Object mbean;
		if (isJmxBean(instanceClass)) {
			// this will throw exception if something happens during initialization
			mbean = mbeanFactory.createDynamicMBean(singletonList(singletonInstance), settings, true);
		} else if (isStandardMBean(instanceClass) || isMXBean(instanceClass) || isDynamicMBean(instanceClass)) {
			mbean = singletonInstance;
		} else {
			logger.trace("Instance with key {} was not registered to jmx, " +
					"because its type or any of its supertypes is not annotated with @JmxBean annotation " +
					"and does not implement neither *MBean nor *MXBean interface", key);
			return;
		}

		ProtoObjectName protoName;
		try {
			protoName = createProtoObjectNameForKey(key);
		} catch (ReflectiveOperationException e) {
			logger.error("Error during generation name for instance with key {}", key, e);
			return;
		}

		protoName = objectNameMapper.apply(protoName);

		ObjectName objectName;
		try {
			objectName = createObjectName(protoName);
		} catch (MalformedObjectNameException | ReflectiveOperationException e) {
			logger.error("Cannot create ObjectName for instance with key {}. " +
					"Proposed proto name was \"{}\".", key, protoName, e);
			return;
		}

		try {
			mbs.registerMBean(mbean, objectName);
			logger.trace("Instance with key {} was successfully registered to jmx with ObjectName \"{}\" ", key, objectName);

			registeredObjectNames.add(objectName);
			registeredSingletons++;
			totallyRegisteredMBeans++;

		} catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			String msg = format("Cannot register MBean for instance with key %s and ObjectName \"%s\"",
					key, objectName);
			logger.error(msg, e);
		}
	}

	public void unregisterSingleton(@NotNull Key<?> key, Object singletonInstance) {
		if (isMBean(singletonInstance.getClass())) {
			try {
				ProtoObjectName name = createProtoObjectNameForKey(key);
				name = objectNameMapper.apply(name);
				ObjectName objectName = createObjectName(name);
				mbs.unregisterMBean(objectName);
				registeredObjectNames.remove(objectName);
			} catch (ReflectiveOperationException | JMException e) {
				String msg =
						format("Error during attempt to unregister MBean for instance with key %s.", key);
				logger.error(msg, e);
			}
		}
	}

	public void registerWorkers(@NotNull WorkerPool pool, Key<?> key, @NotNull List<?> poolInstances, JmxBeanSettings settings) {
		if (poolInstances.isEmpty()) {
			logger.info("Pool of instances with key {} is empty", key);
			return;
		}

		if (poolInstances.stream().map(Object::getClass).collect(toSet()).size() != 1) {
			logger.info("Pool of instances with key {} was not registered to jmx because their types differ", key);
			return;
		}

		if (!isJmxBean(poolInstances.get(0).getClass())) {
			logger.info("Pool of instances with key {} was not registered to jmx, " +
							"because instances' type or any of instances' supertypes is not annotated with @JmxBean annotation", key);
			return;
		}

		ProtoObjectName commonName;
		try {
			commonName = createProtoObjectNameForKey(key, pool);
		} catch (Exception e) {
			String msg = format("Error during generation name for pool of instances with key %s", key.toString());
			logger.error(msg, e);
			return;
		}

		// register mbeans for each worker separately
		for (int i = 0; i < poolInstances.size(); i++) {
			JmxBeanSettings settingsForOptionals = JmxBeanSettings.of(
					settings.getIncludedOptionals(), new HashMap<>(), customTypes);
			registerMBeanForWorker(poolInstances.get(i), i, commonName, key, settingsForOptionals);
		}

		// register aggregated mbean for pool of workers
		DynamicMBean mbean;
		try {
			mbean = mbeanFactory.createDynamicMBean(poolInstances, settings, true);
		} catch (Exception e) {
			String msg = format("Cannot create DynamicMBean for aggregated MBean of pool of workers with key %s",
					key);
			logger.error(msg, e);
			return;
		}

		ProtoObjectName mappedName = objectNameMapper.apply(commonName);

		ObjectName objectName;
		try {
			objectName = createObjectName(mappedName);
		} catch (MalformedObjectNameException | ReflectiveOperationException e) {
			String msg = format("Cannot create ObjectName for aggregated MBean of pool of workers with key %s. " +
					"Proposed String name was \"%s\".", key, mappedName);
			logger.error(msg, e);
			return;
		}

		try {
			mbs.registerMBean(mbean, objectName);
			logger.trace("Pool of instances with key {} was successfully registered to jmx with ObjectName \"{}\"", key, objectName);

			registeredObjectNames.add(objectName);
			registeredPools++;
			totallyRegisteredMBeans++;

		} catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			String msg = format("Cannot register aggregated MBean of pool of workers with key %s " +
					"and ObjectName \"%s\"", key, objectName);
			logger.error(msg, e);
		}
	}

	public void unregisterWorkers(WorkerPool pool, @NotNull Key<?> key, List<?> poolInstances) {
		if (poolInstances.isEmpty()) {
			return;
		}

		if (poolInstances.stream().map(Object::getClass).collect(toSet()).size() != 1) {
			return;
		}

		if (!isJmxBean(poolInstances.get(0).getClass())) {
			return;
		}

		ProtoObjectName commonName;
		try {
			commonName = createProtoObjectNameForKey(key, pool);
		} catch (ReflectiveOperationException e) {
			String msg = format("Error during generation name for pool of instances with key %s", key);
			logger.error(msg, e);
			return;
		}

		// unregister mbeans for each worker separately
		for (int i = 0; i < poolInstances.size(); i++) {
			try {
				ProtoObjectName workerName = addWorkerName(commonName, i);
				ProtoObjectName mappedWorkerName = objectNameMapper.apply(workerName);
				ObjectName objectName = createObjectName(mappedWorkerName);
				mbs.unregisterMBean(objectName);
				registeredObjectNames.remove(objectName);
			} catch (JMException | ReflectiveOperationException e) {
				String msg = format("Error during attempt to unregister mbean for worker" +
								" of pool of instances with key %s. Worker id is \"%d\"",
						key, i);
				logger.error(msg, e);
			}
		}

		ProtoObjectName mappedName = objectNameMapper.apply(commonName);

		// unregister aggregated mbean for pool of workers
		try {
			ObjectName objectName = createObjectName(mappedName);
			mbs.unregisterMBean(objectName);
			registeredObjectNames.remove(objectName);
		} catch (JMException | ReflectiveOperationException e) {
			String msg = format("Error during attempt to unregister aggregated mbean for pool of instances " +
					"with key %s.", key);
			logger.error(msg, e);
		}
	}

	public void unregisterAll() {
		Iterator<ObjectName> iterator = registeredObjectNames.iterator();
		while (iterator.hasNext()) {
			ObjectName objectName = iterator.next();
			try {
				mbs.unregisterMBean(objectName);
			} catch (InstanceNotFoundException | MBeanRegistrationException e) {
				String msg = format("Cannot unregister MBean with ObjectName \"%s\"", objectName.toString());
				logger.error(msg, e);
			}
			iterator.remove();
		}
	}

	private void registerMBeanForWorker(Object worker, int workerId, ProtoObjectName commonName,
			Key<?> key, JmxBeanSettings settings) {
		ProtoObjectName workerName = addWorkerName(commonName, workerId);
		ProtoObjectName mappedWorkerName = objectNameMapper.apply(workerName);

		DynamicMBean mbean;
		try {
			mbean = mbeanFactory.createDynamicMBean(singletonList(worker), settings, false);
		} catch (Exception e) {
			String msg = format("Cannot create DynamicMBean for worker " +
					"of pool of instances with key %s", key.toString());
			logger.error(msg, e);
			return;
		}

		ObjectName objectName;
		try {
			objectName = createObjectName(mappedWorkerName);
		} catch (MalformedObjectNameException | ReflectiveOperationException e) {
			String msg = format("Cannot create ObjectName for worker of pool of instances with key %s. " +
					"Proposed proto object name was \"%s\".", key.toString(), mappedWorkerName);
			logger.error(msg, e);
			return;
		}

		try {
			mbs.registerMBean(mbean, objectName);

			registeredObjectNames.add(objectName);
			totallyRegisteredMBeans++;

		} catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			String msg = format("Cannot register MBean for worker of pool of instances with key %s. " +
					"ObjectName for worker is \"%s\"", key.toString(), objectName);
			logger.error(msg, e);
		}
	}

	private static ProtoObjectName addWorkerName(ProtoObjectName protoObjectName, int workerId) {
		return protoObjectName.withWorkerId("worker-" + workerId);
	}

	private ProtoObjectName createProtoObjectNameForKey(Key<?> key) throws ReflectiveOperationException {
		return createProtoObjectNameForKey(key, null);
	}

	private ProtoObjectName createProtoObjectNameForKey(Key<?> key, @Nullable WorkerPool pool) throws ReflectiveOperationException {
		Class<?> rawType = key.getRawType();
		Package domainPackage = rawType.getPackage();
		String packageName = domainPackage == null ? "" : domainPackage.getName();

		ProtoObjectName protoObjectName = ProtoObjectName.create(rawType.getSimpleName(), packageName);

		Object keyQualifier = key.getQualifier();
		if (keyQualifier instanceof UniqueQualifierImpl) {
			keyQualifier = ((UniqueQualifierImpl) keyQualifier).getOriginalQualifier();
		}
		protoObjectName = protoObjectName.withQualifier(keyQualifier);

		if (pool != null) {
			if (withScopes) {
				Scope scope = pool.getScope();
				protoObjectName = protoObjectName.withScope(scope.getDisplayString());
			}
			Key<?> poolKey = workerPoolKeys.get(pool);
			if (poolKey != null && poolKey.getQualifier() != null) {
				String qualifierString = getQualifierString(poolKey.getQualifier());
				protoObjectName = protoObjectName.withWorkerPoolQualifier("WorkerPool@" + qualifierString);
			}
		}

		Type type = key.getType();
		if (type instanceof ParameterizedType) {
			ParameterizedType pType = (ParameterizedType) type;
			Type[] genericArgs = pType.getActualTypeArguments();
			List<String> genericParams = new ArrayList<>();
			for (Type genericArg : genericArgs) {
				String argClassName = formatSimpleGenericName(genericArg);
				genericParams.add(argClassName);
			}
			protoObjectName = protoObjectName.withGenericParameters(genericParams);
		}

		return protoObjectName;
	}

	private static String formatAnnotationString(String annotationString) {
		if (!annotationString.contains("(")) {
			return "annotation=" + annotationString;
		} else if (!annotationString.startsWith("(")) {
			String annotationName = annotationString.substring(0, annotationString.indexOf('('));
			return annotationName + '=' + annotationString.substring(annotationString.indexOf('(') + 1, annotationString.length() - 1);
		} else {
			return annotationString.substring(1, annotationString.length() - 1);
		}
	}

	private static String formatSimpleGenericName(Type type) {
		if (type instanceof Class) {
			return ((Class<?>) type).getSimpleName();
		}
		ParameterizedType genericType = (ParameterizedType) type;
		return ((Class<?>) genericType.getRawType()).getSimpleName() +
				Arrays.stream(genericType.getActualTypeArguments())
						.map(JmxRegistry::formatSimpleGenericName)
						.collect(joining(";", "<", ">"));
	}

	private ObjectName createObjectName(ProtoObjectName protoObjectName) throws MalformedObjectNameException, ReflectiveOperationException {
		StringJoiner joiner = new StringJoiner(",", protoObjectName.getPackageName() + ":", "");

		if (protoObjectName.getClassName() != null) {
			joiner.add("type=" + protoObjectName.getClassName());
		}

		Object qualifier = protoObjectName.getQualifier();
		if (qualifier != null) {
			String qualifierString = null;
			if (qualifier instanceof Class) {
				Class<?> qualifierClass = (Class<?>) qualifier;
				if (qualifierClass.isAnnotation()) {
					qualifierString = qualifierClass.getSimpleName();
				}
			} else if (qualifier instanceof Annotation) {
				qualifierString = getAnnotationString((Annotation) qualifier);
			}
			if (qualifierString != null){
				qualifierString = formatAnnotationString(qualifierString);
			} else {
				qualifierString = "qualifier=" + qualifier;
			}
			joiner.add(qualifierString);
		}

		String scope = protoObjectName.getScope();
		if (scope != null) {
			joiner.add("scope=" + scope);
		}

		String workerPoolQualifier = protoObjectName.getWorkerPoolQualifier();
		if (workerPoolQualifier != null) {
			joiner.add("workerPool=" + workerPoolQualifier);
		}

		List<String> genericParameters = protoObjectName.getGenericParameters();
		if (genericParameters != null) {
			for (int i = 0; i < genericParameters.size(); i++) {
				int argId = i + 1;
				joiner.add(format(GENERIC_PARAM_NAME_FORMAT, argId, genericParameters.get(i)));
			}
		}

		String workerId = protoObjectName.getWorkerId();
		if (workerId != null) {
			joiner.add("workerId=" + workerId);
		}

		return new ObjectName(joiner.toString());
	}

	// region jmx
	@Override
	public int getRegisteredSingletons() {
		return registeredSingletons;
	}

	@Override
	public int getRegisteredPools() {
		return registeredPools;
	}

	@Override
	public int getTotallyRegisteredMBeans() {
		return totallyRegisteredMBeans;
	}

	@Override
	public String getRefreshPeriod() {
		return formatDuration(mbeanFactory.getSpecifiedRefreshPeriod());
	}

	@Override
	public void setRefreshPeriod(String refreshPeriod) {
		mbeanFactory.setRefreshPeriod(parseDuration(refreshPeriod));
	}

	@Override
	public int getMaxRefreshesPerOneCycle() {
		return mbeanFactory.getMaxJmxRefreshesPerOneCycle();
	}

	@Override
	public void setMaxRefreshesPerOneCycle(int maxRefreshesPerOneCycle) {
		mbeanFactory.setMaxJmxRefreshesPerOneCycle(maxRefreshesPerOneCycle);
	}

	@Override
	public String[] getRefreshStats() {
		return mbeanFactory.getRefreshStats();
	}

	// endregion
}
