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

import io.activej.di.Key;
import io.activej.di.Scope;
import io.activej.di.module.UniqueQualifierImpl;
import io.activej.jmx.DynamicMBeanFactory.JmxCustomTypeAdapter;
import io.activej.worker.WorkerPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static io.activej.common.Preconditions.checkArgument;
import static io.activej.common.StringFormatUtils.formatDuration;
import static io.activej.common.StringFormatUtils.parseDuration;
import static io.activej.jmx.Utils.*;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public final class JmxRegistry implements JmxRegistryMXBean {
	private static final Logger logger = LoggerFactory.getLogger(JmxRegistry.class);

	private static final String GENERIC_PARAM_NAME_FORMAT = "T%d=%s";
	private static final String ROOT_PACKAGE_NAME = "";

	private final MBeanServer mbs;
	private final DynamicMBeanFactory mbeanFactory;
	private final Map<Key<?>, String> keyToObjectNames;
	private final Map<Type, JmxCustomTypeAdapter<?>> customTypes;
	private final Map<WorkerPool, Key<?>> workerPoolKeys = new HashMap<>();
	private final Set<ObjectName> registeredObjectNames = new HashSet<>();
	private boolean withScopes = true;

	// jmx
	private int registeredSingletons;
	private int registeredPools;
	private int totallyRegisteredMBeans;

	private JmxRegistry(@NotNull MBeanServer mbs,
			DynamicMBeanFactory mbeanFactory,
			Map<Key<?>, String> keyToObjectNames,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		this.mbs = mbs;
		this.mbeanFactory = mbeanFactory;
		this.keyToObjectNames = keyToObjectNames;
		this.customTypes = customTypes;
	}

	public static JmxRegistry create(MBeanServer mbs, DynamicMBeanFactory mbeanFactory) {
		return new JmxRegistry(mbs, mbeanFactory, Collections.emptyMap(), Collections.emptyMap());
	}

	public static JmxRegistry create(MBeanServer mbs,
			DynamicMBeanFactory mbeanFactory,
			Map<Key<?>, String> keyToObjectNames,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		return new JmxRegistry(mbs, mbeanFactory, keyToObjectNames, customTypes);
	}

	public JmxRegistry withScopes(boolean withScopes) {
		this.withScopes = withScopes;
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

		String name;
		try {
			name = createNameForKey(key);
		} catch (ReflectiveOperationException e) {
			logger.error("Error during generation name for instance with key {}", key, e);
			return;
		}

		ObjectName objectName;
		try {
			objectName = new ObjectName(name);
		} catch (MalformedObjectNameException e) {
			logger.error("Cannot create ObjectName for instance with key {}. Proposed String name was \"{}\".", key, name, e);
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
					key.toString(), objectName.toString());
			logger.error(msg, e);
		}
	}

	public void unregisterSingleton(@NotNull Key<?> key, Object singletonInstance) {
		if (isMBean(singletonInstance.getClass())) {
			try {
				String name = createNameForKey(key);
				ObjectName objectName = new ObjectName(name);
				mbs.unregisterMBean(objectName);
				registeredObjectNames.remove(objectName);
			} catch (ReflectiveOperationException | JMException e) {
				String msg =
						format("Error during attempt to unregister MBean for instance with key %s.", key.toString());
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

		String commonName;
		try {
			commonName = createNameForKey(key, pool);
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
					key.toString());
			logger.error(msg, e);
			return;
		}

		ObjectName objectName;
		try {
			objectName = new ObjectName(commonName);
		} catch (MalformedObjectNameException e) {
			String msg = format("Cannot create ObjectName for aggregated MBean of pool of workers with key %s. " +
					"Proposed String name was \"%s\".", key.toString(), commonName);
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
					"and ObjectName \"%s\"", key.toString(), objectName.toString());
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

		String commonName;
		try {
			commonName = createNameForKey(key, pool);
		} catch (ReflectiveOperationException e) {
			String msg = format("Error during generation name for pool of instances with key %s", key.toString());
			logger.error(msg, e);
			return;
		}

		// unregister mbeans for each worker separately
		for (int i = 0; i < poolInstances.size(); i++) {
			try {
				String workerName = createWorkerName(commonName, i);
				ObjectName objectName = new ObjectName(workerName);
				mbs.unregisterMBean(objectName);
				registeredObjectNames.remove(objectName);
			} catch (JMException e) {
				String msg = format("Error during attempt to unregister mbean for worker" +
								" of pool of instances with key %s. Worker id is \"%d\"",
						key.toString(), i);
				logger.error(msg, e);
			}
		}

		// unregister aggregated mbean for pool of workers
		try {
			ObjectName objectName = new ObjectName(commonName);
			mbs.unregisterMBean(objectName);
			registeredObjectNames.remove(objectName);
		} catch (JMException e) {
			String msg = format("Error during attempt to unregister aggregated mbean for pool of instances " +
					"with key %s.", key.toString());
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

	private void registerMBeanForWorker(Object worker, int workerId, String commonName,
			Key<?> key, JmxBeanSettings settings) {
		String workerName = createWorkerName(commonName, workerId);

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
			objectName = new ObjectName(workerName);
		} catch (MalformedObjectNameException e) {
			String msg = format("Cannot create ObjectName for worker of pool of instances with key %s. " +
					"Proposed String name was \"%s\".", key.toString(), workerName);
			logger.error(msg, e);
			return;
		}

		try {
			mbs.registerMBean(mbean, objectName);

			registeredObjectNames.add(objectName);
			totallyRegisteredMBeans++;

		} catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			String msg = format("Cannot register MBean for worker of pool of instances with key %s. " +
					"ObjectName for worker is \"%s\"", key.toString(), objectName.toString());
			logger.error(msg, e);
		}
	}

	private static String createWorkerName(String commonName, int workerId) {
		return commonName + format(",workerId=worker-%d", workerId);
	}

	private String createNameForKey(Key<?> key) throws ReflectiveOperationException {
		return createNameForKey(key, null);
	}

	private String createNameForKey(Key<?> key, @Nullable WorkerPool pool) throws ReflectiveOperationException {
		if (keyToObjectNames.containsKey(key)) {
			return keyToObjectNames.get(key);
		}
		Class<?> rawType = key.getRawType();
		Object keyQualifier = key.getQualifier();
		if (keyQualifier instanceof UniqueQualifierImpl) {
			keyQualifier = ((UniqueQualifierImpl) keyQualifier).getOriginalQualifier();
		}
		Package domainPackage = rawType.getPackage();
		String domain = domainPackage == null ? ROOT_PACKAGE_NAME : domainPackage.getName();
		String name = domain + ":" + "type=" + rawType.getSimpleName();

		if (keyQualifier != null) { // with annotation
			name += ',';
			String qualifierString = getQualifierString(keyQualifier);
			if (!qualifierString.contains("(")) {
				name += "annotation=" + qualifierString;
			} else if (!qualifierString.startsWith("(")) {
				name += qualifierString.substring(0, qualifierString.indexOf('('));
				name += '=' + qualifierString.substring(qualifierString.indexOf('(') + 1, qualifierString.length() - 1);
			} else {
				name += qualifierString.substring(1, qualifierString.length() - 1);
			}
		}
		if (pool != null) {
			if (withScopes) {
				Scope scope = pool.getScope();
				name += format(",scope=%s", scope.getDisplayString());
			}
			Key<?> poolKey = workerPoolKeys.get(pool);
			if (poolKey != null && poolKey.getQualifier() != null) {
				String qualifierString = getQualifierString(poolKey.getQualifier());
				name += format(",workerPool=WorkerPool@%s", qualifierString);
			}
		}
		return addGenericParamsInfo(name, key);
	}

	private static String addGenericParamsInfo(String srcName, Key<?> key) {
		Type type = key.getType();
		StringBuilder resultName = new StringBuilder(srcName);
		if (type instanceof ParameterizedType) {
			ParameterizedType pType = (ParameterizedType) type;
			Type[] genericArgs = pType.getActualTypeArguments();
			for (int i = 0; i < genericArgs.length; i++) {
				Type genericArg = genericArgs[i];
				String argClassName = formatSimpleGenericName(genericArg);
				int argId = i + 1;
				resultName.append(",").append(format(GENERIC_PARAM_NAME_FORMAT, argId, argClassName));
			}
		}
		return resultName.toString();
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
