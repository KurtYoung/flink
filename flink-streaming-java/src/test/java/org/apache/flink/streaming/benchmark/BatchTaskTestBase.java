/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.benchmark;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Assert;

public abstract class BatchTaskTestBase extends TestLogger {
	
	protected long memorySize = 0;

	protected MockInputSplitProvider inputSplitProvider;

	protected MockEnvironment mockEnv;

	public void initEnvironment(long memorySize, int bufferSize) {
		this.memorySize = memorySize;
		this.inputSplitProvider = new MockInputSplitProvider();
		this.mockEnv = new MockEnvironment("mock task", this.memorySize, this.inputSplitProvider, bufferSize);
	}

	public <T> void addInputGate(InputGate inputGate, int groupId, TypeSerializerFactory<T> inputSerializer) {
		mockEnv.addInput(inputGate);
		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());
		conf.addInputToGroup(groupId);
		conf.setInputSerializer(inputSerializer, groupId);
	}

	public <T> void addOutputWriter(ResultPartitionWriter writer, TypeSerializerFactory<T> outputSerializer) {
		this.mockEnv.addOutputWriter(writer);
		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());
		conf.addOutputShipStrategy(ShipStrategyType.FORWARD);
		conf.setOutputSerializer(outputSerializer);
	}

	public Configuration getConfiguration() {
		return this.mockEnv.getTaskConfiguration();
	}

	public void registerTask(AbstractInvokable task, 
								@SuppressWarnings("rawtypes") Class<? extends Driver> driver,
								Class<? extends RichFunction> stubClass) {
		
		final TaskConfig config = new TaskConfig(this.mockEnv.getTaskConfiguration());
		config.setDriver(driver);
		config.setStubWrapper(new UserCodeClassWrapper<>(stubClass));
		
		task.setEnvironment(this.mockEnv);
	}

	public MemoryManager getMemoryManager() {
		return this.mockEnv.getMemoryManager();
	}

	@After
	public void shutdownIOManager() throws Exception {
		this.mockEnv.getIOManager().shutdown();
		Assert.assertTrue("IO Manager has not properly shut down.", this.mockEnv.getIOManager().isProperlyShutDown());
	}

	@After
	public void shutdownMemoryManager() throws Exception {
		if (this.memorySize > 0) {
			MemoryManager memMan = getMemoryManager();
			if (memMan != null) {
				Assert.assertTrue("Memory Manager managed memory was not completely freed.", memMan.verifyEmpty());
				memMan.shutdown();
			}
		}
	}
}
