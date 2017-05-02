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

package org.apache.flink.runtime.operators;

public interface FakeTwoInputStreamOperator<IN1, IN2, OUT> extends FakeStreamOperator<OUT> {

	InputSelection firstInputSelection();

	/**
	 * Processes one element that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	InputSelection processElement1(IN1 element) throws Exception;

	/**
	 * Processes one element that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	InputSelection processElement2(IN2 element) throws Exception;


	InputSelection endInput1() throws Exception;

	InputSelection endInput2() throws Exception;
}
