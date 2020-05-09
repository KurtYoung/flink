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

package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.utils.TableEnvUtils

object TableEnvUtil {

  def execInsertSqlAndWaitResult(tEnv: TableEnvironment, insert: String): JobExecutionResult = {
    val tableResult = tEnv.executeSql(insert)
    // wait to finish
    tableResult.getJobClient.get
      .getJobExecutionResult(Thread.currentThread.getContextClassLoader)
      .get
  }

  def execInsertTableAndWaitResult(table: Table, targetPath: String): JobExecutionResult = {
    val tableResult =  table.executeInsert(targetPath)
    // wait to finish
    tableResult.getJobClient.get
      .getJobExecutionResult(Thread.currentThread.getContextClassLoader)
      .get
  }

  /**
    * Register a [[TableSource]] as a table under a given name in the [[TableEnvironment]]'s
    * catalog.
    */
  def registerTableSource(
      tEnv: TableEnvironment,
      name: String,
      tableSource: TableSource[_]): Unit = {
    TableEnvUtils.registerTableSource(tEnv, name, tableSource, false)
  }

  /**
    * Register a [[TableSink]] as a table under a given name in the [[TableEnvironment]]'s catalog.
    */
  def registerTableSink(
      tEnv: TableEnvironment,
      name: String,
      tableSink: TableSink[_]): Unit = {
    TableEnvUtils.registerTableSink(tEnv, name, tableSink, false)
  }
}
