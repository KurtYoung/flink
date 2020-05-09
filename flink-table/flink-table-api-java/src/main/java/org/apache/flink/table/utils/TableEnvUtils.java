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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.Optional;

/**
 * Utility methods to register table source/sink as a Table in {@link TableEnvironment}'s catalog.
 */
@Internal
public class TableEnvUtils {
	/**
	 * Register a table source as a Table under the given name in this
	 * {@link TableEnvironment}'s catalog.
	 */
	public static void registerTableSource(
			TableEnvironment tEnv,
			String name,
			TableSource<?> tableSource,
			boolean isBatch) {
		if (tEnv instanceof TableEnvironmentInternal) {
			CatalogManager catalogManager = ((TableEnvironmentInternal) tEnv).getCatalogManager();
			ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(name));
			Optional<CatalogBaseTable> table = getTemporaryTable(catalogManager, objectIdentifier);

			if (table.isPresent()) {
				if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
					ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
					if (sourceSinkTable.getTableSource().isPresent()) {
						throw new ValidationException(String.format(
							"Table '%s' already exists. Please choose a different name.", name));
					} else {
						ConnectorCatalogTable sourceAndSink = ConnectorCatalogTable.sourceAndSink(
							tableSource,
							sourceSinkTable.getTableSink().get(),
							isBatch);
						catalogManager.dropTemporaryTable(objectIdentifier, false);
						catalogManager.createTemporaryTable(sourceAndSink, objectIdentifier, false);
					}
				} else {
					throw new ValidationException(String.format(
						"Table '%s' already exists. Please choose a different name.", name));
				}
			} else {
				ConnectorCatalogTable source = ConnectorCatalogTable.source(tableSource, isBatch);
				catalogManager.createTemporaryTable(source, objectIdentifier, false);
			}
		} else {
			throw new ValidationException(String.format(
				"This is a workaround to remove registration of TableSource in Table Env. We " +
					"need TableEnvironmentInternal#getCatalogManager to register a table source " +
					"as a Table under the given name '%s' in this TableEnvironment's catalog.",
				name));
		}
	}

	/**
	 * Register a table sink as a Table under the given name in this
	 * {@link TableEnvironment}'s catalog.
	 */
	public static void registerTableSink(
			TableEnvironment tEnv,
			String name,
			TableSink<?> tableSink,
			boolean isBatch) {
		if (tEnv instanceof TableEnvironmentInternal) {
			CatalogManager catalogManager = ((TableEnvironmentInternal) tEnv).getCatalogManager();
			ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(name));
			Optional<CatalogBaseTable> table = getTemporaryTable(catalogManager, objectIdentifier);

			if (table.isPresent()) {
				if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
					ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
					if (sourceSinkTable.getTableSink().isPresent()) {
						throw new ValidationException(String.format(
							"Table '%s' already exists. Please choose a different name.", name));
					} else {
						ConnectorCatalogTable sourceAndSink = ConnectorCatalogTable.sourceAndSink(
							sourceSinkTable.getTableSource().get(),
							tableSink,
							isBatch);
						catalogManager.dropTemporaryTable(objectIdentifier, false);
						catalogManager.createTemporaryTable(sourceAndSink, objectIdentifier, false);
					}
				} else {
					throw new ValidationException(String.format(
						"Table '%s' already exists. Please choose a different name.", name));
				}
			} else {
				ConnectorCatalogTable sink = ConnectorCatalogTable.sink(tableSink, isBatch);
				catalogManager.createTemporaryTable(sink, objectIdentifier, false);
			}
		} else {
			throw new ValidationException(String.format(
				"This is a workaround to remove registration of TableSink in Table Env. We " +
					"need TableEnvironmentInternal#getCatalogManager to register a table sink " +
					"as a Table under the given name '%s' in this TableEnvironment's catalog.",
				name));
		}
	}

	private static Optional<CatalogBaseTable> getTemporaryTable(CatalogManager catalogManager, ObjectIdentifier identifier) {
		return catalogManager.getTable(identifier)
			.filter(CatalogManager.TableLookupResult::isTemporary)
			.map(CatalogManager.TableLookupResult::getTable);
	}
}
