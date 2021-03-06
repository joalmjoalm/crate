/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import io.crate.action.sql.SessionContext;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.DropBlobTable;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.QualifiedName;

import java.util.List;

class DropTableAnalyzer {

    private final Schemas schemas;

    DropTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public DropTableAnalyzedStatement<DocTableInfo> analyze(DropTable node, SessionContext sessionContext) {
        return analyze(node.table().getName(), node.dropIfExists(), sessionContext);
    }

    public DropTableAnalyzedStatement<BlobTableInfo> analyze(DropBlobTable node, SessionContext sessionContext) {
        List<String> parts = node.table().getName().getParts();
        if (parts.size() != 1 && !parts.get(0).equals(BlobSchemaInfo.NAME)) {
            throw new IllegalArgumentException("No blob tables in schema `" + parts.get(0) + "`");
        } else {
            QualifiedName name = new QualifiedName(
                List.of(BlobSchemaInfo.NAME, node.table().getName().getSuffix()));
            return analyze(name, node.ignoreNonExistentTable(), sessionContext);
        }
    }

    private <T extends TableInfo> DropTableAnalyzedStatement<T> analyze(QualifiedName name,
                                                                        boolean dropIfExists,
                                                                        SessionContext sessionContext) {
        T tableInfo;
        try {
            //noinspection unchecked
            tableInfo = (T) schemas.resolveTableInfo(name, Operation.DROP, sessionContext.user(), sessionContext.searchPath());
        } catch (SchemaUnknownException | RelationUnknown e) {
            if (dropIfExists) {
                tableInfo = null;
            } else {
                throw e;
            }
        }
        return new DropTableAnalyzedStatement<>(tableInfo, dropIfExists);
    }
}
