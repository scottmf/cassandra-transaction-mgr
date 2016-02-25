/**
 * Copyright (C) 2016 Scott Feldstein
 *
 * Permission is hereby granted, free of charge, to any person obtaining a 
 * copy of this software and associated documentation files (the "Software"), 
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.scottieknows.data.cassandra.transactions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.MappingManager;

@Component
public class CassandraDataOperations {

    @Autowired
    private CassandraTransactionManager cassandraTransactionManager;
    @Autowired
    private DataObjectHelper dataObjectHelper;

    public void insert(CassandraDataObject dataObject) {
        Statement stmt = dataObjectHelper.getInsertStmt(dataObject);
        cassandraTransactionManager.addStmt(stmt);
    }

    public void update(CassandraDataObject dataObject) {
        Statement stmt = dataObjectHelper.getUpdateStmt(dataObject);
        cassandraTransactionManager.addStmt(stmt);
    }

    public <T> T getAccessor(Class<T> klazz) {
        Session session = cassandraTransactionManager.getSession();
        MappingManager mappingManager = new MappingManager(session);
        return mappingManager.createAccessor(klazz);
    }
}
