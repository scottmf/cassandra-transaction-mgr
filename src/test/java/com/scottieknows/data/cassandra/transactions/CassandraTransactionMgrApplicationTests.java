package com.scottieknows.data.cassandra.transactions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import com.datastax.driver.core.PreparedStatement;
import com.scottieknows.data.cassandra.transactions.CassandraTransactionMgrApplication;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = CassandraTransactionMgrApplication.class)
public class CassandraTransactionMgrApplicationTests {

    @Autowired
    private CassandraTransactionManager cassandraTransactionManager;

    @Test
    @Transactional
    @Rollback
    public void contextLoads() {
        String query = "select * from table";
        PreparedStatement stmt = cassandraTransactionManager.prepare(query);
    }

}
