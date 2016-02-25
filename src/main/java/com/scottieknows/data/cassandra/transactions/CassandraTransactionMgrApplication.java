package com.scottieknows.data.cassandra.transactions;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.datastax.driver.core.Cluster;

@SpringBootApplication
@EnableTransactionManagement
public class CassandraTransactionMgrApplication {

    public static void main(String[] args) {
        SpringApplication.run(CassandraTransactionMgrApplication.class, args);
    }
}
