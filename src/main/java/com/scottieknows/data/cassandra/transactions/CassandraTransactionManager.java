package com.scottieknows.data.cassandra.transactions;

import static java.lang.String.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

@SuppressWarnings("serial")
@Component("transactionManager")
@ManagedResource(objectName="CassandraTransactionManager:name=Transaction", description="Cassandra Transaction Manager")
public class CassandraTransactionManager extends AbstractPlatformTransactionManager {
    private transient final Log log = LogFactory.getLog(CassandraTransactionManager.class);
    
    private final AtomicLong preCommits = new AtomicLong();
    private final AtomicLong preCommitsTotalTime = new AtomicLong();
    private final AtomicLong postCommits = new AtomicLong();
    private final AtomicLong postCommitsTotalTime = new AtomicLong();
    private transient ThreadLocal<Session> localSession = new ThreadLocal<>();
    private transient ThreadLocal<BatchStatement> localBatch = new ThreadLocal<>();

    private final AtomicLong threadNum = new AtomicLong();
    private transient ExecutorService executorService =
        Executors.newFixedThreadPool(10, r -> new Thread(r, "cass-transaction-mgr-" + threadNum.getAndIncrement()));

    private transient ThreadLocal<Collection<Runnable>> preCommitTasks = new ThreadLocal<Collection<Runnable>>() {
        @Override
        public Collection<Runnable> initialValue() {
            return new ArrayList<>();
        }
    };

    private transient ThreadLocal<Collection<Runnable>> postCommitTasks = new ThreadLocal<Collection<Runnable>>() {
        @Override
        public Collection<Runnable> initialValue() {
            return new ArrayList<>();
        }
    };

    private transient final Cluster cluster;
       
    @Autowired
    public CassandraTransactionManager(Cluster cluster) {
        super();
        this.cluster = cluster;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        Session session = cluster.connect();
        localSession.set(session);
        localBatch.set(new BatchStatement(Type.LOGGED));
    }
    
    @Override
    protected void doRollback(DefaultTransactionStatus status) {
        localSession.get().close();
    }

    @Override
    protected Object doSuspend(Object transaction) {
        return new Object[0];
    }

    @Override
    protected void doResume(Object transaction, Object suspendedResources) {
        super.doResume(transaction, suspendedResources);
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        log.debug("doCleanupAfterCompletion");
        super.doCleanupAfterCompletion(transaction);
    }

    @Override
    protected void prepareSynchronization(DefaultTransactionStatus status, TransactionDefinition definition) {
        if (status.isNewSynchronization()) {
            super.prepareSynchronization(status, definition);
            registerSynchronization();
        }
    }

    private void registerSynchronization() {
        log.debug("registeringSynchronization");
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.initSynchronization();
        }
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void suspend() {
                    log.debug("EXECUTING suspend()");
                }
                @Override
                public void resume() {
                    log.debug("EXECUTING resume()");
                }
                @Override
                public void flush() {
                    log.debug("EXECUTING flush()");
                }
                @Override
                public void beforeCompletion() {
                    log.debug("EXECUTING beforeCompletion()");
                }
                @Override
                public void beforeCommit(boolean readOnly) {
                    if (log.isDebugEnabled()) {
                        log.debug("EXECUTING beforeCommit(" + readOnly + ")");
                    }
                    runTasks(preCommits, preCommitsTotalTime, preCommitTasks.get(), false);
                }
                @Override
                public void afterCommit() {
                    log.debug("EXECUTING afterCommit()");
                    runTasks(postCommits, postCommitsTotalTime, postCommitTasks.get(), true);
                }
                @Override
                public void afterCompletion(int status) {
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug("EXECUTING afterCompletion(" + status + ")");
                        }
                        switch (status) {
                            case STATUS_COMMITTED:
                                break;
                            case STATUS_ROLLED_BACK:
                                break;
                            case STATUS_UNKNOWN:
                                break;
                            default:
                                break;
                        }
                    } finally {
//                        clearResources();
                    }
                }
            }
        );
    }

    private void runTasks(AtomicLong numCommits, AtomicLong totalTime, Collection<Runnable> tasks, boolean background) {
        if (log.isDebugEnabled()) {
            log.debug(format("running %s post commit tasks", tasks.size()));
        }
        try {
            for (Runnable r : tasks) {
                try {
                    long start = now();
                    numCommits.addAndGet(1);
                    if (background) {
                        executorService.execute(r);
                    } else {
                        r.run();
                    }
                    totalTime.addAndGet(now() - start);
                } catch (Throwable t) {
                    log.error("problem running post-commit task: " + t,t);
                }
            }
        } finally {
            tasks.clear();
        }
    }

    private long now() {
        return System.currentTimeMillis();
    }

    public void addPreCommitTask(Runnable r) {
        final Collection<Runnable> tasks = preCommitTasks.get();
        tasks.add(r);
    }

    public void addPostCommitTask(Runnable r) {
        final Collection<Runnable> tasks = postCommitTasks.get();
        tasks.add(r);
    }

    @ManagedAttribute(description="Pre-commit tasks - total exec time (ms)")
    public long getPreCommitsTotalExecTime() {
        return preCommitsTotalTime.get();
    }

    @ManagedAttribute(description="Post-commit tasks - total exec time (ms)")
    public long getPostCommitsTotalExecTime() {
        return postCommitsTotalTime.get();
    }

    @ManagedAttribute(description="Number of pre-commit tasks")
    public long getNumPreCommits() {
        return preCommits.get();
    }

    @ManagedAttribute(description="Number of post-commit tasks")
    public long getNumPostCommits() {
        return postCommits.get();
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        localSession.get().execute(localBatch.get());
    }

    public PreparedStatement prepare(String query) {
        return localSession.get().prepare(query);
    }

    public void addStmt(Statement stmt) {
        localBatch.get().add(stmt);
    }

    public Session getSession() {
        return localSession.get();
    }
      
}
