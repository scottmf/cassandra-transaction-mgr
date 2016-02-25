package com.scottieknows.data.cassandra.transactions;

import static java.lang.String.*;

import java.util.ArrayList;
import java.util.Collection;
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
@ManagedResource(objectName="ScottieKnows:name=Transaction", description="ScottieKnows Transaction Manager")
public class CassandraTransactionManager extends AbstractPlatformTransactionManager {
    private transient final Log log = LogFactory.getLog(CassandraTransactionManager.class);
    
    private final AtomicLong preCommits = new AtomicLong();
    private final AtomicLong preCommitsTotalTime = new AtomicLong();
    private final AtomicLong postCommits = new AtomicLong();
    private final AtomicLong postCommitsTotalTime = new AtomicLong();
    private transient ThreadLocal<Session> localSession = new ThreadLocal<>();
    private transient ThreadLocal<BatchStatement> localBatch = new ThreadLocal<>();

/* XXX
    private static final int PARENT_RESOURCE = 0;
    private static final int CACHE_UPDATES   = 1;
    private static final int CACHE_DELETES   = 2;
    private static final int CACHE_SELECTS   = 3;
    private static final int DAO_OPERATIONS_TRAN_INFO   = 4;
    private static final int DB_CACHE_MANAGER_TRAN_INFO = 5;
    private static final String QUEUE_NAME = "DB_CACHE_UPDATE_QUEUE";

    private transient ThreadLocal<Map<Class<?>, Map<Serializable, CacheObject>>> cacheSelects =
        new ThreadLocal<Map<Class<?>, Map<Serializable, CacheObject>>>() {
        public Map<Class<?>,Map<Serializable,CacheObject>> initialValue() {
            return new HashMap<>();
        };
    };
    private transient ThreadLocal<Map<Class<?>, Map<Serializable, CacheObject>>> cacheUpdates = new ThreadLocal<>();
    private transient ThreadLocal<Map<Class<?>, Collection<Serializable>>> cacheDeletes = new ThreadLocal<>();

    private transient ThreadLocal<Map<Class<?>, Cache>> cacheLocal = new ThreadLocal<Map<Class<?>, Cache>>() {
        @Override
        public Map<Class<?>, Cache> initialValue() {
            return new HashMap<Class<?>, Cache>();
        }
    };
*/

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

/*
    public <T> Map<Serializable, T> getAllById(final Collection<? extends Serializable> ids, final Class<T> klazz) {
        final Map<Serializable, T> rtn = new HashMap<Serializable, T>();
        for (final Serializable id : ids) {
            final T obj = getById(id, klazz);
            if (obj == null) {
                continue;
            }
            rtn.put(id, obj);
        }
        return rtn;
    }
*/

    /**
     * if any objects are set to update in the transaction then return them
    public <T> Element getUpdatedElementById(Serializable id, Class<T> klazz) {
        Element tmp;
        if (hasDeletes(klazz) && cacheDeletes.get().get(klazz).contains(id)) {
            return null;
        } else if (hasUpdates(klazz) && null != (tmp = cacheUpdates.get().get(klazz).get(id))) {
            return tmp;
        }
        return null;
    }

    public <T> T getById(Serializable id, Class<T> klazz) {
        Element e = getUpdatedElementById(id, klazz);
        return (e == null) ? null : dbCacheManager.inflate(e.getObjectValue().toString(), klazz);
    }

    private boolean isReadWrite(Set<Class<?>> klazzes) {
        for (Class<?> klazz : klazzes) {
            if (hasDeletes(klazz) || hasUpdates(klazz)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasDeletes(Class<?> klazz) {
        Map<Class<?>, Collection<Serializable>> tmp = cacheDeletes.get();
        if (tmp == null) {
            return false;
        }
        return tmp.containsKey(klazz);
    }
    
    private boolean hasUpdates(Class<?> klazz) {
        Map<Class<?>, Map<Serializable, CacheObject>> tmp = cacheUpdates.get();
        if (tmp == null) {
            return false;
        }
        return tmp.containsKey(klazz);
    }
     */

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
/*
        Object parentResource = super.doSuspend(transaction);
        Object[] rtn = new Object[6];
        rtn[PARENT_RESOURCE] = parentResource;
        rtn[CACHE_UPDATES]   = cacheUpdates.get();
        rtn[CACHE_DELETES]   = cacheDeletes.get();
        rtn[CACHE_SELECTS]   = cacheSelects.get();
        rtn[DAO_OPERATIONS_TRAN_INFO] = daoOperations.getAndClearLocalTranInfo();
        rtn[DB_CACHE_MANAGER_TRAN_INFO] = dbCacheManager.getAndClearLocalTranInfo();
        cacheUpdates.set(null);
        cacheDeletes.set(null);
        return rtn;
*/
        return new Object[0];
    }

    @Override
    protected void doResume(Object transaction, Object suspendedResources) {
/*
        Object[] resources = (Object[]) suspendedResources;
        Object parentResource = resources[PARENT_RESOURCE];
        cacheUpdates.set((Map<Class<?>, Map<Serializable, CacheObject>>) resources[CACHE_UPDATES]);
        cacheDeletes.set((Map<Class<?>, Collection<Serializable>>) resources[CACHE_DELETES]);
        cacheUpdates.set((Map<Class<?>, Map<Serializable, CacheObject>>) resources[CACHE_SELECTS]);
        daoOperations.setLocalTranInfo(resources[DAO_OPERATIONS_TRAN_INFO]);
        dbCacheManager.setLocalTranInfo(resources[DB_CACHE_MANAGER_TRAN_INFO]);
*/
        super.doResume(transaction, suspendedResources);
    }
    
    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        log.debug("doCleanupAfterCompletion");
//        clearResources();
        super.doCleanupAfterCompletion(transaction);
    }

/*
    private void clearResources() {
        log.debug("clearResources");
        cacheDeletes.remove();
        cacheSelects.remove();
        cacheUpdates.remove();
        cacheLocal.remove();
        postCommitTasks.remove();
        daoOperations.clearThreadLocals();
        dbCacheManager.clearThreadLocals();
    }

    private Map<Class<?>, Collection<Serializable>> getCacheDeletes() {
        Map<Class<?>, Collection<Serializable>> deletes = cacheDeletes.get();
        if (deletes == null) {
            initMaps();
            deletes = cacheDeletes.get();
        }
        return deletes;
    }

    private Map<Class<?>, Map<Serializable, CacheObject>> getCacheUpdates() {
        Map<Class<?>, Map<Serializable, CacheObject>> updates = cacheUpdates.get();
        if (updates == null) {
            initMaps();
            updates = cacheUpdates.get();
        }
        return updates;
    }
    
    private void initMaps() {
        if (cacheUpdates.get() == null) {
            cacheUpdates.set(new HashMap<Class<?>, Map<Serializable, CacheObject>>());
        }
        if (cacheDeletes.get() == null) {
            cacheDeletes.set(new HashMap<Class<?>, Collection<Serializable>>());
        }
        if (cacheSelects.get() == null) {
            cacheSelects.set(new HashMap<Class<?>, Map<Serializable, CacheObject>>());
        }
    }
    
    public void updateSelectCache(Map<Class<?>, Map<Serializable, CacheObject>> elements) {
        if (elements == null || elements.size() == 0) {
            return;
        }
        final boolean isReadWrite = isReadWrite(elements.keySet());
        final Map<Class<?>, Cache> map = cacheLocal.get();
        Cache cache;
        for (final Entry<Class<?>, Map<Serializable, CacheObject>> entry : elements.entrySet()) {
            final Class<?> klazz = entry.getKey();
            if (null == (cache = map.get(klazz))) {
                cache = dbCacheManager.getCache(klazz);
                map.put(klazz, cache);
            }
            if (isReadWrite) {
                // if we are in r/w mode cacheSelects should have been instantiated
                final Map<Class<?>, Map<Serializable, CacheObject>> selects = cacheSelects.get();
                Map<Serializable, CacheObject> tmp = null;
                if (null != selects && null == (tmp = selects.get(klazz))) {
                    tmp = new HashMap<Serializable, CacheObject>();
                    selects.put(klazz, tmp);
                }
                tmp.putAll(entry.getValue());
            } else {
                final Cache cache2 = cache;
                final Collection<CacheObject> list = new ArrayList<CacheObject>(entry.getValue().values());
                concurrentJobQueue.execute(QUEUE_NAME, new Runnable() {
                    @Override
                    public void run() {
                        dbCacheManager.set(list, klazz, cache2);
                    }
                });
            }
        }
    }
    
    public void updateCache(Map<Class<?>, Collection<Identifiable<?>>> map) {
        if (map == null || map.size() == 0) {
            return;
        }
        final long timestamp = System.currentTimeMillis();
        for (final Entry<Class<?>, Collection<Identifiable<?>>> entry : map.entrySet()) {
            final Class<?> klazz = entry.getKey();
            final Collection<Identifiable<?>> objs = entry.getValue();
            final Map<Serializable, CacheObject> elements = dbCacheManager.getElements(klazz, objs, timestamp);
            final Map<Class<?>, Map<Serializable, CacheObject>> updates = getCacheUpdates();
            Map<Serializable, CacheObject> tmp;
            if (null == (tmp = updates.get(klazz))) {
                tmp = new HashMap<Serializable, CacheObject>();
                updates.put(klazz, tmp);
            }
            tmp.putAll(elements);
        }
    }

    public void deleteFromCacheAfterCommit(Deletable<?> obj) {
        if (obj == null) {
            return;
        }
        Map<Class<?>, Collection<Serializable>> deletes = getCacheDeletes();
        Collection<Serializable> tmp;
        final Class<?> klazz = obj.getClass();
        if (null == (tmp = deletes.get(klazz))) {
            tmp = new HashSet<Serializable>();
            deletes.put(klazz, tmp);
        }
        tmp.add(obj.getId());
    }
*/

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
//                    updateCache(cacheSelects.get());
//                    updateCache(getCacheUpdates());
//                    deleteFromCache();
                    runTasks(postCommits, postCommitsTotalTime, postCommitTasks.get(), true);
                }
/*
                private void deleteFromCache() {
                    final Map<Class<?>, Collection<Serializable>> map = getCacheDeletes();
                    if (map == null || map.size() == 0) {
                        return;
                    }
                    final boolean debug = log.isDebugEnabled();
                    for (final Entry<Class<?>, Collection<Serializable>> entry : map.entrySet()) {
                        final Class<?> klazz = entry.getKey();
                        final Collection<Serializable> ids = entry.getValue();
                        if (debug) {
                            log.debug("deleting " + ids.size() +
                                      " objects from cache, class=" + klazz.getName());
                        }
                        dbCacheManager.removeAll(ids, klazz);
                    }
                }
                private void updateCache(Map<Class<?>, Map<Serializable, CacheObject>> map) {
                    if (map == null || map.isEmpty()) {
                        return;
                    }
                    final boolean debug = log.isDebugEnabled();
                    final Map<Class<?>, Cache> cacheMap = new HashMap<Class<?>, Cache>();
                    Cache cache;
                    for (final Entry<Class<?>, Map<Serializable, CacheObject>> entry : map.entrySet()) {
                        final Class<?> klazz = entry.getKey();
                        if (null == (cache = cacheMap.get(klazz))) {
                            cache = dbCacheManager.getCache(klazz);
                            cacheMap.put(klazz, cache);
                        }
                        final Collection<CacheObject> elements = entry.getValue().values();
                        if (debug) {
                            log.debug("updating cache for " + elements.size() +
                                      " objects class=" + klazz.getName());
                        }
                        dbCacheManager.set(elements, klazz, cache);
                    }
                }
*/
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
//                    concurrentJobQueue.execute("post-commit-runner", r);
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

    @ManagedAttribute(description="Post-commit tasks - total exec time (ms)")
    public long getPostCommitsTotalExecTime() {
        return postCommitsTotalTime.get();
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
