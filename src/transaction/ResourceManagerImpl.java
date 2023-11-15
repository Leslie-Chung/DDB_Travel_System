package transaction;

import lockmgr.DeadlockException;
import lockmgr.LockManager;
import transaction.entity.ResourceItem;

import java.io.*;
import java.rmi.Naming;
import java.lang.SecurityManager;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Resource Manager for the Distributed Travel Reservation System.
 * <p>
 * Description: toy implementation of the RM
 */

public class ResourceManagerImpl extends java.rmi.server.UnicastRemoteObject implements ResourceManager {
    protected TransactionManager tm = null;
    private String myRMIName = null; // 标识资源管理器的RMI名称，用于区分不同的资源管理器。
    private String dieTime; 
    // RMs
    private HashSet xids = new HashSet(); // 存储已注册的事务ID的集合。
    private LockManager lm = new LockManager();
    
    // 存储不同事务和表之间的关系 <Integer(xid), Hashtable>
    // value也是Hashtable，存的是<string(tablename), RMTable> tablename对应某个table
    // RMTable -> <key, resourceitem> 和 <key, locktype>
    private Hashtable tables = new Hashtable();

    /*
     * 检查传入的RMI名称是否有效。
     * 设置myRMIName。
     * 调用recover()进行资源恢复。
     * 启动一个线程定期与事务管理器TM连接。
     */
    public ResourceManagerImpl(String rmiName) throws RemoteException {
        // check whether the resource is valid
        if (!(rmiName.equals(RMINameCars) || rmiName.equals(RMINameCustomers) ||
                rmiName.equals(RMINameFlights) || rmiName.equals(RMINameRooms)))
            throw new RemoteException("None valid Resource Name : " + rmiName);

        myRMIName = rmiName;
        dieTime = "NoDie";

        recover();

        while (!reconnect()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
        }

        new Thread() {
            public void run() {
                while (true) {
                    try {
                        if (tm != null)
                            tm.ping();
                    } catch (Exception e) {
                        tm = null;
                    }

                    if (tm == null) {
                        reconnect();
                        System.out.println("reconnect tm!");

                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }

                }
            }
        }.start();
    }

    /*
     * 启动资源管理器。
     * 设置安全管理器。
     * 获取RMI名称和端口。
     * 创建ResourceManagerImpl对象并绑定到RMI注册表。
     */
    public static void main(String[] args) {
        System.setSecurityManager(new SecurityManager());

        String rmiName = System.getProperty("rmiName");
        if (rmiName == null || rmiName.equals("")) {
            System.err.println("No RMI name given");
            System.exit(1);
        }

        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            ResourceManagerImpl obj = new ResourceManagerImpl(rmiName);
            Naming.rebind(rmiPort + rmiName, obj);
            System.out.println(rmiName + " bound");
        } catch (Exception e) {
            System.err.println(rmiName + " not bound:" + e);
            System.exit(1);
        }
    }

    public void setDieTime(String time) throws RemoteException {
        dieTime = time;
        System.out.println("Die time set to : " + time);
    }

    public String getID() throws RemoteException {
        return myRMIName;
    }

    // 从事务日志和数据文件中恢复资源管理器的状态。
    public void recover() {
        HashSet t_xids = loadTransactionLogs();
        if (t_xids != null)
            xids = t_xids;

        File dataDir = new File("data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        File[] datas = dataDir.listFiles();
        //main table first
        for (int i = 0; i < datas.length; i++) {
            if (datas[i].isDirectory()) {
                continue;
            }
            // log file
            if (datas[i].getName().endsWith(".log")) {
                continue;
            }
            getTable(datas[i].getName());
        }

        //xtable
        for (int i = 0; i < datas.length; i++) {
            if (!datas[i].isDirectory())
                continue;
            File[] xdatas = datas[i].listFiles();
            int xid = Integer.parseInt(datas[i].getName());
            if (!xids.contains(new Integer(xid))) {
                //this should never happen;
                throw new RuntimeException("ERROR: UNEXPECTED XID");
            }
            for (int j = 0; j < xdatas.length; j++) {
                RMTable xtable = getTable(xid, xdatas[j].getName());
                try {
//                    reacquire all locks for the transaction
//                    should ask coordinator for the status of transaction later
                    xtable.relockAll();
                } catch (DeadlockException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    // 重新连接到事务管理器。
    public boolean reconnect() {
        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            tm = (TransactionManager) Naming.lookup(rmiPort + TransactionManager.RMIName);
            System.out.println(myRMIName + "'s xids is Empty ? " + xids.isEmpty());
            for (Iterator iter = xids.iterator(); iter.hasNext(); ) {
                int xid = ((Integer) iter.next()).intValue();
                System.out.println(myRMIName + " Re-enlist to TM with xid: " + xid);
                // ask coordinator for the status of transaction
                String status = tm.enlist(xid, this);
                if (status.equals(TransactionManager.ABORTED)) {
                    System.out.println("xid has been aborted: " + xid);
                    abort(xid);
                    continue;
                } else if (status.equals(TransactionManager.COMMITTED)) {
                    System.out.println("xid has been committed: " + xid);
                    commit(xid);
                    continue;
                }
                if (dieTime.equals("AfterEnlist"))
                    dieNow();
//                iter.remove();
            }
            System.out.println(myRMIName + " bound to TM");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(myRMIName + " enlist error:" + e);
            return false;
        }

        return true;
    }

    public boolean dieNow() throws RemoteException {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //e.printStackTrace();
        }
        System.exit(1);
        return true; // We won't ever get here since we exited above;
        // but we still need it to please the compiler.
    }

    // 获取与资源管理器RM关联的事务管理器TM。
    public TransactionManager getTransactionManager() throws TransactionManagerUnaccessibleException {
        if (tm != null) {
            try {
                tm.ping();
            } catch (RemoteException e) {
                tm = null;
            }
        }
        if (tm == null) {
            if (!reconnect())
                tm = null;
        }
        if (tm == null)
            throw new TransactionManagerUnaccessibleException();
        else
            return tm;
    }

    protected RMTable loadTable(File file) {
        ObjectInputStream oin = null;
        try {
            oin = new ObjectInputStream(new FileInputStream(file));
            return (RMTable) oin.readObject();
        } catch (Exception e) {
            return null;
        } finally {
            try {
                if (oin != null)
                    oin.close();
            } catch (IOException e1) {
            }
        }
    }

    protected boolean storeTable(RMTable table, File file) {
        file.getParentFile().mkdirs();
        ObjectOutputStream oout = null;
        try {
            oout = new ObjectOutputStream(new FileOutputStream(file));
            oout.writeObject(table);
            oout.flush();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                if (oout != null)
                    oout.close();
            } catch (IOException e1) {
            }
        }
    }

    protected RMTable getTable(int xid, String tablename) {
        Hashtable xidtables = null;
        synchronized (tables) {
            xidtables = (Hashtable) tables.get(new Integer(xid));
            if (xidtables == null) {
                xidtables = new Hashtable();
                tables.put(new Integer(xid), xidtables);
            }
        }
        synchronized (xidtables) {
            RMTable table = (RMTable) xidtables.get(tablename);
            if (table != null)
                return table;
            table = loadTable(new File("data/" + (xid == -1 ? "" : "" + xid + "/") + tablename));
            if (table == null) {
                if (xid == -1)
                    table = new RMTable(tablename, null, -1, lm);
                else {
                    table = new RMTable(tablename, getTable(tablename), xid, lm);
                }
            } else {
                if (xid != -1) {
                    table.setLockManager(lm);
                    table.setParent(getTable(tablename));
                }
            }
            xidtables.put(tablename, table);
            return table;
        }
    }

    protected RMTable getTable(String tablename) {
        return getTable(-1, tablename);
    }

    protected HashSet loadTransactionLogs() {
        return (HashSet) utils.loadObject("data/transactions.log");
    }

    protected boolean storeTransactionLogs(HashSet xids) {
        return utils.storeObject(xids, "data/transactions.log");
    }

    public ResourceItem query(int xid, String tablename, Object key) throws DeadlockException,
            InvalidTransactionException, RemoteException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        try {
            synchronized (xids) {
                xids.add(new Integer(xid));
                storeTransactionLogs(xids);
            }
            getTransactionManager().enlist(xid, this);
        } catch (TransactionManagerUnaccessibleException e) {
            throw new RemoteException(e.getLocalizedMessage(), e);
        }

        if (dieTime.equals("AfterEnlist"))
            dieNow();

        // read twice, first to get lock, then to read.
        // if the item hasn't been locked by other transactions, just read twice and the results are same
        // if the item has been locked by other transactions, then wait for lock and read new result.
        // first to get lock
        RMTable table = getTable(xid, tablename);
        ResourceItem item = table.get(key);
        if (item != null && !item.isDeleted()) {
            table.lock(key, LockManager.READ);

            // then to read values
            // remove old value
            Hashtable xidtables = (Hashtable) tables.get(xid); // can not be null
            synchronized (xidtables) {
                xidtables.remove(tablename);
            }
            // read new value
            table = getTable(xid, tablename);
            item = table.get(key);
            if (!storeTable(table, new File("data/" + xid + "/" + tablename))) {
                throw new RemoteException("System Error: Can't write table to disk!");
            }
            return item;
        }
        return null;
    }

    public Collection<ResourceItem> query(int xid, String tablename, String indexName, Object indexVal) throws DeadlockException,
            InvalidTransactionException, InvalidIndexException, RemoteException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        try {
            synchronized (xids) {
                xids.add(new Integer(xid));
                storeTransactionLogs(xids);
            }
            getTransactionManager().enlist(xid, this);
        } catch (TransactionManagerUnaccessibleException e) {
            throw new RemoteException(e.getLocalizedMessage(), e);
        }

        if (dieTime.equals("AfterEnlist"))
            dieNow();

        Collection<ResourceItem> result = new ArrayList<>();

        // read twice, first to get lock, then to read.
        // if the item hasn't been locked by other transactions, just read twice and the results are same
        // if the item has been locked by other transactions, then wait for lock and read new result.
        // first to get lock
        RMTable table = getTable(xid, tablename);
        synchronized (table) {
            for (Iterator iter = table.keySet().iterator(); iter.hasNext(); ) {
                Object key = iter.next();
                ResourceItem item = table.get(key);
                if (item != null && !item.isDeleted() && item.getIndex(indexName).equals(indexVal)) {
                    table.lock(key, LockManager.READ);
                }
            }
        }

        // then to read values
        // remove old value
        Hashtable xidtables = (Hashtable) tables.get(xid); // can not be null
        synchronized (xidtables) {
            xidtables.remove(tablename);
        }
        // read new value
        table = getTable(xid, tablename);
        synchronized (table) {
            for (Iterator iter = table.keySet().iterator(); iter.hasNext(); ) {
                Object key = iter.next();
                ResourceItem item = table.get(key);
                if (item != null && !item.isDeleted() && item.getIndex(indexName).equals(indexVal)) {
                    // table.lock(key, LockManager.READ); // have been locked
                    result.add(item);
                }
            }
            if (!result.isEmpty()) {
                if (!storeTable(table, new File("data/" + xid + "/" + tablename))) {
                    throw new RemoteException("System Error: Can't write table to disk!");
                }
            }
        }
        return result;
    }

    public boolean update(int xid, String tablename, Object key, ResourceItem newItem) throws DeadlockException,
            InvalidTransactionException, RemoteException {
        /*
         * 参数验证：
         * 检查传入的事务ID (xid) 是否为正整数。
         * 检查传入的表名 (tablename) 是否有效。
         * 检查传入的键 (key) 是否与新资源项 (newItem) 的键相同。
         */
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        if (!key.equals(newItem.getKey()))
            throw new IllegalArgumentException();


        try {
            /*
            * 事务管理器注册：
            * 将当前事务ID (xid) 添加到已注册事务的集合中。
            * 将已注册事务的信息记录到事务日志中。
            */
            synchronized (xids) {
                xids.add(new Integer(xid));
                storeTransactionLogs(xids);
            }
            /*
             * 事务管理器连接：
             * 获取与资源管理器关联的事务管理器 (tm)。
             * 如果事务管理器不可访问，则尝试重新连接。
             */
            getTransactionManager().enlist(xid, this);
        } catch (TransactionManagerUnaccessibleException e) {
            throw new RemoteException(e.getLocalizedMessage(), e);
        }

        if (dieTime.equals("AfterEnlist"))
            dieNow();
        /*
         * 资源表获取：
         * 获取与当前事务和表相关联的资源表 (RMTable)。
         * 如果资源表不存在，则创建一个新的资源表。
         */
        RMTable table = getTable(xid, tablename);
        ResourceItem item = table.get(key);
        if (item != null && !item.isDeleted()) {
        /*
         * 资源项更新：
         * 使用键 (key) 获取当前资源表中的资源项 (item)。
         * 如果资源项存在且未被删除，则对资源项进行写锁定。
         * 将新资源项 (newItem) 放入资源表中。
         */
            table.lock(key, LockManager.WRITE);
            table.put(newItem);
            // 将更新后的资源表写入到磁盘，以确保数据持久化。
            if (!storeTable(table, new File("data/" + xid + "/" + tablename))) {
                throw new RemoteException("System Error: Can't write table to disk!");
            }
            return true;
        }
        return false;
    }

    public boolean insert(int xid, String tablename, ResourceItem newItem) throws DeadlockException,
            InvalidTransactionException, RemoteException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }

        try {
            synchronized (xids) {
                xids.add(new Integer(xid));
                storeTransactionLogs(xids);
            }
            getTransactionManager().enlist(xid, this);
        } catch (TransactionManagerUnaccessibleException e) {
            throw new RemoteException(e.getLocalizedMessage(), e);
        }

        if (dieTime.equals("AfterEnlist"))
            dieNow();

        RMTable table = getTable(xid, tablename);
        ResourceItem item = (ResourceItem) table.get(newItem.getKey());
        if (item != null && !item.isDeleted()) {
            return false;
        }
        table.lock(newItem.getKey(), LockManager.WRITE);
        table.put(newItem);
        if (!storeTable(table, new File("data/" + xid + "/" + tablename))) {
            throw new RemoteException("System Error: Can't write table to disk!");
        }
        return true;
    }

    public boolean delete(int xid, String tablename, Object key) throws DeadlockException, InvalidTransactionException,
            RemoteException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }

        try {
            synchronized (xids) {
                xids.add(new Integer(xid));
                storeTransactionLogs(xids);
            }
            getTransactionManager().enlist(xid, this);
        } catch (TransactionManagerUnaccessibleException e) {
            throw new RemoteException(e.getLocalizedMessage(), e);
        }

        if (dieTime.equals("AfterEnlist"))
            dieNow();

        RMTable table = getTable(xid, tablename);
        ResourceItem item = table.get(key);
        if (item != null && !item.isDeleted()) {
            table.lock(key, LockManager.WRITE);
            item = (ResourceItem) item.clone();
            item.delete();
            table.put(item);
            if (!storeTable(table, new File("data/" + xid + "/" + tablename))) {
                throw new RemoteException("System Error: Can't write table to disk!");
            }
            return true;
        }
        return false;
    }

    public int delete(int xid, String tablename, String indexName, Object indexVal) throws DeadlockException,
            InvalidTransactionException, InvalidIndexException, RemoteException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        try {
            synchronized (xids) {
                xids.add(new Integer(xid));
                storeTransactionLogs(xids);
            }
            getTransactionManager().enlist(xid, this);
        } catch (TransactionManagerUnaccessibleException e) {
            throw new RemoteException(e.getLocalizedMessage(), e);
        }

        if (dieTime.equals("AfterEnlist"))
            dieNow();

        int n = 0;

        RMTable table = getTable(xid, tablename);
        synchronized (table) {
            for (Iterator iter = table.keySet().iterator(); iter.hasNext(); ) {
                Object key = iter.next();
                ResourceItem item = table.get(key);
                if (item != null && !item.isDeleted() && item.getIndex(indexName).equals(indexVal)) {
                    table.lock(item.getKey(), LockManager.WRITE);
                    item = (ResourceItem) item.clone();
                    item.delete();
                    table.put(item);
                    n++;
                }
            }
            if (n > 0) {
                if (!storeTable(table, new File("data/" + xid + "/" + tablename))) {
                    throw new RemoteException("System Error: Can't write table to disk!");
                }
            }
        }
        return n;
    }

    public boolean prepare(int xid) throws InvalidTransactionException, RemoteException {
        /*
         * 当 dieTime 被设置为 "BeforePrepare" 时，系统在 prepare 函数执行之前就会终止。
         * 这意味着事务在准备阶段之前就会因为某种异常情况而失败，模拟了事务准备阶段的失败场景。
         * 在实际的分布式系统中，可能会发生各种故障，例如网络故障、参与者故障等，导致事务在准备阶段失败。
         */
        if (dieTime.equals("BeforePrepare"))
            dieNow();

        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }

        // AfterPrepare: die after it has entered the prepared state, but just before it
        //     * could reply "prepared" to the TM.
        /*
         * 当 dieTime 被设置为 "AfterPrepare" 时，系统在 prepare 函数执行之后，即事务已经进入准备就绪状态，但在向事务协调器（Transaction Manager）发送最终提交之前，系统就会终止。
         * 这模拟了在事务准备阶段成功后，但在最终提交之前发生的异常情况。
         * 在实际系统中，这可能包括事务协调器崩溃或无法与参与者通信等异常情况。
         */
        if (dieTime.equals("AfterPrepare"))
            dieNow();

        System.out.println("Prepared: " + xid);
        return true;
    }

    public void commit(int xid) throws InvalidTransactionException, RemoteException {
        if (dieTime.equals("BeforeCommit"))
            dieNow();
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        // 从存储事务表的数据结构中获取每个表的 RMTable 对象。
        Hashtable xidtables = (Hashtable) tables.get(new Integer(xid));
        if (xidtables != null) {
            synchronized (xidtables) {
                for (Iterator iter = xidtables.entrySet().iterator(); iter.hasNext(); ) {
                    /*
                     * 对于每个表，将事务中的每个项写回到对应的表中。
                     * 如果某个项被标记为删除，就从表中移除该项；否则，将该项放入表中。
                     */
                    Map.Entry entry = (Map.Entry) iter.next();
                    RMTable xtable = (RMTable) entry.getValue();
                    RMTable table = getTable(xtable.getTablename());
                    for (Iterator iter2 = xtable.keySet().iterator(); iter2.hasNext(); ) {
                        Object key = iter2.next();
                        ResourceItem item = xtable.get(key);
                        if (item.isDeleted())
                            table.remove(item);
                        else
                            table.put(item);
                    }
                    //  将更新后的表写回到磁盘，以确保数据的持久性。
                    if (!storeTable(table, new File("data/" + entry.getKey())))
                        throw new RemoteException("Can't write table to disk");
                    // 删除事务中生成的临时表文件
                    new File("data/" + xid + "/" + entry.getKey()).delete();
                }
                // 删除事务目录，该目录包含了该事务涉及的所有表文件。
                new File("data/" + xid).delete();
                tables.remove(new Integer(xid));
            }
        }
        // 释放事务中的所有锁
        if (!lm.unlockAll(xid))
            throw new RuntimeException();
        // 从全局事务列表中移除该事务
        synchronized (xids) {
            xids.remove(new Integer(xid));
        }

        System.out.println("Commit xid: " + xid);
    }

    public void abort(int xid) throws InvalidTransactionException, RemoteException {
        if (dieTime.equals("BeforeAbort"))
            dieNow();
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        Hashtable xidtables = (Hashtable) tables.get(new Integer(xid));
        if (xidtables != null) {
            synchronized (xidtables) {
                for (Iterator iter = xidtables.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    new File("data/" + xid + "/" + entry.getKey()).delete();
                }
                new File("data/" + xid).delete();
                tables.remove(new Integer(xid));
            }
        }

        if (!lm.unlockAll(xid))
            throw new RuntimeException();

        synchronized (xids) {
            xids.remove(new Integer(xid));
        }
        System.out.println("Abort xid: " + xid);
    }
}