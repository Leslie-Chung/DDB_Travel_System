package transaction;

import java.io.File;
import java.rmi.Naming;
import java.lang.SecurityManager;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * 协调和管理多个资源管理器的参与，并通过日志文件记录状态以支持故障恢复。
 */

public class TransactionManagerImpl
        extends java.rmi.server.UnicastRemoteObject
        implements TransactionManager {

    private Integer xidCounter; // 为每个事务分配唯一的事务标识符（XID）的计数器。
    private String dieTime;
    // 维护所有事务涉及的资源管理器的集合，以及与每个事务相关的资源管理器的数量。
    private HashMap<Integer, HashSet<ResourceManager>> RMs = new HashMap<>();
    // 存储所有活动事务的XID及其状态
    private HashMap<Integer, String> xids = new HashMap<>();

    private static final String counterLog = "counter.log";
    private static final String statusLog = "status.log";
    private static final String needRecoveredLog = "needRecovered.log";
    // 存储需要在某些资源管理器或事务管理器故障后进行恢复的事务的XID和相关信息。
    private HashMap<Integer, Integer> xidsToRecover = new HashMap<>();

    public boolean dieNow() throws RemoteException {
        System.exit(1);
        return true; // We won't ever get here since we exited above;
        // but we still need it to please the compiler.
    }

    public void setDieTime(String dieTime) {
        this.dieTime = dieTime;
    }

    public void ping() throws RemoteException {
        System.out.println("TM Get ping signal");
    }

    public TransactionManagerImpl() throws RemoteException {
        xidCounter = 1;
        dieTime = "noDie";

        recover();
    }

    // 实例化 TransactionManagerImpl 对象并将其绑定到RMI注册表。
    public static void main(String[] args) {
        System.setSecurityManager(new SecurityManager());

        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            TransactionManagerImpl obj = new TransactionManagerImpl();
            Naming.rebind(rmiPort + TransactionManager.RMIName, obj);

            System.out.println("TM bound");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("TM not bound:" + e);
            System.exit(1);
        }
    }

    private void storeLog(Object o, String path) {
        IOUtil.storeObject(o, "log/" + path);
    }

    private Object loadLog(String path) {
        return IOUtil.loadObject("log/" + path);
    }

    // 从日志文件中读取已存储的状态，包括计数器、事务状态以及需要恢复的事务信息。
    private void recover() {
        createDataDir();

        xidCounter = (Integer) loadLogAndProcessResult(counterLog, xidCounter);
        xidsToRecover = (HashMap<Integer, Integer>) loadLogAndProcessResult(needRecoveredLog, xidsToRecover);
        HashMap<Integer, String> xids_ = (HashMap<Integer, String>) loadLogAndProcessResult(statusLog, null);
        if (xids_ != null) {
            xids_.entrySet().stream().forEach(entry -> {
                String[] values = entry.getValue().split("_");
                String status = values[0];
                int rmNum = Integer.parseInt(values[1]);
                if (status.equals("COMMITTED")) {
                    setRecoveryLater(entry.getKey(), rmNum);
                }
            });
        }
    }

    private void createDataDir() {
        File dataDir = new File("log");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
    }

    private Object loadLogAndProcessResult(String log, Object defaultValue) {
        Object result = loadLog(log);
        return result != null ? result : defaultValue;
    }

    // 向事务中添加资源管理器，处理事务的初始化和状态转换。
    // 如果事务已被恢复，则直接返回COMMITTED状态，否则根据当前状态和资源管理器的加入更新事务状态。
    public String enlist(int xid, ResourceManager rm) throws RemoteException {
        if (xidsToRecover.containsKey(xid)) {
            updateXidsToRecover(xid);
            return "COMMITTED";
        }
        if (!xids.containsKey(xid)) {
            return "ABORTED";
        }
        updateRMsAndXids(xid, rm);
        return "INITED";
    }

    private void updateXidsToRecover(int xid) {
        int num = xidsToRecover.get(xid);
        synchronized (xidsToRecover) {
            if (num > 1)
                xidsToRecover.put(xid, num - 1);
            IOUtil.storeObject(xidsToRecover, needRecoveredLog);
        }
    }

    private void updateRMsAndXids(int xid, ResourceManager rm) {
        synchronized (RMs) {
            if (!RMs.containsKey(xid)) // recover from failure.
                RMs.put(xid, new HashSet<>());
            HashSet<ResourceManager> relatedRMs = RMs.get(xid);
            relatedRMs.add(rm);
            synchronized (xids) {
                xids.put(xid, "INITED_" + relatedRMs.size());
                storeLog(xids, statusLog);
            }
        }
    }

    // 开始新的事务，为其分配唯一的XID，并在数据结构中记录相应的状态。
    @Override
    public int start() throws RemoteException {
        synchronized (xidCounter) {
            Integer xid = xidCounter++;
            storeLog(xidCounter, counterLog);

            synchronized (xids) {
                xids.put(xid, "INITED_" + 0);
                storeLog(xids, counterLog);
            }

            synchronized (RMs) {
                RMs.put(xid, new HashSet<>());
            }

            return xid;
        }
    }

    /*
     * 实现了两阶段提交协议（2PC）。
     * 在第一阶段，执行准备（prepare）操作，向所有涉及的资源管理器发送准备请求。
     * 如果所有资源管理器都准备就绪，则进入第二阶段，执行提交操作。
     * 根据dieTime标志，可能在提交前或提交后终止事务。
     */
    @Override
    public boolean commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (!xids.containsKey(xid))
            throw new TransactionAbortedException(xid, "TM aborted");
        HashSet<ResourceManager> relatedRMs = RMs.get(xid);
        synchronized (xids) {
            xids.put(xid, "PREPARE_" + relatedRMs.size());
            storeLog(xids, statusLog);
        }

        prepareRMs(xid, relatedRMs);
        checkDieTime("BeforeCommit");
        
        synchronized (xids) {
            xids.put(xid, "COMMITTED_" + relatedRMs.size());
            storeLog(xids, statusLog);
        }

        checkDieTime("AfterCommit");

        commitRMs(xid, relatedRMs);

        synchronized (RMs) {
            RMs.remove(xid);
        }
        synchronized (xids) {
            xids.remove(xid);
            storeLog(xids, statusLog);
        }

        System.out.println("Commit xid: " + xid);
        return true;
    }

    private void checkDieTime(String time) throws RemoteException {
        if (dieTime.equals(time))
            dieNow();
    }

    private void prepareRMs(int xid, HashSet<ResourceManager> RMs)
            throws TransactionAbortedException, RemoteException, InvalidTransactionException {
        for (ResourceManager rm : RMs) {
            try {
                System.out.println("rm: " + rm.getID() + " try to prepare transaction: " + xid);
                if (!rm.prepare(xid)) {
                    this.abort(xid);
                    throw new TransactionAbortedException(xid, "RM aborted");
                }
                System.out.println("rm: " + rm.getID() + " prepare successfully");
            } catch (Exception e) {
                System.out.println("rm prepare failed: " + rm);
                e.printStackTrace();
                abort(xid);
                throw new TransactionAbortedException(xid, "RM aborted");
            }
        }
    }

    private void commitRMs(int xid, HashSet<ResourceManager> RMs) {
        for (ResourceManager rm : RMs) {
            try {
                System.out.println("rm: " + rm.getID() + " try to commit transaction: " + xid);
                rm.commit(xid);
                System.out.println("rm: " + rm.getID() + " commit successfully");
            } catch (Exception e) {
                System.out.println("rm commit failed: " + rm);
                setRecoveryLater(xid, 1);
            }
        }
    }

    // 在某些资源管理器故障后，将事务标记为需要在后续操作中进行恢复。
    private void setRecoveryLater(int xid, int num) {
        synchronized (xidsToRecover) {
            if (xidsToRecover.containsKey(xid)) {
                xidsToRecover.put(xid, xidsToRecover.get(xid) + num);
            } else {
                xidsToRecover.put(xid, num);
            }
            IOUtil.storeObject(xidsToRecover, needRecoveredLog);
        }
    }

    // 执行事务的中止操作，通知所有涉及的资源管理器执行相应的中止操作。
    @Override
    public void abort(int xid) throws RemoteException, InvalidTransactionException {
        if (!xids.containsKey(xid)) {
            throw new InvalidTransactionException(xid, "TM aborted");
        }
        HashSet<ResourceManager> relatedRMs = RMs.get(xid);
        for (ResourceManager rm : relatedRMs) {
            try {
                System.out.println("rm: " + rm.getID() + " try to abort transaction: " + xid);
                rm.abort(xid);
                System.out.println("rm: " + rm.getID() + " abort successfully");
            } catch (Exception e) {
                System.out.println("rm abort failed:: " + rm);
            }
        }
        synchronized (RMs) {
            RMs.remove(xid);
        }
        synchronized (xids) {
            if (xids.containsKey(xid)) {
                xids.remove(xid);
                storeLog(xids, statusLog);
            }
        }

        System.out.println("abort xid: " + xid);
    }

}
