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
    private String dieTime; // dieTime flag
    // 维护所有事务涉及的资源管理器的集合，以及与每个事务相关的资源管理器的数量。
    private HashMap<Integer, HashSet<ResourceManager>> RMs = new HashMap<>();
    // 存储所有活动事务的XID及其状态
    private HashMap<Integer, String> xids = new HashMap<>();
    // 存储需要在某些资源管理器或事务管理器故障后进行恢复的事务的XID和相关信息。
    private HashMap<Integer, Integer> xids_to_be_recovered = new HashMap<>();

    //log path
    private String xidCounterPath = "xidCounter.log";
    private String xidsStatusPath = "xidsStatus.log";
    private String xidsToBeRecoveredPath = "xidsToBeRecovered.log";

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

    // 从日志文件中读取已存储的状态，包括计数器、事务状态以及需要恢复的事务信息。
    private void recover() {
        File dataDir = new File("data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        Object xidCounterTmp = utils.loadObject("data/" + xidCounterPath);
        if (xidCounterTmp != null)
            xidCounter = (Integer) xidCounterTmp;

        Object xidsToDo = utils.loadObject("data/" + xidsToBeRecoveredPath);
        if (xidsToDo != null)
            xids_to_be_recovered = (HashMap<Integer, Integer>) xidsToDo;

        Object xidsTmp = utils.loadObject("data/" + xidsStatusPath);
        if (xidsTmp != null) {
            HashMap<Integer, String> xids_to_be_done = (HashMap<Integer, String>) xidsTmp;
            System.out.println("Redo logs");
            for (Integer xidTmp : xids_to_be_done.keySet()) {
                String[] vals = xids_to_be_done.get(xidTmp).split("_");
                String status = vals[0];
                int rm_num = Integer.parseInt(vals[1]);
                if (status.equals(COMMITTED)) {
                    // redo_logs
                    setRecoveryLater(xidTmp, rm_num);
                }
                // else, simply abort. The rms will be informed to abort transaction when they enlist
            }
            System.out.println("Finish redo logs.");
        }
    }

    public void ping() throws RemoteException {
    }

    // 向事务中添加资源管理器，处理事务的初始化和状态转换。
    // 如果事务已被恢复，则直接返回COMMITTED状态，否则根据当前状态和资源管理器的加入更新事务状态。
    public String enlist(int xid, ResourceManager rm) throws RemoteException {
        if (xids_to_be_recovered.containsKey(xid)) {
            int num = xids_to_be_recovered.get(xid);
            synchronized (xids_to_be_recovered) {
                if (num > 1)
                    xids_to_be_recovered.put(xid, num - 1);
//                else
//                    // do not remove this transaction id if rm dies after receiving the committed message.
//                    xids_to_be_recovered.remove(xid);
                utils.storeObject(xids_to_be_recovered, xidsToBeRecoveredPath);
            }
            return COMMITTED;
        }
        if (!xids.containsKey(xid)) {
            return ABORTED; // the xid has been aborted
        }
        synchronized (RMs) {
            if (!RMs.containsKey(xid)) // recover from failure.
                RMs.put(xid, new HashSet<>());
            HashSet<ResourceManager> xidRMs = RMs.get(xid);
            xidRMs.add(rm);
            synchronized (xids) {
                xids.put(xid, INITED + "_" + xidRMs.size());
                utils.storeObject(xids, "data/" + xidsStatusPath);
            }
        }
        return INITED;
    }

    // 开始新的事务，为其分配唯一的XID，并在数据结构中记录相应的状态。
    @Override
    public int start() throws RemoteException {
        synchronized (xidCounter) {
            Integer newXid = xidCounter++;
            utils.storeObject(xidCounter, "data/" + xidCounterPath);

            // store xid
            synchronized (xids) {
                xids.put(newXid, INITED + "_" + 0);
                utils.storeObject(xids, "data/" + xidsStatusPath);
            }

            synchronized (RMs) {
                RMs.put(newXid, new HashSet<>());
            }

            return newXid;
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
            throw new TransactionAbortedException(xid, "TM");
        HashSet<ResourceManager> xidRMs = RMs.get(xid);
        // 2pc
        // prepare phase
        synchronized (xids) {
            xids.put(xid, PREPARING + "_" + xidRMs.size());
            utils.storeObject(xids, "data/" + xidsStatusPath);
        }
        for (ResourceManager rm : xidRMs) {
            try {
                System.out.println("call rm prepare: " + xid + ": " + rm.getID());
                if (!rm.prepare(xid)) {
                    // rm is not prepared.
                    this.abort(xid);
                    throw new TransactionAbortedException(xid, "RM aborted");
                }
            } catch (Exception e) {
                // rm dies before or during prepare
                System.out.println("rm prepare failed: " + rm);
                e.printStackTrace();
                this.abort(xid);
                throw new TransactionAbortedException(xid, "RM aborted");
            }
        }
        // prepared, die before commit if needed
        if (dieTime.equals("BeforeCommit"))
            dieNow();

        // log commit with xid
        synchronized (xids) {
            xids.put(xid, COMMITTED + "_" + xidRMs.size());
            utils.storeObject(xids, "data/" + xidsStatusPath);
        }

        // die after commit log was written if needed.
        if (dieTime.equals("AfterCommit"))
            dieNow();

        // commit phase
        for (ResourceManager rm : xidRMs) {
            try {
                System.out.println("call rm commit " + xid + ": " + rm.getID());
                rm.commit(xid); // the function return means done signal.
            } catch (Exception e) {
                // rm dies before or during commit
                System.out.println("rm is down before commit: " + rm);
                // let the rm to be recovered when it is relaunched.
                setRecoveryLater(xid, 1);
            }
        }

        // commit log record + completion log record
        // do nothing. actually do not need completion log here because the failure of the following
        // codes can not be checked in our test condition.233
        synchronized (RMs) {
            // remove committed transactions
            RMs.remove(xid);
        }
        synchronized (xids) {
            xids.remove(xid);
            utils.storeObject(xids, "data/" + xidsStatusPath);
        }

        System.out.println("Commit xid: " + xid);
        // success
        return true;
    }

    // 在某些资源管理器故障后，将事务标记为需要在后续操作中进行恢复。
    private void setRecoveryLater(int xid, int num) {
        synchronized (xids_to_be_recovered) {
            // use number instead of rm info, for the rm message is difficult to get
            // TODO more solid implementation is needed.
            if (xids_to_be_recovered.containsKey(xid)) {
                xids_to_be_recovered.put(xid, xids_to_be_recovered.get(xid) + num);
            } else {
                xids_to_be_recovered.put(xid, num);
            }
            utils.storeObject(xids_to_be_recovered, xidsToBeRecoveredPath);
        }
    }

    // 执行事务的中止操作，通知所有涉及的资源管理器执行相应的中止操作。
    @Override
    public void abort(int xid) throws RemoteException, InvalidTransactionException {
        if (!xids.containsKey(xid)) {
            throw new InvalidTransactionException(xid, "abort");
        }
        HashSet<ResourceManager> xidRMs = RMs.get(xid);
        for (ResourceManager rm : xidRMs) {
            try {
                System.out.println("call rm abort " + xid + " : " + rm.getID());
                rm.abort(xid);
                System.out.println("rm abort success: " + rm.getID());
            } catch (Exception e) {
                System.out.println("Some RM is down: " + rm);
            }
        }
        synchronized (RMs) {
            // remove aborted transactions
            RMs.remove(xid);
        }
        synchronized (xids) {
            if (xids.containsKey(xid)) {
                xids.remove(xid);
                utils.storeObject(xids, "data/" + xidsStatusPath);
            }
        }

        System.out.println("Abort xid: " + xid);
    }

    public boolean dieNow() throws RemoteException {
        System.exit(1);
        return true; // We won't ever get here since we exited above;
        // but we still need it to please the compiler.
    }

    public void setDieTime(String dieTime) {
        this.dieTime = dieTime;
    }
}
