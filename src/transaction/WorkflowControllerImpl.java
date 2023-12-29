package transaction;

import lockmgr.DeadlockException;
import transaction.entity.*;

import java.rmi.Naming;
import java.lang.SecurityManager;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class WorkflowControllerImpl
        extends java.rmi.server.UnicastRemoteObject
        implements WorkflowController {

    protected TransactionManager tm = null;
    private ResourceManager rmFlights = null;
    private ResourceManager rmRooms = null;
    private ResourceManager rmCars = null;
    private ResourceManager rmCustomers = null;

    private HashSet<Integer> xids = new HashSet<>();

    private String WCLog = "log/WC.log";

    /*
     * 通过调用 recover() 方法进行恢复，并尝试重新连接资源管理器。
     * 如果连接不成功，会等待一段时间后重试。
     */
    public WorkflowControllerImpl() throws RemoteException, InterruptedException {
        recover();

        while (!reconnect()) {
            Thread.sleep(500);
        }
    }

    // 启动 WorkflowControllerImpl 实例，并将其绑定到 RMI 注册中心。
    public static void main(String args[]) {
        System.setSecurityManager(new SecurityManager());

        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            WorkflowControllerImpl obj = new WorkflowControllerImpl();
            Naming.rebind(rmiPort + WorkflowController.RMIName, obj);
            System.out.println("WC bound");
        } catch (Exception e) {
            System.err.println("WC not bound:" + e);
            System.exit(1);
        }
    }

    private void recover() {
        Object xids_ = IOUtil.loadObject(WCLog);
        if (xids_ != null)
            xids = (HashSet<Integer>) xids_;
    }

    public int start()
            throws RemoteException {
        int xid = tm.start();
        xids.add(xid);
        IOUtil.storeObject(xids, WCLog);
        return xid;
    }

    public boolean commit(int xid)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (!xids.contains(xid))
            throw new InvalidTransactionException(xid, "WC commit error");
        boolean result = tm.commit(xid);
        xids.remove(xid);
        IOUtil.storeObject(xids, WCLog);
        return result;
    }

    public void abort(int xid)
            throws RemoteException,
            InvalidTransactionException {
        if (!xids.contains(xid))
            throw new InvalidTransactionException(xid, "WC abort error");
        tm.abort(xid);
        xids.remove(xid);
        IOUtil.storeObject(xids, WCLog);
    }

    public boolean addFlight(int xid, String flightNum, int numSeats, int price)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (flightNum == null || numSeats < 0)
            return false;
        ResourceItem item = queryItem(rmFlights, xid, flightNum);

        if (item != null) { 
            Flight flight = (Flight) item;
            flight.addSeats(numSeats);
            if (price >= 0)
                flight.setPrice(price);
            return updateFlight(xid, flightNum, flight);
        } else { 
            if (price < 0)
                price = 0;
            Flight flight = new Flight(flightNum, price, numSeats);
            return insertFlight(xid, flight);
        }
    }

    private boolean updateFlight(int xid, String flightNum, Flight flight)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmFlights.update(xid, rmFlights.getID(), flightNum, flight);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
    }

    private boolean insertFlight(int xid, Flight flight)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmFlights.insert(xid, rmFlights.getID(), flight);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
    }

    public boolean deleteFlight(int xid, String flightNum)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (!xids.contains(xid))
            throw new InvalidTransactionException(xid, "deleteFlight error");
        try {
            Collection<ResourceItem> reservations = rmCustomers.query(xid, ResourceManager.TableNameReservations,
                    Reservation.INDEX_RESERV_KEY, flightNum);
            if (!reservations.isEmpty())
                return false;
            ResourceItem item = queryItem(rmFlights, xid, flightNum);
            if (item == null)
                return false;
            rmFlights.delete(xid, rmFlights.getID(), flightNum);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        } catch (InvalidIndexException e) {
            System.err.println("InvalidIndexException: " + e.getMessage());
        }
        return true;
    }

    private ResourceItem queryItem(ResourceManager rm, int xid, String key)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (!xids.contains(xid))
            throw new InvalidTransactionException(xid, "queryItem error");

        ResourceItem item = null;
        try {
            item = rm.query(xid, rm.getID(), key);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }

        return item;
    }

    public boolean addRooms(int xid, String location, int numRooms, int price)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (location == null || numRooms < 0)
            return false;

        ResourceItem item = queryItem(rmRooms, xid, location);

        if (item != null) {
            Hotel hotel = (Hotel) item;
            hotel.addRooms(numRooms);
            if (price >= 0)
                hotel.setPrice(price);
            return updateHotel(xid, location, hotel);
        } else {
            if (price < 0)
                price = 0;
            Hotel hotel = new Hotel(location, price, numRooms);
            return insertHotel(xid, hotel);
        }
    }

    private boolean updateHotel(int xid, String location, Hotel hotel)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmRooms.update(xid, rmRooms.getID(), location, hotel);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
    }

    private boolean insertHotel(int xid, Hotel hotel)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmRooms.insert(xid, rmRooms.getID(), hotel);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
    }

    public boolean deleteRooms(int xid, String location, int numRooms)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (numRooms < 0)
            return false;
        ResourceItem item = queryItem(rmRooms, xid, location);
        if (item == null)
            return false;
        Hotel hotel = (Hotel) (item);
        if (hotel.getNumAvail() < numRooms)
            return false;
        hotel.addRooms(-numRooms);
        return updateHotel(xid, location, hotel);
    }

    public boolean addCars(int xid, String location, int numCars, int price)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (location == null || numCars < 0)
            return false;

        ResourceItem item = queryItem(rmCars, xid, location);

        if (item != null) {
            Car c = (Car) item;
            c.addCars(numCars);
            if (price >= 0)
                c.setPrice(price);
            return updateCar(xid, location, c);
        } else {
            if (price < 0)
                price = 0;
            Car car = new Car(location, price, numCars);
            return insertCar(xid, car);
        }
    }

    private boolean updateCar(int xid, String location, Car c) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmCars.update(xid, rmCars.getID(), location, c);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
    }

    private boolean insertCar(int xid, Car car) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmCars.insert(xid, rmCars.getID(), car);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
    }

    public boolean deleteCars(int xid, String location, int numCars)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (numCars < 0)
            return false;
        ResourceItem item = queryItem(rmCars, xid, location);
        if (item == null)
            return false;
        Car c = (Car) item;
        if (c.getNumAvail() < numCars)
            return false;
        c.addCars(-numCars);
        return updateCar(xid, rmCars.getID(), c);
    }

    public boolean newCustomer(int xid, String custName)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        ResourceItem item = queryItem(rmCustomers, xid, custName);
        if (item != null)
            return true;
        Customer customer = new Customer(custName);
        try {
            return rmCustomers.insert(xid, rmCustomers.getID(), customer);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
    }

    private void concelAll(int xid, String custName) throws InvalidTransactionException,
            RemoteException, TransactionAbortedException, DeadlockException, InvalidIndexException {
        Collection<ResourceItem> results = rmCustomers.query(xid, ResourceManager.TableNameReservations,
                Reservation.INDEX_CUSTNAME, custName);
        for (ResourceItem re : results) {
            Reservation reservation = (Reservation) re;
            String resvKey = reservation.getResvKey();
            switch (reservation.getResvType()) {
                case Reservation.RESERVATION_TYPE_FLIGHT: {
                    Flight flight = (Flight) queryItem(rmFlights, xid, resvKey);
                    flight.bookSeats(-1);
                    updateFlight(xid, resvKey, flight);
                    break;
                }
                case Reservation.RESERVATION_TYPE_CAR: {
                    Car c = (Car) queryItem(rmCars, xid, resvKey);
                    c.bookCars(-1);
                    updateCar(xid, resvKey, c);
                    break;
                }
                case Reservation.RESERVATION_TYPE_HOTEL: {
                    Hotel hotel = (Hotel) queryItem(rmRooms, xid, resvKey);
                    hotel.bookRooms(-1);
                    updateHotel(xid, resvKey, hotel);
                }
            }
        }
    }

    public boolean deleteCustomer(int xid, String custName)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (custName == null)
            return false;

        ResourceItem item = queryItem(rmCustomers, xid, custName);
        if (item == null)
            return false;

        try {
            rmCustomers.delete(xid, rmCustomers.getID(), custName);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }

        try {
            concelAll(xid, custName);
            rmCustomers.delete(xid, ResourceManager.TableNameReservations, Reservation.INDEX_CUSTNAME, custName);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        } catch (InvalidIndexException e) {
            System.err.println(e.getMessage());
        }
        return true;
    }

    public int queryFlight(int xid, String flightNum)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (flightNum == null)
            return -1;
        ResourceItem item = queryItem(rmFlights, xid, flightNum);
        if (item == null)
            return -1;
        return ((Flight) item).getNumAvail();
    }

    public int queryFlightPrice(int xid, String flightNum)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (flightNum == null)
            return -1;
        ResourceItem item = queryItem(rmFlights, xid, flightNum);
        if (item == null)
            return -1;
        return ((Flight) item).getPrice();
    }

    public int queryRooms(int xid, String location)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (location == null)
            return -1;
        ResourceItem item = queryItem(rmRooms, xid, location);
        if (item == null)
            return -1;
        return ((Hotel) item).getNumAvail();
    }

    public int queryRoomsPrice(int xid, String location)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (location == null)
            return -1;
        ResourceItem item = queryItem(rmRooms, xid, location);
        if (item == null)
            return -1;
        return ((Hotel) item).getPrice();
    }

    public int queryCars(int xid, String location)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (location == null)
            return -1;
        ResourceItem item = queryItem(rmCars, xid, location);
        if (item == null)
            return -1;
        return ((Car) item).getNumAvail();
    }

    public int queryCarsPrice(int xid, String location)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (location == null)
            return -1;
        ResourceItem item = queryItem(rmCars, xid, location);
        if (item == null)
            return -1;
        return ((Car) item).getPrice();
    }

    public int queryCustomerBill(int xid, String custName)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (custName == null)
            return -1;
        ResourceItem item = queryItem(rmCustomers, xid, custName);
        if (item == null)
            return -1;
        Collection<ResourceItem> results = null;
        try {
            results = rmCustomers.query(xid, ResourceManager.TableNameReservations,
                    Reservation.INDEX_CUSTNAME, custName);
            if (results == null)
                return 0;
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        } catch (InvalidIndexException e) {
            System.err.println(e.getMessage());
        }

        int total_bill = 0;
        for (ResourceItem re : results) {
            Reservation reservation = (Reservation) re;
            total_bill += reservation.getBill();
        }
        return total_bill;
    }

    public boolean reserveFlight(int xid, String custName, String flightNum)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (custName == null || flightNum == null)
            return false;
        ResourceItem customer = queryItem(rmCustomers, xid, custName);
        if (customer == null)
            return false;
        ResourceItem item = queryItem(rmFlights, xid, flightNum);
        if (item == null)
            return false;
        Flight flight = (Flight) item;
        if (flight.getNumAvail() <= 0)
            return false;
        Reservation reservation = new Reservation(custName, Reservation.RESERVATION_TYPE_FLIGHT, flightNum, flight.getPrice());
        try {
            rmCustomers.insert(xid, ResourceManager.TableNameReservations, reservation);
            flight.bookSeats(1);
            rmFlights.update(xid, rmFlights.getID(), flightNum, flight);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
        return true;
    }

    public boolean reserveCar(int xid, String custName, String location)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (custName == null || location == null)
            return false;
        ResourceItem customer = queryItem(rmCustomers, xid, custName);
        if (customer == null)
            return false;
        ResourceItem car = queryItem(rmCars, xid, location);
        if (car == null)
            return false;
        Car c = (Car) car;
        if (c.getNumAvail() <= 0)
            return false;
        Reservation reservation = new Reservation(custName, Reservation.RESERVATION_TYPE_CAR, location, c.getPrice());
        try {
            rmCustomers.insert(xid, ResourceManager.TableNameReservations, reservation);
            c.bookCars(1);
            rmCars.update(xid, rmCars.getID(), location, c);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
        return true;
    }

    public boolean reserveRoom(int xid, String custName, String location)
            throws RemoteException,
            TransactionAbortedException,
            InvalidTransactionException {
        if (custName == null || location == null)
            return false;
        ResourceItem customer = queryItem(rmCustomers, xid, custName);
        if (customer == null)
            return false;
        ResourceItem item = queryItem(rmRooms, xid, location);
        if (item == null)
            return false;

        Hotel hotel = (Hotel) item;
        if (hotel.getNumAvail() <= 0)
            return false;
        Reservation reservation = new Reservation(custName, Reservation.RESERVATION_TYPE_HOTEL, location, hotel.getPrice());
        try {
            rmCustomers.insert(xid, ResourceManager.TableNameReservations, reservation);
            hotel.bookRooms(1);
            rmRooms.update(xid, rmRooms.getID(), location, hotel);
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }
        return true;
    }

    @Override
    public boolean reserveItinerary(int xid, String custName, List flightNumList, String location, boolean needCar,
            boolean needRoom) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (custName == null || location == null || flightNumList == null)
            return false;
        ResourceItem customer = queryItem(rmCustomers, xid, custName);
        if (customer == null)
            return false;

        for (Object flight : flightNumList) {
            String flightNum = (String) flight;
            ResourceItem item = queryItem(rmFlights, xid, flightNum);
            if (item == null)
                return false;
            Flight flight_ = (Flight) item;
            if (flight_.getNumAvail() <= 0)
                return false;
        }
        if (needRoom) {
            ResourceItem item = queryItem(rmRooms, xid, location);
            if (item == null)
                return false;
            Hotel hotel = (Hotel) item;
            if (hotel.getNumAvail() <= 0)
                return false;
        }
        if (needCar) {
            ResourceItem item = queryItem(rmCars, xid, location);
            if (item == null)
                return false;
            Car c = (Car) item;
            if (c.getNumAvail() <= 0)
                return false;
        }
        
        try {
            rmCustomers.update(xid, rmCustomers.getID(), custName, customer); // just to set WRITE lock for the test
        } catch (DeadlockException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, "dead lock: " + e.getMessage());
        }

        for (Object flight : flightNumList) {
            String flightNum = (String) flight;
            if (!reserveFlight(xid, custName, flightNum))
                return false;
        }
        if (needRoom) {
            if (!reserveRoom(xid, custName, location))
                return false;
        }
        if (needCar) {
            if (!reserveCar(xid, custName, location))
                return false;
        }
        return true;
    }

    public boolean reconnect()
            throws RemoteException {
        /*
         * 获取RMI端口： 从系统属性中获取RMI端口信息。
         */
        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }
        /*
         * 重新连接到各个组件：
         * 使用 Naming.lookup 方法重新查找并绑定到各个 ResourceManager 和 TransactionManager。
         * 这样可以确保 WorkflowControllerImpl 重新获取对这些组件的引用。
         */
        try {
            rmFlights = (ResourceManager) Naming.lookup(rmiPort +
                    ResourceManager.RMINameFlights);
            System.out.println("WC bound to RMFlights");
            rmRooms = (ResourceManager) Naming.lookup(rmiPort +
                    ResourceManager.RMINameRooms);
            System.out.println("WC bound to RMRooms");
            rmCars = (ResourceManager) Naming.lookup(rmiPort +
                    ResourceManager.RMINameCars);
            System.out.println("WC bound to RMCars");
            rmCustomers = (ResourceManager) Naming.lookup(rmiPort +
                    ResourceManager.RMINameCustomers);
            System.out.println("WC bound to RMCustomers");
            tm = (TransactionManager) Naming.lookup(rmiPort +
                    TransactionManager.RMIName);
            System.out.println("WC bound to TM");
        } catch (Exception e) {
            System.err.println("WC cannot bind to some component:" + e);
            return false;
        }

        /*
         * 检查重连结果：
         * 调用各个 ResourceManager 的 reconnect 方法检查是否能够重新连接到 TransactionManager。
         */
        try {
            if (rmFlights.reconnect() && rmRooms.reconnect() &&
                    rmCars.reconnect() && rmCustomers.reconnect()) {
                System.out.println("All RMs connect to TM.");
                return true;
            } else {
                System.err.println("Some RM cannot reconnect.");
            }
        } catch (Exception e) {
            System.err.println("Some RM cannot reconnect:" + e);
            return false;
        }

        return false;
    }

    public boolean dieNow(String who)
            throws RemoteException {
        if (who.equals(TransactionManager.RMIName) ||
                who.equals("ALL")) {
            try {
                tm.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(ResourceManager.RMINameFlights) ||
                who.equals("ALL")) {
            try {
                rmFlights.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(ResourceManager.RMINameRooms) ||
                who.equals("ALL")) {
            try {
                rmRooms.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(ResourceManager.RMINameCars) ||
                who.equals("ALL")) {
            try {
                rmCars.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(ResourceManager.RMINameCustomers) ||
                who.equals("ALL")) {
            try {
                rmCustomers.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(WorkflowController.RMIName) ||
                who.equals("ALL")) {
            System.exit(1);
        }
        return true;
    }

    // 模拟在资源管理器完成事务 enlist 后宕机
    public boolean dieRMAfterEnlist(String who)
            throws RemoteException {
        return dieRMByTime(who, "AfterEnlist");
    }

    private boolean dieRMByTime(String who, String time) throws RemoteException {
        switch (who) {
            case ResourceManager.RMINameFlights: {
                rmFlights.setDieTime(time);
                break;
            }
            case ResourceManager.RMINameCars: {
                rmCars.setDieTime(time);
                break;
            }
            case ResourceManager.RMINameCustomers: {
                rmCustomers.setDieTime(time);
                break;
            }
            case ResourceManager.RMINameRooms: {
                rmRooms.setDieTime(time);
                break;
            }
            default: {
                System.err.println("Invalid RM: " + who);
                return false;
            }
        }
        return true;
    }

    public boolean dieRMBeforePrepare(String who)
            throws RemoteException {
        return dieRMByTime(who, "BeforePrepare");
    }

    public boolean dieRMAfterPrepare(String who)
            throws RemoteException {
        return dieRMByTime(who, "AfterPrepare");
    }

    // 模拟在事务管理器进行 commit 操作前宕机
    public boolean dieTMBeforeCommit()
            throws RemoteException {
        tm.setDieTime("BeforeCommit");
        return true;
    }

    public boolean dieTMAfterCommit()
            throws RemoteException {
        tm.setDieTime("AfterCommit");
        return true;
    }

    // 模拟在资源管理器进行 commit 操作前宕机
    public boolean dieRMBeforeCommit(String who)
            throws RemoteException {
        return dieRMByTime(who, "BeforeCommit");
    }

    public boolean dieRMBeforeAbort(String who)
            throws RemoteException {
        return dieRMByTime(who, "BeforeAbort");
    }
}
