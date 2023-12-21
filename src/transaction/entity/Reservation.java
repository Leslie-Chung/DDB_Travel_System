/*
 * Created on 2005-5-17
 *
 */
package transaction.entity;

import transaction.InvalidIndexException;

/**
 * 预定
 */
public class Reservation extends ResourceItem {
	public static final String INDEX_CUSTNAME = "custName";

	public static final int RESERVATION_TYPE_FLIGHT = 1;

	public static final int RESERVATION_TYPE_HOTEL = 2;

	public static final int RESERVATION_TYPE_CAR = 3;

	protected String custName;

	protected int resvType;

	protected String resvKey;

	protected boolean isdeleted = false;

    public static final String INDEX_RESERV_KEY = "resvKey";
    private int bill;

    public Reservation(String custName, int resvType, String resvKey, int bill) {
        this.custName = custName;
        this.resvType = resvType;
        this.resvKey = resvKey;
        this.bill = bill;
    }

    public Object getIndex(String indexName) throws InvalidIndexException {
        if (indexName.equals(INDEX_CUSTNAME))
            return custName;
        else if (indexName.equals(INDEX_RESERV_KEY))
            return resvKey;
        else
            throw new InvalidIndexException(indexName);
    }

    public Object getKey() {
        return new ReservationKey(custName, resvType, resvKey);
    }

    public int getBill() {
        return bill;
    }

    /**
     * @return Returns the custName.
     */
    public String getCustName() {
        return custName;
    }

    /**
     * @return Returns the resvKey.
     */
    public String getResvKey() {
        return resvKey;
    }

    /**
     * @return Returns the resvType.
     */
    public int getResvType() {
        return resvType;
    }

    public Object clone() {
        Reservation o = new Reservation(getCustName(), getResvType(),
                getResvKey(), getBill());
        o.isdeleted = isdeleted;
        return o;
    }
}