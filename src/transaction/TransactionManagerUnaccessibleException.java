/*
 * Created on 2005-5-18
 *
 */
package transaction;

public class TransactionManagerUnaccessibleException extends Exception {
    public TransactionManagerUnaccessibleException() {
        super("Transaction Manager Unaccessible");
    }
}
