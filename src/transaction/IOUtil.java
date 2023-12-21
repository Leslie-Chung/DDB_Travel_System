package transaction;

import java.io.*;

public class IOUtil {

    public static boolean storeObject(Object o, String path) {
        File xidLog = new File(path);
        ObjectOutputStream oout = null;
        try {
            oout = new ObjectOutputStream(new FileOutputStream(xidLog));
            oout.writeObject(o);
            oout.flush();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            closeStream(oout);
        }
    }

    public static Object loadObject(String path) {
        File xidCounterLog = new File(path);
        ObjectInputStream oin = null;
        try {
            oin = new ObjectInputStream(new FileInputStream(xidCounterLog));
            return oin.readObject();
        } catch (Exception e) {
            return null;
        } finally {
            closeStream(oin);
        }
    }

    private static void closeStream(Closeable stream) {
        try {
            if (stream != null)
                stream.close();
        } catch (IOException e1) {
        }
    }

}
