import java.io.Serializable;
import java.util.Collections;
import java.util.Vector;

public class Bucket implements Serializable {
    //String pageFilePath;
    Vector<KeyPointerPair> keyPointerPairs;
    public Bucket(KeyPointerPair keyPointerPair){
        keyPointerPairs = new Vector<>();
        keyPointerPairs.add(keyPointerPair);

    }

    public void insert(KeyPointerPair keyPointerPair){
        keyPointerPairs.add(keyPointerPair);
        Collections.sort(keyPointerPairs);
    }


}
