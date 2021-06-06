import java.util.Vector;

public class KeyPointerPair implements Comparable{

    Vector<String> key;
    String pointer;
    int rowNum;
    public KeyPointerPair(Vector<String> key,String pointer, int rowNum){
        this.key=key;
        this.pointer=pointer;
        this.rowNum=rowNum;
    }

    @Override
    public int compareTo(Object o) {
        KeyPointerPair keyPointerPair = (KeyPointerPair)o;
        for(int i=0;i<keyPointerPair.key.size();i++){
            if(this.key.get(i).compareTo(keyPointerPair.key.get(i))==0){
                continue;
            }
            else{
                return this.key.get(i).compareTo(keyPointerPair.key.get(i));
            }


        }
        return 0;

    }

    public static void main(String[] args) {

        Vector<String> v1 = new Vector<String>();
        v1.add("seif");
        v1.add("21");
        v1.add("6");
        Vector<String> v2 = new  Vector<String>();
        v2.add("seif");
        v2.add("21");
        v2.add("0");
        Vector<String> v3 = new  Vector<String>();
        v3.add("ahmed");
        v3.add("16");
        v3.add("0");
//        KeyPointerPair p1 = new KeyPointerPair(v1, "seif");
//        KeyPointerPair p2 = new KeyPointerPair(v2, "donia");
//        KeyPointerPair p3 = new KeyPointerPair(v3, "salma");
//
//        Bucket b = new Bucket(p1);
//        b.insert(p2);
//        b.insert(p3);
//        for(KeyPointerPair key: b.keyPointerPairs){
//            System.out.println(key.key);
//        }
    }
}
