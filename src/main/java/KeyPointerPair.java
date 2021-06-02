import java.util.Vector;

public class KeyPointerPair implements Comparable{

    Vector<String> key;
    String pointer;

    public KeyPointerPair(Vector<String> key,String pointer){
        this.key=key;
        this.pointer=pointer;
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
        System.out.println("abc".compareTo("fuck"));
    }
}
