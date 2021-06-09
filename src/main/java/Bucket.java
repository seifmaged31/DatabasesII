import java.io.*;
import java.util.Collections;
import java.util.Properties;
import java.util.Vector;

public class Bucket implements Serializable {
    //String pageFilePath;
    Vector<KeyPointerPair> keyPointerPairs;
    Bucket next;
    int numOfKeys=0;
    public Bucket(KeyPointerPair keyPointerPair){
        keyPointerPairs = new Vector<>();
        keyPointerPairs.add(keyPointerPair);
        numOfKeys++;

    }

    public void insert(KeyPointerPair keyPointerPair){
        keyPointerPairs.add(keyPointerPair);
        Collections.sort(keyPointerPairs);
        numOfKeys++;
    }
    public boolean isFull() throws IOException {
        if(this.numOfKeys==getMaxKeys())
            return true;
        return false;
    }

    public boolean isEmpty(){
        if(this.numOfKeys==0)
            return true;
        return false;
    }
    public void addBucket(KeyPointerPair keyPointerPair){
        this.next= new Bucket(keyPointerPair);
        //serializeBucket(this.next);
    }
    public static Bucket deserializeBucket(String path){

        Bucket bucket = null;
        try{
            FileInputStream fileIn =
                    new FileInputStream(new File(path));
            ObjectInputStream in= new ObjectInputStream(fileIn);
            bucket = (Bucket) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bucket;
    }

    public void serializeBucket(String path){
        try{
            FileOutputStream fileOut =
                    new FileOutputStream(new File(path));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        }
        catch(IOException i){

        }
    }
    public int getMaxKeys() throws FileNotFoundException, IOException {
        File configFile = new File("src/main/resources/DBApp.config");
        int maxRows=0;

        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);


            maxRows = Integer.parseInt(props.getProperty("MaximumKeysCountinIndexBucket"));

            reader.close();
        } catch (FileNotFoundException ex) {
            // file does not exist
        } catch (IOException ex) {
            // I/O error
        }
        return maxRows;

    }
        public boolean updateKeyPointerPairPath(KeyPointerPair keyPointerPair,String newPath,int newRowNum){
            for(KeyPointerPair key:keyPointerPairs){
                if(key.compareTo(keyPointerPair)==0 && key.pointer.equals(keyPointerPair.pointer) && key.rowNum==keyPointerPair.rowNum){
                    key.rowNum=newRowNum;
                    key.pointer=newPath;
                    return true;
                }
            }
            return false;
        }

}
