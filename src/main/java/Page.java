import java.io.*;
import java.util.Properties;
import java.util.Vector;
import java.util.*;

public class Page implements Serializable, Comparable {
    //Vector<Object> keysValues;
    Vector<Row> rows;


    public Page(Row row) {
        rows=new Vector<>();
        //overflowPages= new Hashtable<>();
        rows.add(row);
    }

    public void insert(Row row){

        rows.add(row);
        Collections.sort(rows);

    }






   /* public boolean isFull() throws IOException {
        if(numOfRows==getMaxRows())
            return true;
        return false;
    }

    public boolean isEmpty(){
        if(numOfRows==0)
            return true;
        return false;
    }*/

   /* public static int getMaxRows() throws  FileNotFoundException,IOException{
        File configFile = new File("src/main/resources/DBApp.config");

        int maxRows=0;

        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);


            maxRows = Integer.parseInt(props.getProperty("MaximumRowsCountinPage"));

            reader.close();
        } catch (FileNotFoundException ex) {
            // file does not exist
        } catch (IOException ex) {
            // I/O error
        }
        return maxRows;

    }*/

    public static void main(String [] args) throws IOException,ClassNotFoundException {

       Hashtable htblColNameType = new Hashtable( );
        
        Date d1 = new Date(1998, 11, 17);

        htblColNameType.put("PageInfo0", "Employee_1.class");
        htblColNameType.put("date", d1);
        htblColNameType.put("name", "salma");
        htblColNameType.put("gpa", 0.0);

        

        Row a=new Row("date",htblColNameType);

        Page p = new Page(a);
        Row res = null;
        try {
       /*    FileOutputStream fileOut =
                    new FileOutputStream(new File("src/main/resources/salma.class"));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(a);
            out.close();
            fileOut.close();*/

            FileOutputStream fileOut2 =
                    new FileOutputStream(new File("src/main/resources/donia.class"));
            ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
            out2.writeObject(p); //safla
            out2.close();
            fileOut2.close();
            System.out.println("Serialized data is saved in /src/main/resource/trial.class");
            FileInputStream fileIn =
                    new FileInputStream(new File("src/main/resources/donia.class"));
            ObjectInputStream in= new ObjectInputStream(fileIn);
            res = a;



        } catch (IOException i) {
            i.printStackTrace();
        }


        System.out.println(res);
    }


    @Override
    public int compareTo(Object o) {
        Page p=(Page) o;
//        if(this.rows.firstElement().compareTo(p.rows.firstElement())>0) {
//            return 1;
//        }
//        else if(this.rows.firstElement().compareTo(p.rows.firstElement())>0){
//            return -1;
//            }
//        else
//            return 0;
        return ((Row) rows.firstElement()).compareTo( p.rows.firstElement());


    }
}
