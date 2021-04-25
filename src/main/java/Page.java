import java.io.*;
import java.util.Properties;
import java.util.Vector;

import java.util.*;

public class Page implements Serializable {
    Row max;
    Row min;
    int numOfRows=0;
    Vector<Row> rows;


    public Page(Row row) {
        rows=new Vector<>();
        rows.add(row);
        numOfRows++;
        min=row;
        max=row;

    }

    public void insert(Row row){
        rows.add(row);
        numOfRows++;
        if(((Row)min).compareTo(row)>0){
            min=row;
        }
        else if(((Row)max).compareTo(row)>0)
            {
                max=row;
            }
        Collections.sort(rows);

    }






    public boolean isFull() throws IOException {
        if(numOfRows==getMaxRows())
            return true;
        return false;
    }

    public boolean isEmpty(){
        if(numOfRows==0)
            return true;
        return false;
    }

    public static int getMaxRows() throws  FileNotFoundException,IOException{
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

    }

    public static void main(String [] args) throws IOException,ClassNotFoundException {

     /*  int res=0;
        try {

            Hashtable htblColNameType = new Hashtable( );
        Date d = new Date(1995, 11, 17);
        Date d1 = new Date(1998, 11, 17);
        htblColNameType.put("id", 1);
        htblColNameType.put("date", d1);
        htblColNameType.put("name", "salma");
        htblColNameType.put("gpa", 0.0);

        Row a=new Row("date",htblColNameType);




        Vector<Row> vecRow=new Vector<>();

        /*xx.add(a);
        System.out.println(a);
        xx.add(b);
        System.out.println(b);
        Collections.sort(xx);
        System.out.println(xx.toString());


            /*FileOutputStream fileOut = new FileOutputStream(new File("src/main/resources/trial.class"));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(4);
            out.writeObject(5);
            out.close();
            fileOut.close();
            FileOutputStream fileOut2 = new FileOutputStream(new File("src/main/resources/trial.class"));
            ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
            out2.writeObject(69); //safla
            out2.close();
            fileOut2.close();
            System.out.println("Serialized data is saved in /src/main/resource/trial.class");
            FileInputStream fileIn = new FileInputStream(new File("src/main/resources/trial.class"));
            ObjectInputStream in= new ObjectInputStream(fileIn);
            res = (int)in.readObject();

        /*} catch (IOException i) {
            i.printStackTrace();
        }
        catch(ClassNotFoundException c){
            c.printStackTrace();
        }
        System.out.println(res);
        */
    //}


    }
}
