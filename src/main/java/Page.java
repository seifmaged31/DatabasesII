import java.io.*;
import java.util.Vector;
import java.util.*;

public class Page implements Serializable, Comparable {
    Vector<Row> rows;


    public Page(Row row) {
        rows=new Vector<>();
        rows.add(row);

    }

    public void insert(Row row,int indexOfRow){

        rows.insertElementAt(row,indexOfRow);
        Collections.sort(rows);

    }

    public void delete(Row row){

        rows.remove(row);
        Collections.sort(rows);

    }
    @Override
    public int compareTo(Object o) {
        Page p=(Page) o;
        return ((Row) rows.firstElement()).compareTo( p.rows.firstElement());


    }

    public static void main(String [] args) throws IOException,ClassNotFoundException {

    }



}
