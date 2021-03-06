import java.io.*;
import java.util.Hashtable;
import java.util.Properties;

public class PageInfo implements Serializable, Comparable {

     private Row max;
     private Row min;
     private int pageNum;
     private int numOfRows=0;


    public PageInfo(Row row){

            this.max=row;
            this.min=row;
            numOfRows = 1;

        }



    public Row getMax() {
        return max;
    }

    public void setMax(Row max) {
        this.max = max;
    }

    public Row getMin() {
        return min;
    }

    public void setMin(Row min) {
        this.min = min;
    }

    public int getNumOfRows() {
        return numOfRows;
    }

    public void setNumOfRows(int numOfRows) {
        this.numOfRows = numOfRows;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }

    public boolean isFull() throws IOException {
        if(this.numOfRows==getMaxRows())
            return true;
        return false;
    }

    public boolean isEmpty(){
        if(this.numOfRows==0)
            return true;
        return false;
    }

    public static int getMaxRows() throws FileNotFoundException, IOException {
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

    @Override
    public int compareTo(Object o) {
        PageInfo p = (PageInfo) o;
        return ((Row) this.getMin()).compareTo((Row) p.getMin());
    }
}
