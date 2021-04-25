import java.io.Serializable;

public class PageInfo implements Serializable {
     //private int pageID;
     private Row max;
     private Row min;
     private int numOfRows=0;

        public PageInfo(Row max,Row min){

            this.max=max;
            this.min=min;
            numOfRows=0;

        }

        /*public void insert(Row row){
            this.numOfRows++;
            if(this.max.compareTo(row)<0)
                this.max=row;
            else if(this.min.compareTo(row)>0)
                this.min=row;

        }*/

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
}
