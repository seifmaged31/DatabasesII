import java.io.Serializable;

public class Table implements Serializable {
    String name;
    int numberOfPages;
    public Table (String name){
        this.name=name;
    }
}
