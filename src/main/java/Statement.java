import java.util.ArrayList;
public class Statement extends SQLTerm{

    ArrayList results;

    public Statement(String tableName,String column,String expression,Object value){
        super(tableName,column,expression,value);
        results = new ArrayList();
    }
    // eh el khara eli ehna kattabnah dah

}
