import java.util.ArrayList;
public class Statement extends SQLTerm{

    ArrayList results;

    public Statement(String tableName,String column,String expression,Object value){
        super(tableName,column,expression,value);
        results = new ArrayList();
    }
    public String toString(){
        return this._strTableName + "   " + this._strColumnName + "   " + this._strOperator + "  " + this._objValue;
    }

}
