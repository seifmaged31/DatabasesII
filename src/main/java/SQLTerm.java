import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

public class SQLTerm {
     String _strTableName;
     String _strColumnName;
     String _strOperator;
     Object _objValue;
     public SQLTerm(){

     }

     public SQLTerm(String _strTableName,String _strColumnName, String _strOperator,Object _objValue){
         this._strTableName= _strTableName;
         this._strColumnName= _strColumnName;
         this._strOperator=_strOperator;
         this._objValue=_objValue;

     }


    public static void main(String[] args) {

    }

//    public void set_strOperator(String _strOperator) throws DBAppException {
//       String[] operators = {">", ">=", "<", "<=", "!=", "="};
//       List<String> valid = Arrays.asList(operators);
//       if(valid.contains(_strOperator))
//           this._strOperator = _strOperator;
//       else
//           throw new DBAppException("Invalid Operator");
//    }

}
