import java.util.Hashtable;
import java.util.Iterator;

public class DBApp implements DBAppInterface{
    public void init() {

    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

    }

    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

    }

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }
}
