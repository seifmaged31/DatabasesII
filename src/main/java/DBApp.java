import java.util.Hashtable;
import java.util.Iterator;

public class DBApp implements DBAppInterface{
    public void init() {
        // this does whatever initialization you would like
        // or leave it empty if there is no code you want to
        // execute at application startup

    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        // following method creates one table only
        // strClusteringKeyColumn is the name of the column that will be the primary
        // key and the clustering column as well. The data type of that column will
        // be passed in htblColNameType
        // htblColNameValue will have the column name as key and the data
        // type as value
        // htblColNameMin and htblColNameMax for passing minimum and maximum values
        // for data in the column. Key is the name of the column


    }

    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        // following method creates one index – either multidimensional
        // or single dimension depending on the count of column names passed.

    }

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        // following method inserts one row only.
        // htblColNameValue must include a value for the primary key

    }

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        // following method updates one row only
        // htblColNameValue holds the key and new value
        // htblColNameValue will not include clustering key as column name
        // strClusteringKeyValue is the value to look for to find the rows to update.

    }

    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        // following method could be used to delete one or more rows.
        // htblColNameValue holds the key and value. This will be used in search
        // to identify which rows/tuples to delete.
        //htblColNameValue enteries are ANDED together

    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }
}
