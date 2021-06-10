import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class KeyPointerPair implements Comparable, Serializable {

    Vector<String> key;
    String pointer;
    int rowNum;

    public KeyPointerPair(Vector<String> key, String pointer, int rowNum) {
        this.key = key;
        this.pointer = pointer;
        this.rowNum = rowNum;
    }

    @Override
    public int compareTo(Object o) {
        KeyPointerPair keyPointerPair = (KeyPointerPair) o;
        for (int i = 0; i < keyPointerPair.key.size(); i++) {
            Object current = getValue(this.key.get(i));
            Object compared = getValue(keyPointerPair.key.get(i));
            if (Row.compareObject(current, compared) == 0) {
                continue;
            } else {
                return Row.compareObject(current, compared);
            }

        }
        return 0;

    }

    public static Object getValue(String string) {
        try {
            return (Integer) (Integer.parseInt(string));
        } catch (Exception e) {

        }
        try {
            return (Double) (Double.parseDouble(string));
        } catch (Exception e) {

        }
        try {
            return (Date) (new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH).parse(string));
        } catch (Exception e) {

        }
        return (String) string;

    }

    public void addRecord(Hashtable<String, Object> columnNameStatement) { //[1,2]

        ArrayList<String> keys = new ArrayList<String>(columnNameStatement.keySet());// indices: [2]
        Object rowValue = null;
        for (String key : keys) {
            String value = this.key.get(keys.indexOf(key));
            rowValue = getValue(value);
            Object comparedValue = ((Statement) columnNameStatement.get(key))._objValue;
            Page page = Table.deserializePage(pointer);
            switch (((Statement) columnNameStatement.get(key))._strOperator) {
                case "=":
                    if (Row.compareObject(rowValue, comparedValue) == 0)
                        ((Statement) columnNameStatement.get(key)).results.add(page.rows.get(rowNum));
                    break;

                case ">":
                    if (Row.compareObject(rowValue, comparedValue) > 0)
                        ((Statement) columnNameStatement.get(key)).results.add(page.rows.get(rowNum));
                    break;

                case "<":
                    if (Row.compareObject(rowValue, comparedValue) < 0)
                        ((Statement) columnNameStatement.get(key)).results.add(page.rows.get(rowNum));
                    break;

                case ">=":
                    if (Row.compareObject(rowValue, comparedValue) >= 0)
                        ((Statement) columnNameStatement.get(key)).results.add(page.rows.get(rowNum));
                    break;

                case "<=":
                    if (Row.compareObject(rowValue, comparedValue) <= 0)
                        ((Statement) columnNameStatement.get(key)).results.add(page.rows.get(rowNum));
                    break;

                default:
                    if (Row.compareObject(rowValue, comparedValue) != 0)
                        ((Statement) columnNameStatement.get(key)).results.add(page.rows.get(rowNum));
                    break;

            }
        }

    }

    public Row getRow(String path, int rowNum) {
        Page page = Table.deserializePage(path);
        return page.rows.get(rowNum);
    }
}
