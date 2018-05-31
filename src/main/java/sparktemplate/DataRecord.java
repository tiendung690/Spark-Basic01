package sparktemplate;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Klasa reprezentuje wartosci atrybutow w jednym rekordzie danych
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */

public class DataRecord {

    private StructType structType;
    private Row row;

    /**
     * Konstruktor inicjalizujacy obiekt DataRecord. Tworzy pojedynczy obiekt na podstawie skladowych struktury danych sparka - Dataset.
     * W ten sposob mozna przechowywac pojedyncze wiersze z Dataset bez potrzeby tworzenia pojedynczego Dataset z jednym wierszem i uruchamiania mechanizmow sparka.
     *
     * @param row dane
     * @param structType struktura
     */
    public DataRecord(Row row, StructType structType) {
        //konstruktor przygotowuje wewnetrzna strukture danych
        this.structType = structType;
        this.row = row;
    }

    /**
     * Metoda zwracajaca strukture obiektu.
     *
     * @return struktura
     */
    public StructType getStructType() {
        return structType;
    }

    /**
     * Metoda zwracajaca dane obiektu.
     *
     * @return dane, wartosci
     */
    public Row getRow() {
        return row;
    }

    /**
     * Metoda zwracajaca atrybutow w rekordzie (liczba kolumn w danych)
     *
     * @return liczba atrybutow
     */
    public int getNoAttr() {
        return row.size();
    }

    /**
     * Metoda zwracajaca wartosc atrybutu w rekordzie o podanym numerze
     *
     * @param attributeIndex Numer atrybutu.
     * @return Wartosc atrybutu.
     */
    public String getAttributeValue(int attributeIndex) {
        return row.get(attributeIndex).toString();
    }

    /**
     * Metoda ustawiajaca wartosc atrybutu w rekordzie o podanym numerze
     *
     * @param attributeIndex numer atrybutu
     * @param value          Ustawiana wartosc.
     *                       zgodnosci typu wartosci).
     */
    public void setAttributeValue(int attributeIndex, String value) {

        // 1 wersja prosta
        Object[] obj = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            if (i == attributeIndex) {
                obj[i] = value;
            } else {
                obj[i] = row.get(i);
            }
        }

        // 2 wersja
        //Object[] obj = (Object[]) row.toSeq().toArray( scala.reflect.ClassTag$.MODULE$.apply(Object.class));
        //obj2[attributeIndex]=value;

        this.row = RowFactory.create(obj);
    }

    /**
     * Metoda zwracajaca typ danych atrybutu.
     *
     * @param index numer atrybutu
     * @return typ danych w formacie spark.sql.types.DataType
     */
    public String getAttrType(int index) {
        return structType.fields()[index].dataType().toString();
    }

}
