package sparktemplate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Klasa <tt>DataRecord</tt> reprezentuje wartosci atrybutow w jednym rekordzie danych
 * <p>
 * To nie jest pełna implementacja, ale wymaga uzupełnienia
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */

public class DataRecord {

    //Tutaj pozostaje do rozwiazania problem typow poszczegolnych atrybutow. Czy to będzie tutaj istotne?
    //Wartosci mozna reprezentowac w postaci kolekcji String-ow, ale w ten sposob ni ebedzie informacji o typie wartosci, a czasem to jest potrzebne.

    // Dataset sklada sie z StructType oraz Row
    private StructType structType;
    private Row row;

    public StructType getStructType() {
        return structType;
    }


    public Row getRow() {
        return row;
    }


    public DataRecord(Row row, StructType structType) {
        //konstruktor przygotowuje wewnetrzna strukture danych
        this.structType = structType;
        this.row = row;
    }


    //Zwraca liczbe wartosci w rekordzie (liczba kolumn w danych)
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
        //return "val";
    }

    /**
     * Abstrakcyjna metoda ustawiajaca wartosc atrybutu w rekordzie o podanym
     * numerze
     *
     * @param attributeIndex Numer atrybutu.
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


    //Ponisza metoda jest problematyczna (moze nie beda potrzebne)


    public String getAttrType(int index) {
        // pobiera typ zmapowany przez sparka DataType
        return structType.fields()[index].dataType().toString();

    }

}
