package sparktemplate.datasets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by as on 21.05.2018.
 */
public interface ADataSet {
    /**
     * Abstrakcyjna metoda zwracajaca pobrane dane.
     *
     * @return zbior danych
     */
    public Dataset<Row> getDs();
}