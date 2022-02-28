package vns.nvc.db.selectcompare;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Datatable {

    private final String name;
    private final List<String> columns = new ArrayList<>();
    private final List<List<Object>> rows = new ArrayList<>();

    public Datatable() {
        this(null);
    }
    public Datatable(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void addColumn(String column) {
        columns.remove(column);
        columns.add(column);
    }
    public void addAllColumn(String[] columns) {
        this.columns.clear();
        this.columns.addAll(List.of(columns));
    }
    public void addAllColumn(List<String> columns) {
        this.columns.clear();
        this.columns.addAll(columns);
    }

    public List<String> getColumns() {
        return columns;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public void addRow(Object[] row) {
        rows.add(List.of(row));
    }
    public void addRow(List<Object> row) {
        rows.add(row);
    }

    public void removeRow(int index) {
        rows.remove(index);
    }
    public void removeRow(List<Object> row) {
        rows.remove(row);
    }

    public List<Object> getRow(int index) {
        return rows.get(index);
    }

    public int getRowCount() {
        return rows.size();
    }

    public Object getCell(int columnIndex, int rowIndex) {
        List<Object> row = getRow(rowIndex);
        return row.get(columnIndex);
    }
    public Object getCell(String columnName, int rowIndex) {
        int columnIndex = columns.indexOf(columnName);
        List<Object> row = getRow(rowIndex);
        return row.get(columnIndex);
    }

    public void loadQueryResult(ResultSet rs) {
        try {
            List<Object> row;
            ResultSetMetaData rsMetaData = rs.getMetaData();
            int columnCount = rsMetaData.getColumnCount();
            for (int i=1; i<=columnCount; i++) {
                addColumn(rsMetaData.getColumnName(i));
            }
            while (rs.next()) {
                row = new ArrayList<>();
                for (int i=1; i<=columnCount; i++) {
                    row.add(rs.getString(i));
                }
                addRow(row);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void loadCSVReader(CSVReader reader) {
        try {
            int i=0;
            String[] lineInArray;
            while ((lineInArray = reader.readNext()) != null) {
                if (i==0) {
                    addAllColumn(lineInArray);
                } else {
                    addRow(lineInArray);
                }
                i++;
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }
}
