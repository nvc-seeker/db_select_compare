package vns.nvc.db.select.compare;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) {
        Connection connection = DBUtil.getConnection();
        assert connection != null;

        Datatable dataSelect = new Datatable();
        System.out.println(new Date());
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM students");
//            CSVWriter csvWriter = new CSVWriter(new FileWriter("D:/WORKING/NVC_PJ/db_select_compare/db_log.csv"));
//            csvWriter.writeAll(rs, true);
            dataSelect.loadQueryResult(rs);
            System.out.println(dataSelect.getRowCount());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Datatable dataLog = new Datatable();
        System.out.println(new Date());
        try {
            CSVReader reader = new CSVReader(new FileReader("D:/WORKING/NVC_PJ/db_select_compare/db_log.csv"));
            dataLog.loadCSVReader(reader);
            System.out.println(dataLog.getRowCount());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(new Date());

        Datatable[] datatables = filterDatatablesWithStatus(List.of("id"), dataLog, dataSelect);

        System.out.println(datatables[0].getRowCount());
        System.out.println(datatables[1].getRowCount());
        System.out.println(datatables[2].getRowCount());
        System.out.println(new Date());
    }

    public static Datatable[] filterDatatablesWithStatus(List<String> columnKeys, Datatable logData, Datatable data) {
        Datatable[] datatables = new Datatable[3];
        datatables[0] = data; // INSERT
        datatables[1] = new Datatable(); // UPDATE
        datatables[2] = new Datatable(); // DELETE

        //datatables[0].addAllColumn(logData.getColumns());
        datatables[1].addAllColumn(logData.getColumns());
        datatables[2].addAllColumn(logData.getColumns());

        List<Object> rowDataLog;
        List<Object> rowData;

        Map<String, List<Object>> dataMapper = new HashMap<>();
        String key;

        for (int i=0; i<data.getRowCount(); i++) {
            key = "";
            for (String columnKey : columnKeys) {
                 key = data.getCell(columnKey, i) + "%_%";
            }
            dataMapper.put(key, data.getRow(i));
        }

        loopDataLog:
        for (int i=0; i<logData.getRowCount(); i++) {
            key = "";
            for (String columnKey : columnKeys) {
                key = logData.getCell(columnKey, i) + "%_%";
            }

            rowDataLog = logData.getRow(i);
            rowData = dataMapper.get(key);

            if (rowData != null) {
                // Remove data UPDATED <=> rest data INSERTED
                data.removeRow(rowData);
                for (int ii=0; ii<rowDataLog.size(); ii++) {
                    if (!String.valueOf(rowDataLog.get(ii)).equals(String.valueOf(rowData.get(ii)))) {
                        // Add data UPDATED
                        datatables[1].addRow(rowData);
                        continue loopDataLog;
                    }
                }
            } else {
                // Add data DELETED
                datatables[2].addRow(rowDataLog);
            }
        }

//        for (int y=0; y<data.getRowCount(); y++) {
//            rowData = data.getRow(y);
//            datatables[0].addRow(rowData);
//        }

        return datatables;
    }
}
