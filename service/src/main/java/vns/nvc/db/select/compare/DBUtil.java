package vns.nvc.db.select.compare;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class DBUtil {

    static class DriverShim implements Driver {
        private final Driver driver;
        DriverShim(Driver d) {
            this.driver = d;
        }
        public boolean acceptsURL(String u) throws SQLException {
            return this.driver.acceptsURL(u);
        }
        public Connection connect(String u, Properties p) throws SQLException {
            return this.driver.connect(u, p);
        }
        public int getMajorVersion() {
            return this.driver.getMajorVersion();
        }
        public int getMinorVersion() {
            return this.driver.getMinorVersion();
        }
        public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
            return this.driver.getPropertyInfo(u, p);
        }
        public boolean jdbcCompliant() {
            return this.driver.jdbcCompliant();
        }

        @Override
        public Logger getParentLogger() {
            return null;
        }
    }

    private static final String DB_DRIVER_PATH="driver.file.path";
    private static final String DB_DRIVER_CLASS="driver.class.name";
    private static final String DB_USERNAME="db.username";
    private static final String DB_PASSWORD="db.password";
    private static final String DB_URL ="db.url";

    private static Connection connection = null;
    private static Properties properties = null;

    static {
        try {
            properties = new Properties();
            properties.load(new FileInputStream("D:\\WORKING\\NVC_PJ\\db_select_compare/db.properties"));

            URL u = new URL(String.format("jar:file:%s!/", properties.getProperty(DB_DRIVER_PATH)));
            String classname = properties.getProperty(DB_DRIVER_CLASS);
            URLClassLoader ucl = new URLClassLoader(new URL[] { u });
            Driver d = (Driver)Class.forName(classname, true, ucl).newInstance();
            DriverManager.registerDriver(new DriverShim(d));
        } catch (ClassNotFoundException | IOException | SQLException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        try {
            connection = DriverManager.getConnection(properties.getProperty(DB_URL),properties.getProperty(DB_USERNAME) , properties.getProperty(DB_PASSWORD) );
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
