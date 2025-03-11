import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CarsDBRepository implements CarRepository {

    private JdbcUtils dbUtils;

    private static final Logger logger= LogManager.getLogger();

    public CarsDBRepository(Properties props) {
        logger.info("Initializing CarsDBRepository with properties: {} ",props);
        dbUtils = new JdbcUtils(props);
    }

    @Override
    public List<Car> findByManufacturer(String manufacturerN) {
        logger.traceEntry();
        Connection conn = dbUtils.getConnection();
        List<Car> masini = new ArrayList<>();
        try(PreparedStatement preStatement = conn.prepareStatement("select * from Masini where manufacturer=?"))
        {
            preStatement.setString(1, manufacturerN);
            try(ResultSet result = preStatement.executeQuery())
            {
                while(result.next())
                {
                    int id = result.getInt("id");
                    String manufacturer = result.getString("manufacturer");
                    String model = result.getString("model");
                    int year = result.getInt("year");
                    Car masina = new Car(manufacturer, model, year);
                    masina.setId(id);
                    masini.add(masina);
                }
            }
        }
        catch(SQLException ex)
        {
            logger.error(ex);
            System.err.println("Error - DataBase " + ex);
        }
        logger.traceExit(masini);
        return masini;
    }

    @Override
    public List<Car> findBetweenYears(int min, int max) {
        logger.traceEntry();
        Connection conn = dbUtils.getConnection();
        List<Car> masini = new ArrayList<>();
        try(PreparedStatement preStatement = conn.prepareStatement("select * from Masini where year between ? and ?"))
        {
            preStatement.setInt(1, min);
            preStatement.setInt(2, max);
            try(ResultSet result = preStatement.executeQuery())
            {
                while(result.next())
                {
                    int id = result.getInt("id");
                    String manufacturer = result.getString("manufacturer");
                    String model = result.getString("model");
                    int year = result.getInt("year");
                    Car masina = new Car(manufacturer, model, year);
                    masina.setId(id);
                    masini.add(masina);
                }
            }
        }
        catch(SQLException ex) {
            logger.error(ex);
            System.err.println("Error - DataBase " + ex);
        }
        logger.traceExit(masini);
        return masini;
    }


    @Override
    public void add(Car elem) {
        logger.traceEntry("saving task {}", elem);
        Connection conn = dbUtils.getConnection();
        try(PreparedStatement preStatement = conn.prepareStatement("insert into Masini (manufacturer, model, year) values (?, ?, ?)")){
            preStatement.setString(1, elem.getManufacturer());
            preStatement.setString(2, elem.getModel());
            preStatement.setInt(3, elem.getYear());
            int result = preStatement.executeUpdate();
            logger.trace("Saved {} instances", result);
        }
        catch(SQLException ex) {
            logger.error(ex);
            System.err.println("Error - DataBase " + ex);
        }
        logger.traceExit();
    }

    @Override
    public void update(Integer integer, Car elem) {
        logger.traceEntry("updating {}", elem);
        Connection conn = dbUtils.getConnection();
        try(PreparedStatement preStatement = conn.prepareStatement("update Masini set manufacturer=?, model=?, year=? where id=?")){
            preStatement.setString(1, elem.getManufacturer());
            preStatement.setString(2, elem.getModel());
            preStatement.setInt(3, elem.getYear());
            preStatement.setInt(4, integer);
            int result = preStatement.executeUpdate();
            logger.trace("Updated {} instances", result);
        }
        catch(SQLException ex) {
            logger.error(ex);
            System.err.println("Error - DataBase " + ex);
        }
        logger.traceExit();
    }

    @Override
    public Iterable<Car> findAll() {
        logger.traceEntry();
        Connection conn = dbUtils.getConnection();
        List<Car> masini = new ArrayList<>();
        try(PreparedStatement preStatement = conn.prepareStatement("select * from Masini"))
        {
            try(ResultSet result = preStatement.executeQuery())
            {
                while(result.next())
                {
                    int id = result.getInt("id");
                    String manufacturer = result.getString("manufacturer");
                    String model = result.getString("model");
                    int year = result.getInt("year");
                    Car masina = new Car(manufacturer, model, year);
                    masina.setId(id);
                    masini.add(masina);
                }
            }
        }
        catch(SQLException ex) {
            logger.error(ex);
            System.err.println("Error - DataBase " + ex);
        }
        logger.traceExit(masini);
        return masini;
    }
}