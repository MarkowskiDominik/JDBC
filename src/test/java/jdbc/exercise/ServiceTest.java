package jdbc.exercise;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

public class ServiceTest {

	private static final String USER = "root";
	private static final String PASSWORD = "rhQQ2yxrkt92#cgm";
	private Service service;

	@Before
	public void setUp() {
		service = new Service(USER, PASSWORD);		
	}
	
	@Test
	public void printTable() throws ClassNotFoundException, SQLException {
		service.printTable("employee");
	}
}
