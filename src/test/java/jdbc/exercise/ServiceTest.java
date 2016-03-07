package jdbc.exercise;

import java.io.File;
import java.io.IOException;
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
	public void printFullTable() throws ClassNotFoundException, SQLException {
		service.printTable("division", 1, -1);
		service.printTable("employee", 1, -1);
		service.printTable("project", 1, -1);
		service.printTable("employee2project", 1, -1);
	}
	
	@Test
	public void printFirstNElementsFromTable() throws ClassNotFoundException, SQLException {
		service.printTable("division", 1, 2);
		service.printTable("employee", 1, 5);
		service.printTable("project", 1, 2);
		service.printTable("employee2project", 1, 10);
	}
	
	@Test
	public void printNElementsFromTableFromNumberX() throws ClassNotFoundException, SQLException {
		service.printTable("division", 2, 2);
		service.printTable("employee", 3, 5);
		service.printTable("project", 3, 2);
		service.printTable("employee2project", 7, 10);
	}

	@Test
	public void printMoreThanElementsInTable() throws ClassNotFoundException, SQLException {
		service.printTable("division", 1, 10);
		service.printTable("employee", 1, 10);
		service.printTable("project", 1, 10);
		service.printTable("employee2project", 7, 25);
	}
	
	@Test
	public void insertDataToTableFromCSV() throws ClassNotFoundException, SQLException, IOException {
		service.storeData(new File("src\\test\\resources\\insert-MyTab.csv"));
	}
}
