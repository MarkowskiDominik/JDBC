package jdbc.exercise;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import dnl.utils.text.table.TextTable;

public class Service {

	private final String user;
	private final String password;

	public Service(String user, String password) {
		this.user = user;
		this.password = password;
	}

	public Service() {
		this("root", "rhQQ2yxrkt92#cgm");
	}

	public void printTable(String tabName, int fromRowIdx, int toRowIdx) throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		try (Connection con = DriverManager
				.getConnection("jdbc:mysql://localhost/starter_kit?" + "user=" + user + "&password=" + password)) {

			TextTable textTable;
			try (PreparedStatement ps = con.prepareStatement("select * from " + tabName + " limit ? offset ?")) {
				if (toRowIdx != -1) {
					ps.setInt(1, toRowIdx);
				}
				else {
					ps.setLong(1, Long.MAX_VALUE);
				}
				ps.setInt(2, fromRowIdx - 1);

				try (ResultSet rs = ps.executeQuery()) {
					textTable = new TextTable(getColumnHeader(rs), getTableData(rs));
					textTable.printTable();
				}
			}
		}
	}

	private String[] getColumnHeader(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		String[] columnHeader = new String[rsmd.getColumnCount()]; 
		for (int columnIndex = 1; columnIndex <= rsmd.getColumnCount(); columnIndex++) {
			columnHeader[columnIndex-1] = rsmd.getColumnName(columnIndex);
		}
		return columnHeader;
	}

	private Object[][] getTableData(ResultSet rs) throws SQLException {
		List<List<Object>> rowList = new ArrayList<List<Object>>();
		int rowIndex = 0;
		int columnCount = rs.getMetaData().getColumnCount();
		while (rs.next()) {
			List<Object> row = new ArrayList<Object>(columnCount);
			for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
				row.add(rs.getObject(columnIndex));
			}
			rowList.add(row);
			rowIndex++;
		}
		
		Object[][] tableData = new Object[rowList.size()][columnCount];
		rowIndex = 0;
		for (List<Object> row: rowList) {
			tableData[rowIndex] = row.toArray();
			rowIndex++;
		}
		return tableData;
	}
	
	public void storeData(File file) throws ClassNotFoundException, SQLException, IOException {
		Class.forName("com.mysql.jdbc.Driver");
		try (Connection con = DriverManager
				.getConnection("jdbc:mysql://localhost/starter_kit?" + "user=" + user + "&password=" + password)) {

			FileReader fileReader = new FileReader(file);
			CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader("ID", "AGE", "NAME");
			CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);
	        
			try (PreparedStatement ps = con.prepareStatement("INSERT INTO " + file.getName() + " (ID, AGE, NAME) VALUES(?, ?, ?)")) {
				con.setAutoCommit(false);
				for (CSVRecord csvRecord : csvFileParser.getRecords()) {
					ps.setInt(1, Integer.parseInt(csvRecord.get("ID")));
					ps.setString(2, csvRecord.get("AGE"));
					ps.setString(3, csvRecord.get("NAME"));
					ps.addBatch();
				}
				ps.executeBatch();
			}
			csvFileParser.close();
		}
	}
}
