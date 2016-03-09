package jdbc.exercise;

import java.io.BufferedReader;
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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.mysql.fabric.xmlrpc.base.Array;

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

			try (PreparedStatement ps = con.prepareStatement("select * from " + tabName + " limit ? offset ?")) {
				if (toRowIdx != -1) {
					ps.setInt(1, toRowIdx);
				}
				else {
					ps.setLong(1, Long.MAX_VALUE);
				}
				ps.setInt(2, fromRowIdx - 1);

				try (ResultSet rs = ps.executeQuery()) {
					TextTable textTable = new TextTable(getColumnHeader(rs), getTableData(rs));
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

			String tableName = file.getName().split("\\-|\\.")[1].toLowerCase();
			String headers = new BufferedReader(new FileReader(file)).readLine();
			
			CSVParser csvFileParser = new CSVParser(new FileReader(file), CSVFormat.DEFAULT.withHeader(headers.split(",")));
			try (PreparedStatement ps = getStatementPattern(con, tableName, headers)) {
				con.setAutoCommit(false);
				
				List<String> headersList = Arrays.asList(headers.split(","));
				List<CSVRecord> csvRecords = csvFileParser.getRecords();
				
				for (int row = 1; row < csvRecords.size(); row++) {
					for (int column = 1; column <= headersList.size(); column++) {
						ps.setObject(column, csvRecords.get(row).get(headersList.get(column-1)));
					}
					System.out.println(ps.toString());
					ps.addBatch();
				}
				ps.executeBatch();
			}
			
			con.commit();
			csvFileParser.close();
		}
	}

	private PreparedStatement getStatementPattern(Connection con, String tableName, String headers) throws SQLException {
		String statement = "INSERT INTO " + tableName + " (" + headers + ") VALUES(?";
		for (int index = 1; index < headers.split(",").length; index++) {
			statement += ", ?";
		}
		statement += ");";
		return con.prepareStatement(statement);
	}
}
