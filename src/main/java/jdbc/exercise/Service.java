package jdbc.exercise;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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
			try (PreparedStatement ps = con.prepareStatement("select * from " + tabName)) {
//				ps.setString(1, tabName);

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
		Object tableData[][] = new Object[10][rs.getMetaData().getColumnCount()];
		int rowIndex = 0;
		int columnCount = rs.getMetaData().getColumnCount();
		while (rs.next()) {
			for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
				tableData[rowIndex][columnIndex-1] = rs.getObject(columnIndex);
			}
			rowIndex++;
		}
		return tableData;
	}
}
