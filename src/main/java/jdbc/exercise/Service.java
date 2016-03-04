package jdbc.exercise;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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

	public void printTable(String tabName) throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		try (Connection con = DriverManager
				.getConnection("jdbc:mysql://localhost/starter_kit?" + "user=" + user + "&password=" + password)) {

			try (PreparedStatement ps = con.prepareStatement("select * from " + tabName)) {
//				ps.setString(1, tabName);
				
				try (ResultSet rs = ps.executeQuery()) {
					printColumnHeader(rs);
					printData(rs);
				}
			}
		}
	}

	private void printColumnHeader(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		for (int j = 1; j <= rsmd.getColumnCount(); j++) {
			System.out.print(rsmd.getColumnName(j));
			System.out.print(" ");
			System.out.println(rsmd.getColumnTypeName(j));
		}		
	}

	private void printData(ResultSet rs) throws SQLException {
		while (rs.next()) {
			System.out.print(rs.getInt(1));
			System.out.print(" ");
			System.out.print(rs.getString(2));
			System.out.println();
		}		
	}
}
