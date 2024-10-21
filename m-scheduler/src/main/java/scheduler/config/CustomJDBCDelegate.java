package scheduler.config;

import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CustomJDBCDelegate extends StdJDBCDelegate {

	@Override
	protected Object getObjectFromBlob(ResultSet rs, String colName)
			throws ClassNotFoundException, IOException, SQLException {
		byte[] bytes = rs.getBytes(colName);
		Object map = null;
		ByteArrayInputStream bais = null;
		ObjectInputStream ois = null;
		try {
			bais = new ByteArrayInputStream(bytes);
			ois = new ObjectInputStream(bais);
			map = ois.readObject();
		} catch (EOFException ex1) {
			bais.close();
		} catch (IOException e) {
			// Error in de-serialization
			e.printStackTrace();
		}

		return map;
	}

	//@Override
	protected Object getObjectFromBlob2(ResultSet rs, String colName)
			throws ClassNotFoundException, IOException, SQLException {
		int bytesRead = 0;

		Object map = null;
		ByteArrayInputStream bais = null;
		ObjectInputStream ois = null;
		
		InputStream in = rs.getBinaryStream(colName);
		byte[] buff = new byte[8192];

		try {
			ByteArrayOutputStream bao = new ByteArrayOutputStream();

			while ((bytesRead = in.read(buff)) != -1) {
				bao.write(buff, 0, bytesRead);
			}

			byte[] data = bao.toByteArray();
			bais = new ByteArrayInputStream(data);

			ois = new ObjectInputStream(bais);
			map = ois.readObject();
		} catch (EOFException ex1) {
			bais.close();
		} catch (IOException e) {
			// Error in de-serialization
			e.printStackTrace();
		}

		return map;
	}

	@Override
	protected Object getJobDataFromBlob(ResultSet rs, String colName)
			throws ClassNotFoundException, IOException, SQLException {
		return getObjectFromBlob(rs, colName);
	}
}
