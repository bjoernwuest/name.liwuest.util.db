package name.liwuest.util.db;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.SoftReferenceObjectPool;

public class CDBConnection {
	private final static Logger LOG = Logger.getLogger(CDBConnection.class.getPackage().getName());
	private final static ObjectPool<PoolableConnection> m_ConnectionPool;
	
	
	static {
		Properties props = new Properties();
		System.out.println(Paths.get("db.conf").toAbsolutePath());
		try { props.load(new FileInputStream(Paths.get(System.getProperty("dbconfig")).toAbsolutePath().toFile())); }
		catch (Exception E1) {
			try { props.load(CDBConnection.class.getClassLoader().getResourceAsStream("db.conf")); }
			catch (Exception E2) {
				try { props.load(new FileInputStream(Paths.get("db.conf").toAbsolutePath().toFile())); }
				catch (Exception Ex) {
					LOG.log(Level.SEVERE, "Failed to load data base properties file 'db.conf'.", Ex);
					System.exit(-1);
				}
			}
		}
		
		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(String.format("jdbc:%1$s://%2$s:%3$s/%4$s?user=%5$s&password=%6$s%7$s", props.getProperty("DatabaseManagementSystem"), props.getProperty("DatabaseHostAddress"), props.getProperty("DatabaseHostPort"), props.getProperty("DatabaseSchemaName"), props.getProperty("DatabaseUsername"), props.getProperty("DatabasePassword"), props.getProperty("DatabaseAdditionalConnectionParameters")), props);
		PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
		m_ConnectionPool = new SoftReferenceObjectPool<>(poolableConnectionFactory);
		poolableConnectionFactory.setPool(m_ConnectionPool);
	}
	
	
	/** <p>Returns a read-only connection to the data base.</p>
	 * 
	 * <p>The connection is <b>not</b> in auto commit mode, i.e. {@link Connection#commit()} has to be invoked manually.</p>
	 * 
	 * @return Connection to the data base.
	 * @throws EDatabase if there is a problem with the pooled data base connection, e.g. no more connections left in pool, pool closed, etc..
	 */
	public final static synchronized Connection connect() throws EDatabase { return connect(false); }
	
	
	/** <p>Returns a connection to the data base.</p>
	 * 
	 * <p>The connection is <b>not</b> in auto commit mode, i.e. {@link Connection#commit()} has to be invoked manually.</p>
	 * 
	 * @param Writeable Set to {@code true} to get a data base connection that can be used to write to the data base. Otherwise, a read-only connection is returned.
	 * @return Connection to the data base.
	 * @throws EDatabase if there is a problem with the pooled data base connection, e.g. no more connections left in pool, pool closed, etc..
	 */
	public final static synchronized Connection connect(boolean Writeable) throws EDatabase {
		try {
			Connection conn = m_ConnectionPool.borrowObject();
			conn.setReadOnly(!Writeable);
			conn.setAutoCommit(false);
			return conn;
		} catch (Exception Ex) { throw new EDatabase(Ex); }
	}
}
