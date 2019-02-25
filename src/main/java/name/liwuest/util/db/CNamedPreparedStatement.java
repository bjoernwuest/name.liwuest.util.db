package name.liwuest.util.db;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import name.liwuest.util.types.CPair;

public class CNamedPreparedStatement {
	private final String m_SQL;
	private final Map<String, LinkedList<Integer>> m_Indexes;
	
	/** <p>Parses the given SQL.</p>
	 * 
	 * @param SQL The SQL to parse.
	 * @return The parsed SQL to be used within {@link PreparedStatement}.
	 */
	private final String m_ParseString(String SQL) {
		StringBuffer result = new StringBuffer(SQL.length());
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;
		int counter = 1;
		for (int pos = 0; pos < SQL.length(); pos++) {
			if (inSingleQuote) {
				char c = SQL.charAt(pos);
				result.append(c);
				if ('\'' == c) { inSingleQuote = false; }
			} else if (inDoubleQuote) {
				char c = SQL.charAt(pos);
				result.append(c);
				if ('"' == c) { inDoubleQuote = false; }
			} else {
				char c = SQL.charAt(pos);
				if ('\'' == c) {
					result.append(c);
					inSingleQuote = true;
				} else if ('"' == c) {
					result.append(c);
					inDoubleQuote = true;
				} else if (':' == c) {
					pos++;
					StringBuffer name = new StringBuffer(10);
					for (; pos < SQL.length(); pos++) {
						c = SQL.charAt(pos);
						if (!Character.isJavaIdentifierPart(c)) {
							pos--; // push back symbol
							break;
						}
						name.append(c);
					}
					result.append('?');
					LinkedList<Integer> indexes = m_Indexes.get(name.toString());
					if (null == indexes) {
						indexes = new LinkedList<>();
						m_Indexes.put(name.toString(), indexes);
					}
					indexes.add(counter++);
				} else { result.append(c); }
			}
		}
		return result.toString();
	}
	

	
	protected CNamedPreparedStatement(CNamedPreparedStatement Parent) {
		m_SQL = Parent.m_SQL;
		m_Indexes = Parent.m_Indexes;
	}
	public CNamedPreparedStatement(String SQL) {
		m_Indexes = new TreeMap<>();
		m_SQL = m_ParseString(SQL);
	}
	
	
	public final COpenedNamedPreparedStatement open(Connection Conn) throws SQLException { return new COpenedNamedPreparedStatement(this, Conn.prepareStatement(m_SQL)); }
	
	
	final Map<String, LinkedList<Integer>> _getIndexes() { return m_Indexes; }
	
	
	public final static class COpenedNamedPreparedStatement extends CNamedPreparedStatement implements AutoCloseable {
		// FIXME: consider batch queue length handling
		
		/** <p>The {@link CNamedPreparedStatement} this one is derived from.</p> */
		private final CNamedPreparedStatement m_Parent;
		/** <p>The actual prepared statement to use.</p> */
		private final PreparedStatement p_Stmt;
		/** <p>Internally stored parameter values.</p> */
		private final Map<String, CPair<Type, CPair<Object, Object>>> m_Values = new TreeMap<>();
		
		
		/** <p>Pass local parameters to {@link #p_Stmt prepared statement}.</p>
		 * 
		 * @throws SQLException if there is any SQL problem with setting the parameters.
		 */
		private void m_SetParameters() throws SQLException {
			TreeSet<String> keys = new TreeSet<>(m_Values.keySet());
			keys.retainAll(m_Parent._getIndexes().keySet());
			for (String key : keys) {
				CPair<Type, CPair<Object, Object>> v = m_Values.get(key);
				for (int idx : m_Parent._getIndexes().get(key)) {
					switch (v.getLeft()) {
						case URL: { p_Stmt.setURL(idx, (URL)(v.getRight().getLeft())); break; }
						case Timestamp: {
							if (null == v.getRight().getRight()) { p_Stmt.setTimestamp(idx, (Timestamp)(v.getRight().getLeft())); }
							else { p_Stmt.setTimestamp(idx, (Timestamp)(v.getRight().getLeft()), (Calendar)(v.getRight().getRight())); }
							break;
						}
						case Time: {
							if (null == v.getRight().getRight()) { p_Stmt.setTime(idx, (Time)(v.getRight().getLeft())); }
							else { p_Stmt.setTime(idx, (Time)(v.getRight().getLeft()), (Calendar)(v.getRight().getRight())); }
							break;
						}
						case String: { p_Stmt.setString(idx, (String)(v.getRight().getLeft())); break; }
						case Short: { p_Stmt.setShort(idx, (Short)(v.getRight().getLeft())); break; }
						case SQLXML: { p_Stmt.setSQLXML(idx, (SQLXML)(v.getRight().getLeft())); break; }
						case RowId: { p_Stmt.setRowId(idx, (RowId)(v.getRight().getLeft())); break; }
						case Ref: { p_Stmt.setRef(idx, (Ref)(v.getRight().getLeft())); break; }
						case Object: {
							if (null == CPair.class.cast(v.getRight().getRight()).getLeft()) { p_Stmt.setObject(idx, v.getRight().getLeft()); }
							else if (null == CPair.class.cast(v.getRight().getRight()).getRight()) { p_Stmt.setObject(idx, v.getRight().getLeft(), (Integer)(CPair.class.cast(v.getRight().getRight()).getLeft())); }
							else { p_Stmt.setObject(idx, v.getRight().getLeft(), (Integer)(CPair.class.cast(v.getRight().getRight()).getLeft()), (Integer)(CPair.class.cast(v.getRight().getRight()).getRight())); }
							break;
						}
						case Null: {
							if (null == v.getRight().getRight()) { p_Stmt.setNull(idx, (Integer)(v.getRight().getLeft())); }
							else { p_Stmt.setNull(idx, (Integer)(v.getRight().getLeft()), (String)(v.getRight().getRight())); }
							break;
						}
						case NString: { p_Stmt.setNString(idx, (String)(v.getRight().getLeft())); break; }
						case NClobReader: {
							if (null == v.getRight().getRight()) { p_Stmt.setNClob(idx, (Reader)(v.getRight().getLeft())); }
							else { p_Stmt.setNClob(idx, (Reader)(v.getRight().getLeft()), (Long)(v.getRight().getRight())); }
							break;
						}
						case NClob: { p_Stmt.setNClob(idx, (NClob)(v.getRight().getLeft())); break; }
						case NCharacterStream: {
							if (null == v.getRight().getRight()) { p_Stmt.setNCharacterStream(idx, (Reader)(v.getRight().getLeft())); }
							else { p_Stmt.setNCharacterStream(idx, (Reader)(v.getRight().getLeft()), (Long)(v.getRight().getRight())); }
							break;
						}
						case Long: { p_Stmt.setLong(idx, (Long)(v.getRight().getLeft())); break; }
						case Int: { p_Stmt.setInt(idx, (Integer)(v.getRight().getLeft())); break; }
						case Float: { p_Stmt.setFloat(idx, (Float)(v.getRight().getLeft())); break; }
						case Double: { p_Stmt.setDouble(idx, (Double)(v.getRight().getLeft())); break; }
						case Date: {
							if (null == v.getRight().getRight()) { p_Stmt.setDate(idx, (Date)(v.getRight().getLeft())); }
							else { p_Stmt.setDate(idx, (Date)(v.getRight().getLeft()), (Calendar)(v.getRight().getRight())); }
							break;
						}
						case ClobReader: {
							if (null == v.getRight().getRight()) { p_Stmt.setClob(idx, (Reader)(v.getRight().getLeft())); }
							else { p_Stmt.setClob(idx, (Reader)(v.getRight().getLeft()), (Long)(v.getRight().getRight())); }
							break;
						}
						case Clob: { p_Stmt.setClob(idx, (Clob)(v.getRight().getLeft())); break; }
						case CharacterStream: {
							if (null == v.getRight().getRight()) { p_Stmt.setCharacterStream(idx, (Reader)(v.getRight().getLeft())); }
							else {
								if (Integer.class.isInstance(v.getRight().getRight())) { p_Stmt.setCharacterStream(idx, (Reader)(v.getRight().getLeft()), Integer.class.cast(v.getRight().getRight())); }
								else { p_Stmt.setCharacterStream(idx, (Reader)(v.getRight().getLeft()), (Long)(v.getRight().getRight())); }
							}
							break;
						}
						case Bytes: { p_Stmt.setBytes(idx, (byte[])(v.getRight().getLeft())); break; }
						case Byte: { p_Stmt.setByte(idx, (Byte)(v.getRight().getLeft())); break; }
						case Boolean: { p_Stmt.setBoolean(idx, (Boolean)(v.getRight().getLeft())); break; }
						case BlobIS: {
							if (null == v.getRight().getRight()) { p_Stmt.setBlob(idx, (InputStream)(v.getRight().getLeft())); }
							else { p_Stmt.setBlob(idx, (InputStream)(v.getRight().getLeft()), (Long)(v.getRight().getRight())); }
							break;
						}
						case Blob: { p_Stmt.setBlob(idx, (Blob)(v.getRight().getLeft())); break; }
						case BinaryStream: {
							if (null == v.getRight().getRight()) { p_Stmt.setBinaryStream(idx, (InputStream)(v.getRight().getLeft())); }
							else {
								if (Integer.class.isInstance(v.getRight().getRight())) { p_Stmt.setBinaryStream(idx, (InputStream)(v.getRight().getLeft()), Integer.class.cast(v.getRight().getRight())); }
								else { p_Stmt.setBinaryStream(idx, (InputStream)(v.getRight().getLeft()), (Long)(v.getRight().getRight())); }
							}
							break;
						}
						case BigDecimal: { p_Stmt.setBigDecimal(idx, (BigDecimal)(v.getRight().getLeft())); break; }
						case AsciiStream: {
							if (null == v.getRight().getRight()) { p_Stmt.setAsciiStream(idx, (InputStream)(v.getRight().getLeft())); }
							else {
								if (Integer.class.isInstance(v.getRight().getRight())) { p_Stmt.setAsciiStream(idx, (InputStream)(v.getRight().getLeft()), Integer.class.cast(v.getRight().getRight())); }
								else { p_Stmt.setAsciiStream(idx, (InputStream)(v.getRight().getLeft()), (Long)(v.getRight().getRight())); }
							}
							break;
						}
						case Array: { p_Stmt.setArray(idx, (Array)(v.getRight().getLeft())); break; }
						default: {
							System.err.println("Unknown type: " + v.getLeft());
							System.exit(-1);
						}
					}
				}
			}
		}
		
		
		COpenedNamedPreparedStatement(CNamedPreparedStatement Parent, PreparedStatement PrepStatement) {
			super(Parent);
			m_Parent = Parent;
			p_Stmt = PrepStatement;
		}
		
		/** <p>See {@link PreparedStatement#close()}.</p>
		 * 
		 * @throws SQLException See {@link PreparedStatement#close()}.
		 */
		public void close() throws SQLException { p_Stmt.close(); }
		/** <p>See {@link PreparedStatement#closeOnCompletion()}.</p>
		 * 
		 * @return The named prepared statement itself.
		 * @throws SQLException See {@link PreparedStatement#closeOnCompletion()}.
		 */
		public COpenedNamedPreparedStatement closeOnCompletion() throws SQLException { p_Stmt.closeOnCompletion(); return this; }
		/** <p>See {@link PreparedStatement#isCloseOnCompletion()}.</p>
		 * 
		 * @return See {@link PreparedStatement#isCloseOnCompletion()}.
		 * @throws SQLException See {@link PreparedStatement#isCloseOnCompletion()}.
		 */
		public boolean isCloseOnCompletion() throws SQLException { return p_Stmt.isCloseOnCompletion(); }
		/** <p>See {@link PreparedStatement#isClosed()}.</p>
		 * 
		 * @return See {@link PreparedStatement#isClosed()}.
		 * @throws SQLException See {@link PreparedStatement#isClosed()}.
		 */
		public boolean isClosed() throws SQLException { return p_Stmt.isClosed(); }
		/** <p>See {@link PreparedStatement#cancel()}.</p>
		 * 
		 * @return The named prepared statement itself.
		 * @throws SQLException See {@link PreparedStatement#cancel()}.
		 */
		public COpenedNamedPreparedStatement cancel() throws SQLException { p_Stmt.cancel(); return this; }
		
		
		/** <p>See {@link PreparedStatement#addBatch()}.</p>
		 * 
		 * @return The named prepared statement itself.
		 * @throws SQLException See {@link PreparedStatement#addBatch()}.
		 */
		public COpenedNamedPreparedStatement addBatch() throws SQLException {
			m_SetParameters();
			p_Stmt.addBatch();
			return this;
		}
		/** <p>See {@link PreparedStatement#clearBatch()}.</p>
		 * 
		 * @return The named prepared statement itself.
		 * @throws SQLException See {@link PreparedStatement#clearBatch()}.
		 */
		public COpenedNamedPreparedStatement clearBatch() throws SQLException { p_Stmt.clearBatch(); return this; }
		/** <p>See {@link PreparedStatement#executeBatch()}.</p>
		 * 
		 * @return The named prepared statement itself.
		 * @throws SQLException See {@link PreparedStatement#executeBatch()}.
		 */
		public COpenedNamedPreparedStatement executeBatch() throws SQLException { p_Stmt.executeBatch(); return this; }
		/** <p>See {@link PreparedStatement#execute()}.</p>
		 * 
		 * @return The named prepared statement itself.
		 * @throws SQLException See {@link PreparedStatement#execute()}.
		 */
		public COpenedNamedPreparedStatement execute() throws SQLException {
			m_SetParameters();
			p_Stmt.execute();
			return this;
		}
		/** <p>See {@link PreparedStatement#executeQuery()}.</p>
		 * 
		 * @return See {@link PreparedStatement#executeQuery()}.
		 * @throws SQLException See {@link PreparedStatement#executeQuery()}.
		 */
		public ResultSet executeQuery() throws SQLException {
			m_SetParameters();
			return p_Stmt.executeQuery();
		}
		/** <p>See {@link PreparedStatement#executeUpdate()}.</p>
		 * 
		 * @return See {@link PreparedStatement#executeUpdate()}.
		 * @throws SQLException See {@link PreparedStatement#executeUpdate()}.
		 */
		public int executeUpdate() throws SQLException {
			m_SetParameters();
			return p_Stmt.executeUpdate();
		}
		
		/** <p>See {@link PreparedStatement#clearWarnings()}.</p>
		 * 
		 * @return The named prepared statement itself.
		 * @throws SQLException See {@link PreparedStatement#clearWarnings()}.
		 */
		public COpenedNamedPreparedStatement clearWarnings() throws SQLException { p_Stmt.clearWarnings(); return this; }
		/** <p>See {@link PreparedStatement#getWarnings()}.</p>
		 * 
		 * @return See {@link PreparedStatement#getWarnings()}.
		 * @throws SQLException See {@link PreparedStatement#getWarnings()}.
		 */
		public SQLWarning getWarnings() throws SQLException { return p_Stmt.getWarnings(); }
		/** <p>See {@link PreparedStatement#clearParameters()}.</p>
		 * 
		 * @return The named prepared statement itself.
		 */
		public COpenedNamedPreparedStatement clearParameters() { m_Values.clear(); return this; }

		private static enum Type {
			Array,
			AsciiStream,
			BigDecimal,
			BinaryStream,
			Blob,
			BlobIS,
			Boolean,
			Byte,
			Bytes,
			CharacterStream,
			Clob,
			ClobReader,
			Date,
			Double,
			Float,
			Int,
			Long,
			NCharacterStream,
			NClob,
			NClobReader,
			NString,
			Null,
			Object,
			Ref,
			RowId,
			SQLXML,
			Short,
			String,
			Time,
			Timestamp,
			URL,
			UnicodeStream
		}
		
		/** <p>See {@link PreparedStatement#setArray(String, Array)}.</p>
		 *
		 * @param ParameterName The name of the parameter to set.
		 * @param Value See {@link PreparedStatement#setArray(String, Array)}.
		 * @return The named prepared statement itself.
		 */
		public COpenedNamedPreparedStatement setArray(String ParameterName, Array Value) { m_Values.put(ParameterName, new CPair<>(Type.Array, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setAsciiStream(String ParameterName, InputStream Value) { m_Values.put(ParameterName, new CPair<>(Type.AsciiStream, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setAsciiStream(String ParameterName, InputStream Value, int Length) { m_Values.put(ParameterName, new CPair<>(Type.AsciiStream, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setAsciiStream(String ParameterName, InputStream Value, long Length) { m_Values.put(ParameterName, new CPair<>(Type.AsciiStream, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setBigDecimal(String ParameterName, BigDecimal Value) { m_Values.put(ParameterName, new CPair<>(Type.BigDecimal, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setBinaryStream(String ParameterName, InputStream Value) { m_Values.put(ParameterName, new CPair<>(Type.BinaryStream, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setBinaryStream(String ParameterName, InputStream Value, int Length) { m_Values.put(ParameterName, new CPair<>(Type.BinaryStream, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setBinaryStream(String ParameterName, InputStream Value, long Length) { m_Values.put(ParameterName, new CPair<>(Type.BinaryStream, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setBlob(String ParameterName, Blob Value) { m_Values.put(ParameterName, new CPair<>(Type.Blob, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setBlob(String ParameterName, InputStream Value) { m_Values.put(ParameterName, new CPair<>(Type.BlobIS, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setBlob(String ParameterName, InputStream Value, long Length) { m_Values.put(ParameterName, new CPair<>(Type.BlobIS, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setBoolean(String ParameterName, boolean Value) { m_Values.put(ParameterName, new CPair<>(Type.Boolean, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setByte(String ParameterName, byte Value) { m_Values.put(ParameterName, new CPair<>(Type.Byte, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setBytes(String ParameterName, byte[] Value) { m_Values.put(ParameterName, new CPair<>(Type.Bytes, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setCharacterStream(String ParameterName, Reader Value) { m_Values.put(ParameterName, new CPair<>(Type.CharacterStream, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setCharacterStream(String ParameterName, Reader Value, int Length) { m_Values.put(ParameterName, new CPair<>(Type.CharacterStream, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setCharacterStream(String ParameterName, Reader Value, long Length) { m_Values.put(ParameterName, new CPair<>(Type.CharacterStream, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setClob(String ParameterName, Clob Value) { m_Values.put(ParameterName, new CPair<>(Type.Clob, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setClob(String ParameterName, Reader Value) { m_Values.put(ParameterName, new CPair<>(Type.ClobReader, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setClob(String ParameterName, Reader Value, long Length) { m_Values.put(ParameterName, new CPair<>(Type.ClobReader, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setDate(String ParameterName, Date Value) { m_Values.put(ParameterName, new CPair<>(Type.Date, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setDate(String ParameterName, Date Value, Calendar Calendar) { m_Values.put(ParameterName, new CPair<>(Type.Date, new CPair<>(Value, Calendar))); return this; }
		public COpenedNamedPreparedStatement setDouble(String ParameterName, double Value) { m_Values.put(ParameterName, new CPair<>(Type.Double, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setFloat(String ParameterName, float Value) { m_Values.put(ParameterName, new CPair<>(Type.Float, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setInt(String ParameterName, int Value) { m_Values.put(ParameterName, new CPair<>(Type.Int, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setLong(String ParameterName, long Value) { m_Values.put(ParameterName, new CPair<>(Type.Long, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setNCharacterStream(String ParameterName, Reader Value) { m_Values.put(ParameterName, new CPair<>(Type.NCharacterStream, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setNCharacterStream(String ParameterName, Reader Value, long Length) { m_Values.put(ParameterName, new CPair<>(Type.NCharacterStream, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setNClob(String ParameterName, NClob Value) { m_Values.put(ParameterName, new CPair<>(Type.NClob, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setNClob(String ParameterName, Reader Value) { m_Values.put(ParameterName, new CPair<>(Type.NClobReader, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setNClob(String ParameterName, Reader Value, long Length) { m_Values.put(ParameterName, new CPair<>(Type.NClobReader, new CPair<>(Value, Length))); return this; }
		public COpenedNamedPreparedStatement setNString(String ParameterName, String Value) { m_Values.put(ParameterName, new CPair<>(Type.NString, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setNull(String ParameterName, int SQLType) { m_Values.put(ParameterName, new CPair<>(Type.Null, new CPair<>(SQLType, null))); return this; }
		public COpenedNamedPreparedStatement setNull(String ParameterName, int SQLType, String TypeName) { m_Values.put(ParameterName, new CPair<>(Type.Null, new CPair<>(SQLType, TypeName))); return this; }
		public COpenedNamedPreparedStatement setObject(String ParameterName, Object Value) { m_Values.put(ParameterName, new CPair<>(Type.Object, new CPair<>(Value, new CPair<>(null, null)))); return this; }
		public COpenedNamedPreparedStatement setObject(String ParameterName, Object Value, int TargetSQLType) { m_Values.put(ParameterName, new CPair<>(Type.Object, new CPair<>(Value, new CPair<>(TargetSQLType, null)))); return this; }
		public COpenedNamedPreparedStatement setObject(String ParameterName, Object Value, int TargetSQLType, int sCalendareOrLength) { m_Values.put(ParameterName, new CPair<>(Type.Object, new CPair<>(Value, new CPair<>(TargetSQLType, sCalendareOrLength)))); return this; }
		public COpenedNamedPreparedStatement setRef(String ParameterName, Ref Value) { m_Values.put(ParameterName, new CPair<>(Type.Ref, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setRowId(String ParameterName, RowId Value) { m_Values.put(ParameterName, new CPair<>(Type.RowId, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setSQLXML(String ParameterName, SQLXML Value) { m_Values.put(ParameterName, new CPair<>(Type.SQLXML, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setShort(String ParameterName, short Value) { m_Values.put(ParameterName, new CPair<>(Type.Short, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setString(String ParameterName, String Value) { m_Values.put(ParameterName, new CPair<>(Type.String, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setTime(String ParameterName, Time Value) { m_Values.put(ParameterName, new CPair<>(Type.Time, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setTime(String ParameterName, Time Value, Calendar Calendar) { m_Values.put(ParameterName, new CPair<>(Type.Time, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setTimestamp(String ParameterName, Timestamp Value) { m_Values.put(ParameterName, new CPair<>(Type.Timestamp, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setTimestamp(String ParameterName, Timestamp Value, Calendar Calendar) { m_Values.put(ParameterName, new CPair<>(Type.Timestamp, new CPair<>(Value, null))); return this; }
		public COpenedNamedPreparedStatement setURL(String ParameterName, URL Value) { m_Values.put(ParameterName, new CPair<>(Type.URL, new CPair<>(Value, null))); return this; }
	}
}
