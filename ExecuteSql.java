package com.mps.actions.data;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringEscapeUtils;

import com.mps.base.IMActionException;
import com.mps.base.BaseAction;
import com.mps.base.BaseCode;
import com.mps.base.IAction;
import com.mps.util.BaseData;
import com.mps.util.SQLConnection;

/**
 * 供ActiveX操作資料庫<br>
 * ActiveX會傳入加密過的sql語法，本程式會將其解密後執行
 */
public class ExecuteSql extends BaseAction implements IAction {
	
	private String[] dsNames = {BaseData.getDataSourceName()};
	
	public String getVersion() {
		return "1.0.0.0";
	}

	public ExecuteSql(String actionName) {
		super(actionName);
	}

	public void execute(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		try {
			switchMethod(request, response);
		}
		catch (Exception e) {
			showMessage(response, BaseCode.IM_EXECUTE_STATUSCODE_EORROR, e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * 依照傳入資料的不同呼叫不同的Method做處理
	 * @param request request
	 * @param response response
	 * @throws NamingException
	 * @throws SQLException
	 * @throws IOException
	 * @throws IMActionException
	 */
	private void switchMethod(HttpServletRequest request, HttpServletResponse response) throws NamingException, SQLException, IOException, IMActionException{
		String[] encstr = request.getParameterValues("encstr");
		if(encstr == null || encstr.length == 0) throw new IMActionException("未傳遞encstr");
		String ip = request.getRemoteAddr();
		int cs = getConnSwitch(request.getParameter("conn"));
		if(encstr.length > 1) {
			imlog.info("多筆執行update、inset、delete-IP:" + ip);
			showMessage(response, BaseCode.IM_EXECUTE_STATUSCODE_RIGHT, update(dsNames[cs], encstr));
		}
		else {
			if(encstr[0].length() <= 2) throw new IMActionException("錯誤的encstr");
			String strStatement = encoder.decode(encstr[0]);
			if(strStatement.length() <= 6) throw new IMActionException("錯誤的encstr(" + encstr[0] + ")");
			if(strStatement.toLowerCase().matches("^(update|insert|delete)[\\s\\S]*")) {
				imlog.info("單筆執行update、insert、delete-IP:" + ip);
				showMessage(response, BaseCode.IM_EXECUTE_STATUSCODE_RIGHT, update(dsNames[cs], strStatement));
			}
			else if(strStatement.toLowerCase().matches("^(create|alter|drop)[\\s\\S]*")) {
				imlog.info("單筆執行create、alter、drop-IP:" + ip);
				showMessage(response, BaseCode.IM_EXECUTE_STATUSCODE_RIGHT, update(dsNames[cs], strStatement));
			}
			else {
                
                imlog.debug("BEGIN.....................");
				imlog.info("查詢-IP:" + ip);
				if("insert".equals(request.getParameter("mode"))) {
					showMessage(response, BaseCode.IM_EXECUTE_STATUSCODE_RIGHT, insertmode(dsNames[cs], strStatement));
				}
				else {
					showMessage(response, BaseCode.IM_EXECUTE_STATUSCODE_RIGHT, query(dsNames[cs], strStatement));
				}
                imlog.debug("END......................");
			}
		}
		
	}
	
	/**
	 * 將select出來的資料，變成可以直接insert用的欄位值
	 * @param dsName DataSourceName目前無作用
	 * @param strStatement 要查詢的select語法
	 * @return 可以直接insert用的欄位值
	 * @throws NamingException
	 * @throws SQLException
	 */
	private ArrayList insertmode(String dsName, String strStatement)  throws NamingException, SQLException{
		imlog.info("執行的語法為:" + strStatement);
		ArrayList data = new ArrayList();
		SQLConnection conn = new SQLConnection(className);
		try{
			conn.open();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(strStatement);
			ResultSetMetaData rsmd = rs.getMetaData();
			int colCount = rsmd.getColumnCount();
			while(rs.next()) {
				StringBuffer sb = new StringBuffer();
				for(int i = 1; i <= colCount; i++) {
					if(i > 1) {
						sb.append(",");
					}
					String value = rs.getString(i);
					if(value != null) {
						switch(rsmd.getColumnType(i)) {
							case Types.VARCHAR:
							case Types.CHAR:
								sb.append("'").append(StringEscapeUtils.escapeSql(value)).append("'");
								break;
							case Types.DECIMAL:
							case Types.INTEGER:
								sb.append(value);
								break;
							case Types.DATE:
								sb.append("DATE('").append(value).append("')");
								break;
							case Types.TIME:
								sb.append("TIME('").append(value).append("')");
								break;
							case Types.TIMESTAMP:
								sb.append("TIMESTAMP('").append(value).append("')");
								break;							
						}
					}
					else {
						sb.append(value);
					}
				}
				data.add(sb.toString());
			}
		}
		finally {
			conn.close();
		}
		return data;
	}
	
	/**
	 * 把select執行後的結果傳出
	 * @param dsName DataSource Name 目前沒做用
	 * @param strStatement 已解密的sql語法
	 * @return select出來的資料，格式為：!@![欄位值1];[欄位值2];[欄位值3]....;
	 * @throws NamingException
	 * @throws SQLException
	 */
	private ArrayList query(String dsName, String strStatement) throws NamingException, SQLException{
		imlog.info("執行的語法為:" + strStatement);
		ArrayList data = new ArrayList();
		SQLConnection conn = new SQLConnection(className);
		try{
			conn.open();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(strStatement);
			int colCount = rs.getMetaData().getColumnCount();
			while(rs.next()) {
				StringBuffer sb = new StringBuffer("!@!");
				for(int i = 0; i < colCount; i++) {
					if(i > 0) {
						sb.append(";");
					}
					String value = rs.getString(i + 1);
					sb.append((value == null)?"":value);
				}
				sb.append(";");
				data.add(sb.toString());
			}
		}
		finally {
			conn.close();
		}
		return data;
	}
	
	/**
	 * 執行單句sql的insert、update、delete語法
	 * @param dsName
	 * @param strStatement
	 * @return 異動資料的筆數
	 * @throws NamingException
	 * @throws SQLException
	 */
	private String update(String dsName, String strStatement) throws NamingException, SQLException{
		imlog.debug("[CSR STRESS TEST 1-2]-更新資料庫(" + strStatement + ")--start");
		imlog.info("單筆執行:執行的語法為:" + strStatement);
		String count = "";
		SQLConnection conn = new SQLConnection(className);
		try{
			conn.open();
			Statement stmt = conn.createStatement();
			count += stmt.executeUpdate(strStatement);
		}
		finally {
			conn.close();
		}
		imlog.debug("[CSR STRESS TEST 1-2]-更新資料庫(" + strStatement + ")--end");
		return count;
	}
	
	/**
	 * 批次執行insert、update、delete
	 * @param dsName datasource name，目前沒做用
	 * @param encstr 尚未解密的批次執行sql語法
	 * @return 每句語法所異動到的資料筆數，各筆數間以「,」分隔 
	 * @throws NamingException
	 * @throws SQLException
	 * @throws IMActionException
	 */
	private synchronized String update(String dsName, String[] encstr) throws NamingException, SQLException, IMActionException {
		String exeCount = "";
		SQLConnection conn = new SQLConnection(className);
		try{
			conn.open();
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			String strStatement = "";
			for(int i = 0; i < encstr.length; i++) {
				if(encstr[i].length() <= 2) throw new IMActionException("錯誤的encstr(第" + (i + 1) + "個)");
				strStatement = encoder.decode(encstr[i]);
				imlog.info("多筆執行:執行語法為:" + strStatement);
				if(strStatement.length() > 6 && strStatement.substring(0, 6).toLowerCase().matches("insert|delete|update")) {
					exeCount += "," + stmt.executeUpdate(strStatement);
				}
				else throw new IMActionException("錯誤的encstr(第" + (i + 1) + "個:" + encstr[i] + ")");
			}
			conn.commit();
		}
		catch (SQLException e) {
			if(conn != null) {conn.rollback();}
			throw e;
		}
		catch (IMActionException e) {
			if(conn != null) {conn.rollback();}
			throw e;
		}
		finally {
			conn.close();
		}
		return exeCount.substring(1);
	}
	
	/**
	 * 將String類別的值轉成int
	 * @param conn 要使用的Connection
	 * @return 已轉成int的值
	 * @throws IMActionException
	 */
	private int getConnSwitch(String conn) throws IMActionException{
		if(conn == null || "".equals(conn) || !conn.matches("[0-9]+")) throw new IMActionException("錯誤的conn");
		int index = Integer.parseInt(conn) - 1;
		if(index >= dsNames.length) throw new IMActionException("錯誤的conn");
		return index;
	} 
	
}
