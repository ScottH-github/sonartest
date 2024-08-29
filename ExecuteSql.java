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
 * ��ActiveX�ާ@��Ʈw<br>
 * ActiveX�|�ǤJ�[�K�L��sql�y�k�A���{���|�N��ѱK�����
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
	 * �̷ӶǤJ��ƪ����P�I�s���P��Method���B�z
	 * @param request request
	 * @param response response
	 * @throws NamingException
	 * @throws SQLException
	 * @throws IOException
	 * @throws IMActionException
	 */
	private void switchMethod(HttpServletRequest request, HttpServletResponse response) throws NamingException, SQLException, IOException, IMActionException{
		String[] encstr = request.getParameterValues("encstr");
		if(encstr == null || encstr.length == 0) throw new IMActionException("���ǻ�encstr");
		String ip = request.getRemoteAddr();
		int cs = getConnSwitch(request.getParameter("conn"));
		if(encstr.length > 1) {
			imlog.info("�h������update�Binset�Bdelete-IP:" + ip);
			showMessage(response, BaseCode.IM_EXECUTE_STATUSCODE_RIGHT, update(dsNames[cs], encstr));
		}
		else {
			if(encstr[0].length() <= 2) throw new IMActionException("���~��encstr");
			String strStatement = encoder.decode(encstr[0]);
			if(strStatement.length() <= 6) throw new IMActionException("���~��encstr(" + encstr[0] + ")");
			if(strStatement.toLowerCase().matches("^(update|insert|delete)[\\s\\S]*")) {
				imlog.info("�浧����update�Binsert�Bdelete-IP:" + ip);
				showMessage(response, BaseCode.IM_EXECUTE_STATUSCODE_RIGHT, update(dsNames[cs], strStatement));
			}
			else if(strStatement.toLowerCase().matches("^(create|alter|drop)[\\s\\S]*")) {
				imlog.info("�浧����create�Balter�Bdrop-IP:" + ip);
				showMessage(response, BaseCode.IM_EXECUTE_STATUSCODE_RIGHT, update(dsNames[cs], strStatement));
			}
			else {
                
                imlog.debug("BEGIN.....................");
				imlog.info("�d��-IP:" + ip);
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
	 * �Nselect�X�Ӫ���ơA�ܦ��i�H����insert�Ϊ�����
	 * @param dsName DataSourceName�ثe�L�@��
	 * @param strStatement �n�d�ߪ�select�y�k
	 * @return �i�H����insert�Ϊ�����
	 * @throws NamingException
	 * @throws SQLException
	 */
	private ArrayList insertmode(String dsName, String strStatement)  throws NamingException, SQLException{
		imlog.info("���檺�y�k��:" + strStatement);
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
	 * ��select����᪺���G�ǥX
	 * @param dsName DataSource Name �ثe�S����
	 * @param strStatement �w�ѱK��sql�y�k
	 * @return select�X�Ӫ���ơA�榡���G!@![����1];[����2];[����3]....;
	 * @throws NamingException
	 * @throws SQLException
	 */
	private ArrayList query(String dsName, String strStatement) throws NamingException, SQLException{
		imlog.info("���檺�y�k��:" + strStatement);
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
	 * �����ysql��insert�Bupdate�Bdelete�y�k
	 * @param dsName
	 * @param strStatement
	 * @return ���ʸ�ƪ�����
	 * @throws NamingException
	 * @throws SQLException
	 */
	private String update(String dsName, String strStatement) throws NamingException, SQLException{
		imlog.debug("[CSR STRESS TEST 1-2]-��s��Ʈw(" + strStatement + ")--start");
		imlog.info("�浧����:���檺�y�k��:" + strStatement);
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
		imlog.debug("[CSR STRESS TEST 1-2]-��s��Ʈw(" + strStatement + ")--end");
		return count;
	}
	
	/**
	 * �妸����insert�Bupdate�Bdelete
	 * @param dsName datasource name�A�ثe�S����
	 * @param encstr �|���ѱK���妸����sql�y�k
	 * @return �C�y�y�k�Ҳ��ʨ쪺��Ƶ��ơA�U���ƶ��H�u,�v���j 
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
				if(encstr[i].length() <= 2) throw new IMActionException("���~��encstr(��" + (i + 1) + "��)");
				strStatement = encoder.decode(encstr[i]);
				imlog.info("�h������:����y�k��:" + strStatement);
				if(strStatement.length() > 6 && strStatement.substring(0, 6).toLowerCase().matches("insert|delete|update")) {
					exeCount += "," + stmt.executeUpdate(strStatement);
				}
				else throw new IMActionException("���~��encstr(��" + (i + 1) + "��:" + encstr[i] + ")");
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
	 * �NString���O�����নint
	 * @param conn �n�ϥΪ�Connection
	 * @return �w�নint����
	 * @throws IMActionException
	 */
	private int getConnSwitch(String conn) throws IMActionException{
		if(conn == null || "".equals(conn) || !conn.matches("[0-9]+")) throw new IMActionException("���~��conn");
		int index = Integer.parseInt(conn) - 1;
		if(index >= dsNames.length) throw new IMActionException("���~��conn");
		return index;
	} 
	
}
