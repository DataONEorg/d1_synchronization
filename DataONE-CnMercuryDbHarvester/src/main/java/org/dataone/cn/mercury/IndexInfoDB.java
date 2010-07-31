package org.dataone.cn.mercury;

/*
 *   3/29 new method h_0  one insert per call to update on new instance detected.
 */

import java.sql.*;
import gov.ornl.mercury.harvest.beans.*;

public class IndexInfoDB 
{

	private DBAccessor access;

	
	public IndexInfo updateIndexInfo(IndexInfo indexInfo)
	{
		Connection conn = access.getConnection();
		String sql = "select index_info_id from " + access.getDbName() + ".index_info where instance=? and index_name=?";
		try
		{
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, indexInfo.getInstance());
			pstmt.setString(2, indexInfo.getIndexName());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next())
			{
				indexInfo.setIndexInfoID(rs.getInt(1));
			}
			else
			{
				String insertSQL = "insert into " + access.getDbName() + ".index_info values (?, ?,?)";
				PreparedStatement insPstmt = conn.prepareStatement(insertSQL);
				insPstmt.setString(1, null);
				insPstmt.setString(2, indexInfo.getInstance());
				insPstmt.setString(3, indexInfo.getIndexName());
				insPstmt.executeUpdate();
				indexInfo.setIndexInfoID(this.getIndexID(indexInfo));
			}
		}
		catch(Exception e)
		{
			System.out.println("Caught Exception inside IndexInfoDB.UpdateIndexInfo:" + e);
			//e.printStackTrace();
		}
		return indexInfo;
	}
	
	public int getIndexID(IndexInfo indexInfo)
	{
		Connection conn = access.getConnection();
		String sql = "select index_info_id from " + access.getDbName() + ".index_info where instance = ? and index_name=?";
		int indexID = 0;
		try
		{
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, indexInfo.getInstance());
			pstmt.setString(2, indexInfo.getIndexName());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next())
			{
				indexID = rs.getInt(1); 
				
			}
		}
		catch(Exception e)
		{
			System.out.println("Exception inside IndexInfoDB.getIndexID:");
			e.printStackTrace();
		}
		return indexID;
	}

    public DBAccessor getAccess() {
        return access;
    }

    public void setAccess(DBAccessor access) {
        this.access = access;
    }

}
