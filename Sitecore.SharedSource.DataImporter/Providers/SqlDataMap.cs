﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sitecore.Data.Fields;
using Sitecore.Data.Items;
using Sitecore.Data;
using System.Data;
using System.Data.SqlClient;
using System.Web;
using Sitecore.SharedSource.DataImporter.Mappings.Fields;
using Sitecore.SharedSource.DataImporter.Extensions;
using System.Collections;

namespace Sitecore.SharedSource.DataImporter.Providers
{
	public class SqlDataMap : BaseDataMap {
		
		#region Properties

		#endregion Properties

		#region Constructor

		public SqlDataMap(Database db, string connectionString, Item importItem, string lastUpdated = "") : base (db, connectionString, importItem, lastUpdated) {
		}
		
		#endregion Constructor

        #region Override Methods

        /// <summary>
        /// uses a SqlConnection to get data
        /// </summary>
        /// <returns></returns>
        public override IEnumerable<object> GetImportData()
        {
            DataSet ds = new DataSet();
            SqlConnection dbCon = new SqlConnection(this.DatabaseConnectionString);
            dbCon.Open();


            if (DeltasOnly)
            {
                Query = string.Format("{0} WHERE {1} > '{2}'", this.Query, LastUpdatedFieldName,
                    LastUpdated.ToString("d"));
            }
            SqlCommand command = new SqlCommand(this.Query, dbCon);
            command.CommandTimeout = 3000;
            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(ds);
            dbCon.Close();

            DataTable dt = ds.Tables[0].Copy();
            
            return (from DataRow dr in dt.Rows
                    select dr).Cast<object>();
        }

        public override IEnumerable<object> SyncDeletions()
        {
            DataSet ds = new DataSet();
            SqlConnection dbCon = new SqlConnection(this.DatabaseConnectionString);
            dbCon.Open();

            SqlDataAdapter adapter = new SqlDataAdapter(this.MissingItemsQuery, dbCon);
            adapter.Fill(ds);
            dbCon.Close();

            DataTable dt = ds.Tables[0].Copy();

            return (from DataRow dr in dt.Rows
                    select dr).Cast<object>();
        }

	    public override void TakeHistorySnapshot()
	    {
            DataSet ds = new DataSet();
            SqlConnection dbCon = new SqlConnection(this.DatabaseConnectionString);
            dbCon.Open();

            SqlDataAdapter adapter = new SqlDataAdapter(this.HistorySnapshotQuery, dbCon);
            adapter.Fill(ds);
            dbCon.Close();
	    }

	    /// <summary>
        /// doesn't handle any custom data
        /// </summary>
        /// <param name="newItem"></param>
        /// <param name="importRow"></param>
        public override void ProcessCustomData(ref Item newItem, object importRow)
        {
        }
        
        /// <summary>
        /// gets custom data from a DataRow
        /// </summary>
        /// <param name="importRow"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        protected override string GetFieldValue(object importRow, string fieldName)
        {
            DataRow item = importRow as DataRow;
            object f = item[fieldName];
            return (f != null) ? f.ToString() : string.Empty;
        }

        #endregion Override Methods

        #region Methods

        #endregion Methods
    }
}
