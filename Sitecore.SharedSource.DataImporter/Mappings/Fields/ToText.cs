﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sitecore.SharedSource.DataImporter.Extensions;
using Sitecore.Data.Items;
using System.Web;
using Sitecore.Data.Fields;
using System.Data;
using System.Collections;
using Sitecore.SecurityModel;
using Sitecore.SharedSource.DataImporter.Providers;

namespace Sitecore.SharedSource.DataImporter.Mappings.Fields
{
    /// <summary>
    /// this stores the plain text import value as is into the new field
    /// </summary>
    public class ToText : BaseMapping, IBaseField
    {
		
		#region Properties 

        /// <summary>
        /// name field delimiter
        /// </summary>
		public char[] comSplitr = { ',' };

		private IEnumerable<string> _existingDataNames;
		/// <summary>
		/// the existing data fields you want to import
		/// </summary>
        public IEnumerable<string> ExistingDataNames {
			get {
				return _existingDataNames;
			}
			set {
				_existingDataNames = value;
			}
		}

		private string _delimiter;
		/// <summary>
		/// the delimiter you want to separate imported data with
		/// </summary>
        public string Delimiter {
			get {
				return _delimiter;
			}
			set {
				_delimiter = value;
			}
		}
		
		#endregion Properties
		
		#region Constructor

		public ToText(Item i) : base(i) {
            //store fields
            ExistingDataNames = i.Fields["From What Fields"].Value.Split(comSplitr, StringSplitOptions.RemoveEmptyEntries);
			Delimiter = i.Fields["Delimiter"].Value;
		}

		#endregion Constructor
		
		#region Methods

		public virtual void FillField(BaseDataMap map, ref Item newItem, string importValue) {
            //store the imported value as is
            Field f = newItem.Fields[NewItemField];
		    if (f != null)
		    {
		        if (string.IsNullOrEmpty(f.Value) || this.OverwriteValue)
		        {
                    try
                    {
                        using (new SecurityDisabler())
                        {
                            newItem.Editing.BeginEdit();
                            f.Value = importValue.Trim();
                            newItem.Editing.EndEdit();
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                   
		        }
		    }
		}

        #endregion Methods

        #region IBaseField Methods

        /// <summary>
        /// returns a string list of fields to import
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetExistingFieldNames()
        {
            return ExistingDataNames;
        }

        /// <summary>
        /// return the delimiter to separate imported values with
        /// </summary>
        /// <returns></returns>
        public string GetFieldValueDelimiter()
        {
            return Delimiter;
        }

        #endregion IBaseField Methods
    }
}
