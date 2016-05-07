using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Text;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Maintenance;
using Sitecore.ContentSearch.SearchTypes;
using Sitecore.Data;
using Sitecore.Data.Fields;
using Sitecore.Data.Items;
using Sitecore.Data.Managers;
using Sitecore.Globalization;
using Sitecore.Reflection;
using Sitecore.SharedSource.DataImporter.Extensions;
using Sitecore.SharedSource.DataImporter.Mappings;
using Sitecore.SharedSource.DataImporter.Mappings.Fields;
using Sitecore.SharedSource.DataImporter.Utility;

namespace Sitecore.SharedSource.DataImporter.Providers
{
    /// <summary>
    ///     The BaseDataMap is the base class for any data provider. It manages values stored in sitecore
    ///     and does the bulk of the work processing the fields
    /// </summary>
    public abstract class BaseDataMap
    {
        #region Constructor

        public BaseDataMap(Database db, string connectionString, Item importItem, string lastUpdated = "")
        {
            Overwrite = importItem.Fields["Overwrite Item"] != null &&
                        ((CheckboxField) importItem.Fields["Overwrite Item"]).Checked;
            SkipExistingItems = importItem.Fields["Skip Existing Items"] != null &&
                                ((CheckboxField) importItem.Fields["Skip Existing Items"]).Checked;
            SearchIndex = importItem.Fields["Search Index"] != null ? importItem.Fields["Search Index"].Value : "";
            DeltasOnly = importItem.Fields["Deltas Only"] != null &&
                         ((CheckboxField) importItem.Fields["Deltas Only"]).Checked;
            LastUpdatedFieldName = importItem.Fields["Last Updated Field Name"] != null
                ? importItem.Fields["Last Updated Field Name"].Value
                : "";
            LastUpdated = string.IsNullOrEmpty(lastUpdated) ? DateTime.Now.AddDays(-30) : DateTime.Parse(lastUpdated);
            MissingItemsQuery = importItem.Fields["Missing Items Query"] != null
                ? importItem.Fields["Missing Items Query"].Value
                : "";
            HistorySnapshotQuery = importItem.Fields["History Snapshot Query"] != null
                ? importItem.Fields["History Snapshot Query"].Value
                : "";
            NotificationAddressList = importItem.Fields["Notification Addresses"]?.Value.Split(',').ToList() ?? new List<string>();

            SendEmail = importItem.Fields["Notification Email"] != null &&
                        ((CheckboxField)importItem.Fields["Notification Email"]).Checked;

            //instantiate log
            log = new StringBuilder();

            //setup import details
            SitecoreDB = db;
            DatabaseConnectionString = connectionString;
            //get query
            Query = importItem.Fields["Query"].Value;
            if (string.IsNullOrEmpty(Query))
            {
                Log("Error", "the 'Query' field was not set");
            }
            //get parent and store it
            var parentID = importItem.Fields["Import To Where"].Value;
            if (!string.IsNullOrEmpty(parentID))
            {
                var parent = SitecoreDB.Items[parentID];
                if (parent.IsNotNull())
                    Parent = parent;
                else
                    Log("Error", "the 'To Where' item is null");
            }
            else
            {
                Log("Error", "the 'To Where' field is not set");
            }
            //get new item template
            var templateID = importItem.Fields["Import To What Template"].Value;
            if (!string.IsNullOrEmpty(templateID))
            {
                var templateItem = SitecoreDB.Items[templateID];
                if (templateItem.IsNotNull())
                {
                    if ((BranchItem) templateItem != null)
                    {
                        NewItemTemplate = (BranchItem) templateItem;
                    }
                    else
                    {
                        NewItemTemplate = (TemplateItem) templateItem;
                    }
                }
                else
                {
                    Log("Error", "the 'To What Template' item is null");
                }
            }
            else
            {
                Log("Error", "the 'To What Template' field is not set");
            }
            //more properties
            ItemNameDataField = importItem.Fields["Pull Item Name from What Fields"].Value;
            ItemNameMaxLength = int.Parse(importItem.Fields["Item Name Max Length"].Value);
            var iLang = SitecoreDB.GetItem(importItem.Fields["Import To Language"].Value);
            ImportToLanguage = LanguageManager.GetLanguage(iLang.Name);
            if (ImportToLanguage == null)
                Log("Error", "The 'Import Language' field is not set");

            //foldering information
            FolderByDate = ((CheckboxField) importItem.Fields["Folder By Date"]).Checked;
            FolderByName = ((CheckboxField) importItem.Fields["Folder By Name"]).Checked;
            DateField = importItem.Fields["Date Field"].Value;
            if (FolderByName || FolderByDate)
            {
                //setup a default type to an ordinary folder
                var FolderItem = SitecoreDB.Templates["{A87A00B1-E6DB-45AB-8B54-636FEC3B5523}"];
                //if they specify a type then use that
                var folderID = importItem.Fields["Folder Template"].Value;
                if (!string.IsNullOrEmpty(folderID))
                    FolderItem = SitecoreDB.Templates[folderID];
                FolderTemplate = FolderItem;
            }

            //start handling fields
            var Fields = GetItemByTemplate(importItem, FieldsFolderID);
            if (Fields.IsNotNull())
            {
                var c = Fields.GetChildren();
                if (c.Any())
                {
                    foreach (Item child in c)
                    {
                        //create an item to get the class / assembly name from
                        var bm = new BaseMapping(child);
                        if (!string.IsNullOrEmpty(bm.HandlerAssembly))
                        {
                            if (!string.IsNullOrEmpty(bm.HandlerClass))
                            {
                                //create the object from the class and cast as base field to add it to field definitions
                                IBaseField bf = null;
                                try
                                {
                                    bf =
                                        (IBaseField)
                                            ReflectionUtil.CreateObject(bm.HandlerAssembly, bm.HandlerClass,
                                                new object[] {child});
                                }
                                catch (FileNotFoundException fnfe)
                                {
                                    Log("Error",
                                        string.Format("the field:{0} binary {1} specified could not be found",
                                            child.Name, bm.HandlerAssembly));
                                }
                                if (bf != null)
                                    FieldDefinitions.Add(bf);
                                else
                                    Log("Error",
                                        string.Format("the field: '{0}' class type {1} could not be instantiated",
                                            child.Name, bm.HandlerClass));
                            }
                            else
                            {
                                Log("Error",
                                    string.Format("the field: '{0}' Handler Class {1} is not defined", child.Name,
                                        bm.HandlerClass));
                            }
                        }
                        else
                        {
                            Log("Error",
                                string.Format("the field: '{0}' Handler Assembly {1} is not defined", child.Name,
                                    bm.HandlerAssembly));
                        }
                    }
                }
                else
                {
                    Log("Warn", "there are no fields to import");
                }
            }
            else
            {
                Log("Warn", "there is no 'Fields' folder");
            }
        }

        #endregion Constructor

        #region Properties

        /// <summary>
        ///     the log is returned with any messages indicating the status of the import
        /// </summary>
        protected StringBuilder log;

        /// <summary>
        ///     template id of the fields folder
        /// </summary>
        public static readonly string FieldsFolderID = "{98EF4356-8BFE-4F6A-A697-ADFD0AAD0B65}";

        /// <summary>
        ///     the parent item where the new items will be imported into
        /// </summary>
        public Item Parent { get; set; }

        /// <summary>
        ///     the reference to the sitecore database that you'll import into and query from
        /// </summary>
        public Database SitecoreDB { get; set; }

        /// <summary>
        ///     the template to create new items with
        /// </summary>
        public CustomItemBase NewItemTemplate { get; set; }

        /// <summary>
        ///     the sitecore field value of fields used to build the new item name
        /// </summary>
        public string ItemNameDataField { get; set; }

        private string[] _NameFields;

        /// <summary>
        ///     the string array of fields used to build the new item name
        /// </summary>
        public string[] NameFields
        {
            get
            {
                if (_NameFields == null)
                {
                    string[] comSplitr = {","};
                    _NameFields = ItemNameDataField.Split(comSplitr, StringSplitOptions.RemoveEmptyEntries);
                }
                return _NameFields;
            }
            set { _NameFields = value; }
        }

        /// <summary>
        ///     max length for item names
        /// </summary>
        public int ItemNameMaxLength { get; set; }

        public Language ImportToLanguage { get; set; }

        private List<IBaseField> _fieldDefinitions = new List<IBaseField>();

        /// <summary>
        ///     the definitions of fields to import
        /// </summary>
        public List<IBaseField> FieldDefinitions
        {
            get { return _fieldDefinitions; }
            set { _fieldDefinitions = value; }
        }

        /// <summary>
        ///     tells whether or not to folder new items by a date
        /// </summary>
        public bool FolderByDate { get; set; }

        /// <summary>
        ///     tells whether or not to folder new items by first letter of their name
        /// </summary>
        public bool FolderByName { get; set; }

        /// <summary>
        ///     the name of the field that stores a date to folder by
        /// </summary>
        public string DateField { get; set; }

        /// <summary>
        ///     the template used to create the folder items
        /// </summary>
        public TemplateItem FolderTemplate { get; set; }

        /// <summary>
        ///     the connection string to the database you're importing from
        /// </summary>
        public string DatabaseConnectionString { get; set; }

        /// <summary>
        ///     the query used to retrieve the data
        /// </summary>
        public string Query { get; set; }

        /// <summary>
        ///     Whether or not the system should create a new version of the item in Sitecore or merely update it
        /// </summary>
        public bool Overwrite { get; set; }

        /// <summary>
        ///     Whether or not the system should skip over items that already are in Sitecore
        /// </summary>
        public bool SkipExistingItems { get; set; }

        public bool SendEmail { get; set; }

        public string SearchIndex { get; set; }

        public bool DeltasOnly { get; set; }

        public string LastUpdatedFieldName { get; set; }

        public DateTime LastUpdated { get; set; }

        public string MissingItemsQuery { get; set; }

        public string HistorySnapshotQuery { get; set; }

        public List<string> NotificationAddressList { get; set; }

        #endregion Properties

        #region Abstract Methods

        /// <summary>
        ///     gets the data to be imported
        /// </summary>
        /// <returns></returns>
        public abstract IEnumerable<object> GetImportData();

        public abstract IEnumerable<object> SyncDeletions();

        public abstract void TakeHistorySnapshot();

        /// <summary>
        ///     this is used to process custom fields or properties
        /// </summary>
        public abstract void ProcessCustomData(ref Item newItem, object importRow);

        /// <summary>
        ///     Defines how the subclass will retrieve a field value
        /// </summary>
        protected abstract string GetFieldValue(object importRow, string fieldName);

        #endregion Abstract Methods

        #region Static Methods

        /// <summary>
        ///     will begin looking for or creating date folders to get a parent node to create the new items in
        /// </summary>
        /// <param name="parentNode">current parent node to create or search folder under</param>
        /// <param name="dt">date time value to folder by</param>
        /// <param name="folderType">folder template type</param>
        /// <returns></returns>
        public static Item GetDateParentNode(Item parentNode, DateTime dt, TemplateItem folderType)
        {
            //get year folder
            var year = parentNode.Children[dt.Year.ToString()];
            if (year == null)
            {
                //build year folder if you have to
                year = parentNode.Add(dt.Year.ToString(), folderType);
            }
            //set the parent to year
            parentNode = year;

            //get month folder
            var month = parentNode.Children[dt.ToString("MM")];
            if (month == null)
            {
                //build month folder if you have to
                month = parentNode.Add(dt.ToString("MM"), folderType);
            }
            //set the parent to year
            parentNode = month;

            //get day folder
            var day = parentNode.Children[dt.ToString("dd")];
            if (day == null)
            {
                //build day folder if you have to
                day = parentNode.Add(dt.ToString("dd"), folderType);
            }
            //set the parent to year
            parentNode = day;

            return parentNode;
        }

        /// <summary>
        ///     will begin looking for or creating letter folders to get a parent node to create the new items in
        /// </summary>
        /// <param name="parentNode">current parent node to create or search folder under</param>
        /// <param name="letter">the letter to folder by</param>
        /// <param name="folderType">folder template type</param>
        /// <returns></returns>
        public static Item GetNameParentNode(Item parentNode, string letter, TemplateItem folderType)
        {
            //get letter folder
            var letterItem = parentNode.Children[letter];
            if (letterItem == null)
            {
                //build year folder if you have to
                letterItem = parentNode.Add(letter, folderType);
            }
            //set the parent to year
            return letterItem;
        }

        #endregion Static Methods

        #region Methods

        /// <summary>
        ///     searches under the parent for an item whose template matches the id provided
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="TemplateID"></param>
        /// <returns></returns>
        protected Item GetItemByTemplate(Item parent, string TemplateID)
        {
            var x = from Item i in parent.GetChildren()
                where i.Template.IsID(TemplateID)
                select i;
            return (x.Any()) ? x.First() : null;
        }

        /// <summary>
        ///     Used to log status information while the import is processed
        /// </summary>
        /// <param name="errorType"></param>
        /// <param name="message"></param>
        protected void Log(string errorType, string message)
        {
            log.AppendFormat("DATA_IMPORTER - {0} : {1}", errorType, message).AppendLine().AppendLine();
        }

        /// <summary>
        ///     processes each field against the data provided by subclasses
        /// </summary>
        public string Process()
        {
            using (new Sitecore.SecurityModel.SecurityDisabler())
            {

                #region process

                IEnumerable<object> importItems;
                var removedItems = Enumerable.Empty<object>();
                try
                {
                    importItems = GetImportData();
                }
                catch (Exception ex)
                {
                    importItems = Enumerable.Empty<object>();
                    Log("Connection Error", ex.Message);
                }

                long line = 0;

                try
                {
                    //Loop through the data source
                    foreach (var importRow in importItems)
                    {
                        line++;

                        var newItemName = GetNewItemName(importRow);
                        if (string.IsNullOrEmpty(newItemName))
                            continue;

                        var thisParent = GetParentNode(importRow, newItemName);
                        if (thisParent.IsNull())
                            throw new NullReferenceException("The new item's parent is null");

                        CreateNewItem(thisParent, importRow, newItemName);
                    }
                }
                catch (Exception ex)
                {
                    Log("Error (line: " + line + ")", ex.Message);
                }

                if (!string.IsNullOrEmpty(MissingItemsQuery))
                {
                    try
                    {
                        removedItems = SyncDeletions();
                        line = 0;
                        //Loop through the data source
                        foreach (var removeRow in removedItems)
                        {
                            line++;
                            var itemName = GetNewItemName(removeRow);
                            if (string.IsNullOrEmpty(itemName))
                                continue;

                            var thisParent = GetParentNode(removeRow, itemName);
                            if (thisParent.IsNull())
                            {
                                throw new NullReferenceException("The new item's parent is null");
                            }

                            RemoveItem(thisParent, removeRow, itemName);
                            Log("Removed Item", itemName + " was removed from Sitecore");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log("SyncDeletions Error (line: " + line + ")", ex.Message);
                    }
                }

                var lineNumber = 0;
                if (!string.IsNullOrEmpty(HistorySnapshotQuery))
                {
                    try
                    {
                        TakeHistorySnapshot();
                    }
                    catch (Exception ex)
                    {
                        Log("TakeHistorySnapshot Error (line: " + lineNumber + ")", ex.Message);
                    }
                }

                //if no messages then you're good
                if (log.Length < 1 || !log.ToString().Contains("Error"))
                    Log("Success", "the import completed successfully");

                if (SendEmail)
                {

                    var mail = new MailMessage {From = new MailAddress("sitecore@meau.com")};
                    foreach (var emailAddress in NotificationAddressList)
                    {
                        mail.To.Add(emailAddress);
                    }
                    mail.Subject = "Updated Portal Materials";
                    mail.Body = log.ToString();
                    try
                    {
                        MainUtil.SendMail(mail);
                    }
                    catch (Exception ex)
                    {
                        Log("Error Sending Email", ex.Message);
                    }
                }
                return log.ToString();

                #endregion


            }
        }

        public void CreateNewItem(Item parent, object importRow, string newItemName)
        {
            var nItemTemplate = GetNewItemTemplate(importRow);
            var updating = false;
            using (new LanguageSwitcher(ImportToLanguage))
            {
                //get the parent in the specific language
                parent = SitecoreDB.GetItem(parent.ID);

                Item newItem = null;
                //search for the child by name
                if (string.IsNullOrEmpty(SearchIndex))
                {
                    var existingItems = parent.Axes.GetDescendants().Where(x => x.Name == newItemName);
                    var firstItem = true;
                    foreach (var existingItem in existingItems)
                    {
                        if (firstItem)
                        {
                            newItem = existingItem;
                        }
                        else
                        {
                            existingItem.Delete();
                            Log("Duplicate Item Found", "Deleting Duplicate Item (matNum: " + existingItem.Name + ")");
                        }

                        firstItem = false;
                    }
                }
                else
                {
                    var index = ContentSearchManager.GetIndex(SearchIndex);
                    using (var context = index.CreateSearchContext())
                    {
                        var query =
                            context.GetQueryable<SearchResultItem>().Where(m => m.Name.Equals(newItemName)).FirstOrDefault(f => f.Name.Equals(newItemName));
                        if (query != null)
                        {
                            newItem = SitecoreDB.GetItem(query.ItemId);
                        }
                   }
                }
                if (newItem != null)
                {
                    updating = true;
                    if (SkipExistingItems)
                        return;
                    if (!Overwrite)
                        newItem = newItem.Versions.AddVersion();
                }

                //if not found then create one
                if (newItem == null)
                {
                    if (nItemTemplate is BranchItem)
                        newItem = parent.Add(newItemName, (BranchItem) nItemTemplate);
                    else
                        newItem = parent.Add(newItemName, (TemplateItem) nItemTemplate);
                }

                if (newItem == null)
                {
                    Log("Null Item", "the new item created was null)");
                    throw new NullReferenceException("the new item created was null");
                }
                using (new EditContext(newItem, true, false))
                {
                    //add in the field mappings
                    foreach (var d in FieldDefinitions)
                    {
                        var values = GetFieldValues(d.GetExistingFieldNames(), importRow);

                        d.FillField(this, ref newItem, String.Join(d.GetFieldValueDelimiter(), values));
                    }

                    //calls the subclass method to handle custom fields and properties
                    ProcessCustomData(ref newItem, importRow);

                    Log("INFO", $"{newItem.Fields["MaterialNumber"].Value} Material {(updating ? "Updated" : "Added")}");
                }
            }
        }

        public void RemoveItem(Item parent, object importRow, string itemName)
        {
            using (new LanguageSwitcher(ImportToLanguage))
            {
                //get the parent in the specific language
                parent = SitecoreDB.GetItem(parent.ID);

                Item item = null;
                //search for the child by name
                if (string.IsNullOrEmpty(SearchIndex))
                {
                    item = parent.Axes.GetDescendants().FirstOrDefault(x => x.Name == itemName);
                }
                else
                {
                    var index = ContentSearchManager.GetIndex(SearchIndex);
                    using (var context = index.CreateSearchContext())
                    {
                        var query =
                            context.GetQueryable<SearchResultItem>().Where(m => m.Name == itemName);
                        if (query.Any())
                        {
                            item = SitecoreDB.GetItem(query.First().ItemId);
                        }
                    }
                }
                if (item != null)
                {
                    item.Recycle();
                }
            }
        }

        public virtual CustomItemBase GetNewItemTemplate(object importRow)
        {
            //Create new item
            if (NewItemTemplate == null || NewItemTemplate.InnerItem.IsNull())
                throw new NullReferenceException("The 'Import To What Template' item is null");
            return NewItemTemplate;
        }

        /// <summary>
        ///     creates an item name based on the name field values in the importRow
        /// </summary>
        public string GetNewItemName(object importRow)
        {
            if (!NameFields.Any())
                throw new NullReferenceException("there are no 'Name' fields specified");

            var strItemName = new StringBuilder();
            foreach (var nameField in NameFields)
            {
                try
                {
                    strItemName.Append(GetFieldValue(importRow, nameField));
                }
                catch (ArgumentException ex)
                {
                    if (string.IsNullOrEmpty(ItemNameDataField))
                        throw new NullReferenceException("the 'Name' field is empty");
                    throw new NullReferenceException(
                        string.Format("the field name: '{0}' does not exist in the import row", nameField));
                }
            }
            return StringUtility.GetNewItemName(strItemName.ToString(), ItemNameMaxLength);
        }

        /// <summary>
        ///     retrieves all the import field values specified
        /// </summary>
        public IEnumerable<string> GetFieldValues(IEnumerable<string> fieldNames, object importRow)
        {
            var list = new List<string>();
            foreach (var f in fieldNames)
            {
                try
                {
                    list.Add(GetFieldValue(importRow, f));
                }
                catch (ArgumentException ex)
                {
                    if (string.IsNullOrEmpty(f))
                        Log("Field Error", "the 'From' field name is empty");
                    else
                        Log("Field Error", string.Format("the field name: '{0}' does not exist in the import row", f));
                }
            }
            return list;
        }

        /// <summary>
        ///     gets the parent of the new item created. will create folders based on name or date if configured to
        /// </summary>
        protected Item GetParentNode(object importRow, string newItemName)
        {
            var thisParent = Parent;
            if (FolderByDate)
            {
                var date = DateTime.Now;
                var dateValue = string.Empty;
                try
                {
                    dateValue = GetFieldValue(importRow, DateField);
                }
                catch (ArgumentException ex)
                {
                    if (string.IsNullOrEmpty(DateField))
                        Log("Field Error", "the date name field is empty");
                    else
                        Log("Field Error",
                            string.Format("the field name: '{0}' does not exist in the import row", DateField));
                }
                if (!string.IsNullOrEmpty(dateValue))
                {
                    if (DateTime.TryParse(dateValue, out date))
                        thisParent = GetDateParentNode(Parent, date, FolderTemplate);
                    else
                        Log("Error", "the date value could not be parsed");
                }
                else
                {
                    Log("Error", "the date value was empty");
                }
            }
            else if (FolderByName)
            {
                thisParent = GetNameParentNode(Parent, newItemName.Substring(0, 1), FolderTemplate);
            }
            return thisParent;
        }

        #endregion Methods
    }
}