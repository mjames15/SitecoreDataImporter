using System.Configuration;
using System.IO;
using Sitecore.Data.Fields;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using Sitecore.Reflection;
using Sitecore.SharedSource.DataImporter.Providers;
using Sitecore.Tasks;

namespace Sitecore.SharedSource.DataImporter.ScheduledTasks
{
    public class DataImportTask
    {
        public void RunImport(Item[] itemArray, CommandItem commandItem, ScheduleItem scheduledItem)
        {
            var importItem = itemArray[0];
            var currentDB = Context.ContentDatabase;
            var connectionStringKey = importItem.Fields["Default Connection String"] != null
                ? importItem.Fields["Default Connection String"].Value
                : "EPrise";
            var connectionString = ConfigurationManager.ConnectionStrings[connectionStringKey].ToString();

            //new import
            TextField hc = importItem.Fields["Handler Class"];
            TextField ha = importItem.Fields["Handler Assembly"];
            if (ha != null && !string.IsNullOrEmpty(ha.Value))
            {
                if (hc != null && !string.IsNullOrEmpty(hc.Value))
                {
                    BaseDataMap map = null;
                    try
                    {
                        map =
                            (BaseDataMap)
                                ReflectionUtil.CreateObject(ha.Value, hc.Value,
                                    new object[] { currentDB, connectionString, importItem, scheduledItem.LastRun.ToString() });
                    }
                    catch (FileNotFoundException fnfe)
                    {
                        Log.Info("DataImportTaskError", "the binary specified could not be found");
                    }
                    if (map != null)
                        Log.Info(map.Process(), "DataImportTask");
                    else
                        Log.Info("DataImportTaskError", "the data map provided could not be instantiated");
                }
                else
                {
                    Log.Info("DataImportTaskError", "import handler class is not defined");
                }
            }
            else
            {
                Log.Info("DataImportTaskError", "import handler assembly is not defined");
            }
        }
    }
}