// This is the default URL for triggering event grid function in the local environment.
// http://localhost:7071/admin/extensions/EventGridExtensionConfig?functionName={functionname} 

using System;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Avro.File;
using Avro.Generic;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FunctionEGDWDumper
{

    public static class Function1
    {
        private static readonly string StorageConnectionString = Environment.GetEnvironmentVariable("StorageConnectionString");
        private static readonly string SqlDwConnection = Environment.GetEnvironmentVariable("SqlDwConnection");

        /// <summary>
        /// Use the accompanying .sql script to create this table in the data warehouse
        /// </summary>  
        private const string TableName = "dbo.StoveTempsDemo";

        [FunctionName("EventGridTriggerMigrateData")]
        public static void Run([EventGridTrigger]JObject eventGridEvent, TraceWriter log)
        {
            log.Info("C# EventGrid trigger function processed a request.");
            log.Info(eventGridEvent.ToString(Formatting.Indented));

            try
            {
                // Copy to a static Album instance
                EventGridEHEvent ehEvent = eventGridEvent.ToObject<EventGridEHEvent>();

                // Get the URL from the event that points to the Capture file
                var uri = new Uri(ehEvent.data.fileUrl);

                // Get data from the file and migrate to data warehouse
                Dump(uri);
            }
            catch (Exception e)
            {
                string s = string.Format(CultureInfo.InvariantCulture,
                    "Error processing request. Exception: {0}, Request: {1}", e, eventGridEvent.ToString());
                log.Error(s);
            }
        }

        /// <summary>
        /// Dumps the data from the Avro blob to the data warehouse (DW). 
        /// Before running this, ensure that the DW has the required <see cref="TableName"/> table created.
        /// </summary>   
        private static void Dump(Uri fileUri)
        {
            // Get the blob reference
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var blob = blobClient.GetBlobReferenceFromServer(fileUri);

            using (var dataTable = GetStoveTempMetrics())
            {
                // Parse the Avro File
                using (var avroReader = DataFileReader<GenericRecord>.OpenReader(blob.OpenRead()))
                {
                    while (avroReader.HasNext())
                    {
                        GenericRecord r = avroReader.Next();

                        byte[] body = (byte[])r["Body"];
                        var stoveTempMeasure = DeserializeToStoveTempMeasure(body);

                        // Add the row to in memory table
                        AddStoveMetricsToTable(dataTable, stoveTempMeasure);
                    }
                }

                if (dataTable.Rows.Count > 0)
                {
                    BatchInsert(dataTable);
                    
                }
            }
        }

        /// <summary>
        /// Open connection to data warehouse. Write the parsed data to the table. 
        /// </summary>   
        private static void BatchInsert(DataTable table)
        {
            // Write the data to SQL DW using SqlBulkCopy
            using (var sqlDwConnection = new SqlConnection(SqlDwConnection))
            {
                sqlDwConnection.Open();

                using (var bulkCopy = new SqlBulkCopy(sqlDwConnection))
                {
                    bulkCopy.BulkCopyTimeout = 30;
                    bulkCopy.DestinationTableName = TableName;
                    bulkCopy.WriteToServer(table);
                }
            }
        }

        /// <summary>
        /// Deserialize data and return object with expected properties.
        /// </summary> 
        private static StoveTemps DeserializeToStoveTempMeasure(byte[] body)
        {
            string payload = Encoding.ASCII.GetString(body);
            return JsonConvert.DeserializeObject<StoveTemps>(payload);
        }

        /// <summary>
        /// Define the in-memory table to store the data. The columns match the columns in the .sql script.
        /// </summary>   
        private static DataTable GetStoveTempMetrics()
        {
            var dt = new DataTable();
            dt.Columns.AddRange
            (
                new DataColumn[5]
                {
                    new DataColumn("Published", typeof(string)),
                    new DataColumn("EventName", typeof(string)),
                    new DataColumn("SupplyTemp", typeof(float)),
                    new DataColumn("ReturnTemp", typeof(float)),
                    new DataColumn("ChargeLevel", typeof(float))
                }
            );

            return dt;
        }


        /// <summary>
        /// For each parsed record, add a row to the in-memory table.
        /// </summary>  
        private static void AddStoveMetricsToTable(DataTable table, StoveTemps wtm)
        {
            table.Rows.Add(wtm.timestamp, wtm.eventName, wtm.SupplyTemp, wtm.ReturnTemp, wtm.ChargeLevel);
        }     
        
    }
}
