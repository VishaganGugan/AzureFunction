using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Threading.Tasks;


namespace JsonCosmosFucntion
{
    public static class MainFunction
    {
        
        [FunctionName("Function1")]
        public static async Task RunAsync([ServiceBusTrigger("sampletopic", "SampleSubscription", AccessRights.Manage, Connection = "ServicebusConnectionString")]BrokeredMessage mySbMsg, TraceWriter log)
            {
            // URI Of The Cosmos DB Account
            string EndpointUrI = Environment.GetEnvironmentVariable(Constants.FunctionConfiguration.Cosmos_Db_URI);
            // Primary Key of the Cosmos DB Account
            string PrimaryKey = Environment.GetEnvironmentVariable(Constants.FunctionConfiguration.Primary_Key);
            // Database Name inside Cosmos DB
            string DatabaseId = Environment.GetEnvironmentVariable(Constants.FunctionConfiguration.Database_Id);
            // ContainerID
            string ContainerId = Environment.GetEnvironmentVariable(Constants.FunctionConfiguration.Container_Id);
            // ConnectionPolicy connectionPolicy = new ConnectionPolicy { UserAgentSuffix = "samples-net/3" };
            // Creates a client of DocumentClient Class
            DocumentClient client;
            // Converting the Brokered message into Stream
            Stream msgStream = mySbMsg.GetBody<Stream>();
            // Creating an instance of StreamReader of type msgStream
            StreamReader reader = new StreamReader(msgStream);
            // Converting the stream into string
            string queueMessage = reader.ReadToEnd();
            string JsonAsText = string.Empty;

            CloudBlockBlob blockBlob;
            dynamic msg = JsonConvert.DeserializeObject(queueMessage);
            string fileName = msg.FileName;
            var sessionID = msg.SessionId;
            var connectionString = Environment.GetEnvironmentVariable(Constants.FunctionConfiguration.Connection_String);
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            // Create Blob service client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            // Get Container Reference
            CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable(Constants.FunctionConfiguration.Container_Name));
            // Get Blob Reference
            blockBlob = container.GetBlockBlobReference(fileName);
            // Download Blob as Text.
            JsonAsText = blockBlob.DownloadText();
            JObject newJson = JObject.Parse(JsonAsText);






            try
            {
                using (client = new DocumentClient(new Uri(EndpointUrI), PrimaryKey))
                {
                   

                        var collectionLink = UriFactory.CreateDocumentCollectionUri(DatabaseId, ContainerId);
                        Document created = await client.CreateDocumentAsync(collectionLink, newJson);
                    if (created != null)
                    {
                        blockBlob.DeleteIfExists();
                    }
                        

                    

                }
            }

            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                log.Error($"{de.StatusCode} error occurred: {de.Message}, Message: {baseException.Message}");
            }
            catch (System.Exception e)
            {
                Exception baseException = e.GetBaseException();
                log.Error(string.Format("Error: {0}, Message: {1}", e.Message, baseException.Message));
                throw;
            }

            //async Task RunDocumentsDemo()
            //{
            //    await BasicCRUDAsync();

                
            //}

            //async Task BasicCRUDAsync()
            //{
                
            //    var collectionLink = UriFactory.CreateDocumentCollectionUri(databaseId, containerID);
            //    Document created = await client.CreateDocumentAsync(collectionLink, newJson);
            //    Console.WriteLine(created);
                

            //}
            

        }
    }
}
