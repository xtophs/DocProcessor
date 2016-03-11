using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;

namespace DocProcessor
{

    public class BlobInformation
    {
        public string BlobName
        {
            get; set;
        }

        public string ContainerName { get; set;  }
        public long BlobSize { get; set; }
    }

    public class Functions
    {
        // This function will get triggered/executed when a new message is written 
        // on an Azure Queue called queue.
        public static void ProcessNewFileQueueMessage([QueueTrigger("newfile-queue")] BlobInformation message,
                        [Blob("{ContainerName}/{BlobName}", FileAccess.Read)] Stream input,
                        [Queue("smallfile-queue")] CloudQueue smallQueue,
                        [Queue("largefile-queue")] CloudQueue largeQueue,
            TextWriter log)
        {
            try
            {
                Console.WriteLine("NewFileQueue: {0}/{1}", message.ContainerName, message.BlobName);

                // Here you would crack open the blob and extract meta data

                // Look at file size and put another processing message into the right queue
                var ser = new JsonSerializer();
                var sw = new StringWriter();
                ser.Serialize(sw, new BlobInformation { BlobName = message.BlobName, BlobSize = input.Length });

                if (input.Length > 20000)
                {
                    largeQueue.AddMessage(new CloudQueueMessage(sw.ToString()));
                }
                else
                {
                    smallQueue.AddMessage(new CloudQueueMessage(sw.ToString()));
                }
            }
            catch( Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw ex;
            }
        }

        public static void ProcessLargeFileQueueMessage([QueueTrigger("largefile-queue")]  BlobInformation msg,
            [Blob("files/{BlobName}", FileAccess.Read)] CloudBlockBlob fileBlob,
            [Blob("outbox/{BlobName}", FileAccess.Write)] Stream outputBlob)
        {
            try
            {
                Console.WriteLine("Processing SMALL LARGE {0} of size {1}", msg.BlobName, msg.BlobSize);

                using (var inputStream = fileBlob.OpenRead())
                {
                    inputStream.CopyTo(outputBlob, 4096);
                    inputStream.Close();
                }

                fileBlob.Delete();
            }
            catch( Exception ex )
            {
                Console.WriteLine(ex.ToString());   
                throw ex;
            }
        }

        public static void ProcessSmallFileQueueMessage([QueueTrigger("smallfile-queue")]  BlobInformation msg,          
            [Blob("files/{BlobName}", FileAccess.ReadWrite)] CloudBlockBlob fileBlob,
            [Blob("outbox/{BlobName}", FileAccess.Write)] Stream outputBlob)
        {
            try
            {
                Debug.WriteLine("Processing Small File");
                Console.WriteLine("Processing SMALL blob {0} of size {1}", msg.BlobName, msg.BlobSize);
                
                using (var inputStream = fileBlob.OpenRead())
                {
                    inputStream.CopyTo(outputBlob, 4096);
                    inputStream.Close();
                }

                fileBlob.Delete();
            }
            catch( Exception ex )
            {
                Debug.WriteLine(ex.ToString());
                Console.WriteLine(ex.ToString());
                throw ex;
            }
        }
        public static void ProcessNewFileArrived([BlobTrigger("files/{name}")] Stream input,
            string name, [Queue("newfile-queue")] out BlobInformation message)
        {
            try
            {
                Console.WriteLine(string.Format("Received {0} from {1}", name ));


                message = new BlobInformation() { BlobName = name, ContainerName = "files", BlobSize = 0 };
            }
            catch( Exception ex )
            {
                Console.WriteLine(ex.ToString());
                throw ex;
            }
        }
    }
}
