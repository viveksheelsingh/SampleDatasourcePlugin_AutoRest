using Microsoft.AzureBackup.DatasourcePlugin.Models;
using System.Net;

namespace SamplePlugin
{
    /// <summary>
    /// Plugin specific LoopBackMetadata class. See https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/224106/Loopback-Context
    /// </summary>
    internal class LoopBackMetadata
    {
        public string Foo { get; set; }
        public string Bar { get; set; }
        public string ErrorCode { get; set; }
    }

    /// <summary>
    /// Plugin specific implementation for maintaning the operations collection
    /// </summary>
    internal class OperationDetails
    {
        public string OperationPayload { get; set; } // plugin specific data
        public string OperationKind { get; set; }
        public string Status { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? CreatedTime { get; set; }
        public DateTime? EndTime { get; set; }
        public DateTime? PurgeTime { get; set; }
        public Error? OpError { get; set; }
        public HttpStatusCode? StatusCode { get; set; }

        public static OperationDetails CreateOperationDetails(DateTime createdTime, Response.KindEnum kind)
        {
            return new OperationDetails()
            {
                CreatedTime = createdTime,
                EndTime = null,
                PurgeTime = null,
                OperationPayload = "custom plugin payload",
                StartTime = createdTime,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.RunningEnum.ToString(),
                OperationKind = kind.ToString(),
                OpError = null,
                StatusCode = null,
            };
        }
    }

    internal static class OperationsMap
    {
        /// <summary>
        /// Plugin specific implementation for maintaning the operations collection.
        /// This sample uses an in-memory dictionary. 
        /// For more details, please refer to: https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/220542/Idempotency-and-crash-scenarios
        /// </summary>
        private static Dictionary<string, OperationDetails> opMap = new Dictionary<string, OperationDetails>();

       
        /// <summary>
        /// Adds a new operation to the operations map.
        /// </summary>
        /// <param name="operationid"></param>
        public  static void AddToOperationsMap(string operationid, OperationDetails opDetails)
        {
            opMap.Add(operationid, opDetails);
        }

        /// <summary>
        /// Get status of a given operation
        /// </summary>
        /// <param name="operationid"></param>
        /// <returns></returns>
        public  static OperationDetails GetOperationDetails(string operationid)
        {
            if (opMap.TryGetValue(operationid, out var opDetails))
            {
                return opDetails;
            }
            else return null;
        }
    }

}
