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

    internal class PluginMetadata
    {
        /// <summary>
        /// e.g. a plugin might want to store the src-dataplane version 
        /// e.g. SQL engine version.
        /// </summary>
        public string Foo { get; set; }
        public string Bar { get; set; }
    }

    /// <summary>
    /// Plugin specific implementation for maintaning the operations collection
    /// </summary>
    internal class OperationDetails
    {
        public string OperationPayload { get; set; } // plugin specific data
        public string OperationKind { get; set; }
        public string Status { get; set; }
        public DateTimeOffset StartTime { get; set; }
        public DateTimeOffset CreatedTime { get; set; }
        public DateTimeOffset EndTime { get; set; }
        public DateTimeOffset PurgeTime { get; set; }
        public Error? OpError { get; set; }
        public HttpStatusCode? StatusCode { get; set; }

        public OperationDetails (DateTimeOffset createdTime, OperationType kind)
        {
            CreatedTime = createdTime;
            //EndTime = null;
            //PurgeTime = null;
            OperationPayload = "custom plugin payload";
            StartTime = createdTime;
            Status = ExecutionStatus.Running.ToString();
            OperationKind = kind.ToString();
            OpError = null;
            StatusCode = HttpStatusCode.OK;
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

        public  static int gcOffsetInHours = 8; // Operations will be purged 8 hours after their completion

        /// <summary>
        /// Adds a new operation to the operations map.
        /// </summary>
        /// <param name="operationid"></param>
        public static void AddOperation(string operationid, OperationDetails opDetails)
        {
            opMap.Add(operationid, opDetails);
        }

        /// <summary>
        /// Get status of a given operation
        /// </summary>
        /// <param name="operationid"></param>
        /// <returns></returns>
        public  static OperationDetails GetOperation(string operationid)
        {
            if (opMap.TryGetValue(operationid, out var opDetails))
            {
                return opDetails;
            }
            else return null;
        }

        /// <summary>
        /// Overload for failed case
        /// </summary>
        /// <param name="operationId"></param>
        /// <param name="endTime"></param>
        /// <param name="code"></param>
        /// <param name="error"></param>
        public static void UpdateOperation(string operationId, DateTimeOffset endTime, HttpStatusCode code, Error error)
        {
            OperationDetails opDetails = GetOperation(operationId);
            opDetails.EndTime = endTime;
            opDetails.PurgeTime = endTime.AddHours(gcOffsetInHours);
            opDetails.StatusCode = code;
            opDetails.OpError = error;
            opDetails.Status = ExecutionStatus.Failed.ToString();

            // Set back in the map
            opMap[operationId] = opDetails;
        }

        /// <summary>
        /// Overload for success case
        /// </summary>
        /// <param name="endTime"></param>
        public static void UpdateOperation(string operationId, DateTimeOffset endTime)
        {
            OperationDetails opDetails = GetOperation(operationId);
            opDetails.EndTime = endTime;
            opDetails.PurgeTime = endTime.AddHours(gcOffsetInHours);
            opDetails.Status = ExecutionStatus.Succeeded.ToString();

            // Set back in the map
            opMap[operationId] = opDetails;
        }
    }

}
