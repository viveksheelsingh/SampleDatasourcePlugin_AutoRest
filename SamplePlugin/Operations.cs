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
        public DateTime? StartTime { get; set; }
        public DateTime? CreatedTime { get; set; }
        public DateTime? EndTime { get; set; }
        public DateTime? PurgeTime { get; set; }
        public Error? OpError { get; set; }
        public HttpStatusCode? StatusCode { get; set; }

        public OperationDetails (DateTime createdTime, Response.KindEnum kind)
        {
            CreatedTime = createdTime;
            EndTime = null;
            PurgeTime = null;
            OperationPayload = "custom plugin payload";
            StartTime = createdTime;
            Status = Response.StatusEnum.RunningEnum.ToString();
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
        public static void UpdateOperation(string operationId, DateTime endTime, HttpStatusCode code, Error error)
        {
            OperationDetails opDetails = GetOperation(operationId);
            opDetails.EndTime = endTime;
            opDetails.PurgeTime = endTime.AddHours(gcOffsetInHours);
            opDetails.StatusCode = code;
            opDetails.OpError = error;
            opDetails.Status = Response.StatusEnum.FailedEnum.ToString();

            // Set back in the map
            opMap[operationId] = opDetails;
        }

        /// <summary>
        /// Overload for success case
        /// </summary>
        /// <param name="endTime"></param>
        public static void UpdateOperation(string operationId, DateTime endTime)
        {
            OperationDetails opDetails = GetOperation(operationId);
            opDetails.EndTime = endTime;
            opDetails.PurgeTime = endTime.AddHours(gcOffsetInHours);
            opDetails.Status = Response.StatusEnum.SucceededEnum.ToString();

            // Set back in the map
            opMap[operationId] = opDetails;
        }
    }

}
