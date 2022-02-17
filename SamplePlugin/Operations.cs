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
    }

}
