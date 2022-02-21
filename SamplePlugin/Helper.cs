using Microsoft.AspNetCore.Mvc;
using Microsoft.AzureBackup.DatasourcePlugin.Models;
using Newtonsoft.Json;
using System.Net;
using static Microsoft.AzureBackup.DatasourcePlugin.Models.Response;

namespace SamplePlugin
{
    public static class Helper
    {
        public static Dictionary <string, string> headers = new Dictionary<string, string>();
        public static Dictionary <string, string> qparams = new Dictionary<string, string>();
        public static void CopyReqHeaderAndQueryParams(HttpRequest Request)
        {
            foreach (var h in Request.Headers)
            {
                headers[h.Key] = h.Value;
            }
            foreach (var q in Request.Query)
            {
                qparams[q.Key] = q.Value;
            }
        }

        public static void AddResponseHeaders(HttpRequest request, HttpResponse response)
        {
            response.Headers.Add("x-ms-request-id", request.Headers["x-ms-correlation-request-id"]);
        }

        public static Response FormErrorResponse(Exception ex, DateTime createdTime, KindEnum kind)
        {
            Error err = new Error()
            {
                Code = ex.HResult.ToString(),  // TODO: Give json error resource guidance
                Message = ex.Message,
                RecommendedAction = "Some recommended action",
                InnerError = new InnerError()
                {
                    Code = "innerErrorCode",  // This will be a code that comes from your source-dataplane
                    AdditionalInfo = new Dictionary<string, string>()
                        {
                            // This message will also come from source data plane. Fill ths if you want to show this on the Portal.
                            // e.g. https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/210788/Error-Modelling
                            { "DetailedNonLocalisedMessage", "セッションはすでに存在します：StorageUnit6000C298-c804-c37e-9f9d-dc8ae1f5ef89_0にはまだ前のセッションがあります99892eb3bfbc4a718234f9be80676f06アクティブ" }
                        }
                },
            };

            var errResponse = new Response()
            {
                Id = qparams["operationId"],
                Kind = kind,
                Status = StatusEnum.FailedEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = DateTime.UtcNow,
                PurgeTime = DateTime.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                FailedResponse = FormFailedStatus(kind, err),
            };
            OperationsMap.UpdateOperation(qparams["operationId"], DateTime.UtcNow, HttpStatusCode.InternalServerError, err);

            return errResponse;
        }

        private static BaseStatus FormFailedStatus(KindEnum kind, Error err)
        {
            switch (kind)
            {
                case KindEnum.ValidateForProtectionEnum:
                    {
                        return new ValidateForProtectionStatus()
                        {
                            Error = err,
                        };
                    }
                    break;

                // other cases for other verbs...
                // case KindEnum.StartProtectionEnum: 
                // etc.

                default:
                    {
                        return new BaseStatus()
                        {
                            Error = err
                        };
                    }
            }
        }
    }
}
