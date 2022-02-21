/*/////////////////////////////////////////////////////////////////////////////////////////
 * Sample controller code.
 * Complete project available at: https://github.com/viveksheelsingh/SampleDatasourcePlugin
 * 
 /////////////////////////////////////////////////////////////////////////////////////////*/

using Microsoft.AspNetCore.Mvc;
using Microsoft.AzureBackup.DatasourcePlugin.Models;
using Microsoft.Internal.AzureBackup.DataProtection.Common.Interface;
using Microsoft.Internal.AzureBackup.DataProtection.Common.PitManager;
using Microsoft.Internal.AzureBackup.DataProtection.PitManagerInterface;
using Microsoft.Internal.CloudBackup.Common.Diag;
using Newtonsoft.Json;
using System.Net;
using Datasource = Microsoft.AzureBackup.DatasourcePlugin.Models.Datasource;
using DatasourceSet = Microsoft.AzureBackup.DatasourcePlugin.Models.DatasourceSet;
using Error = Microsoft.AzureBackup.DatasourcePlugin.Models.Error;
using InnerError = Microsoft.AzureBackup.DatasourcePlugin.Models.InnerError;
using PolicyInfo = Microsoft.Internal.AzureBackup.DataProtection.Common.Interface.PolicyInfo;

namespace SamplePlugin.Controllers
{

    [ApiController]
    public class SamplePluginController : ControllerBase
    {

        private readonly ILogger<SamplePluginController> _logger;
        private readonly string streamName = "testStream";

        public SamplePluginController(ILogger<SamplePluginController> logger)
        {
            _logger = logger;
        }


        #region Plugin Verb Controllers

        /// <summary>
        /// Controller for the ValidateForProtection verb
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:ValidateForProtection")]
        public IActionResult ValidateForProtection(ValidateForProtectionRequest request)
        {
            var createdTime = DateTime.UtcNow;

            try
            {
                // Copy over the request headers and query params.
                Helper.CopyReqHeaderAndQueryParams(Request);
                Helper.AddResponseHeaders(Request, Response);

                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForProtectionEnum);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Get the DS and DSSet details from request
                Datasource ds = request.Datasource;
                DatasourceSet dsSet = request.DatasourceSet;

                // Get the vault MSI token. 
                string vaultMSIMgmtToken = request.DatasourceAccessToken.MgmtPlaneToken;
                string vaultMSIDataplaneToken = request.DatasourceAccessToken.DataPlaneToken;

                // Perform RBAC checks. Must check if the Vault MSI has the right RBAC setup on the datasource
                // https://docs.microsoft.com/en-us/rest/api/authorization/permissions/list-for-resource


                // Now do any validations on the datasource/datasourceSet (specific to each plugin):
                // - Can this datasource be backed up using this policy ?
                // - Is the datasource in a state where it can be backed up ? 
                // - Are any other networking pre-requisites met for backup to succeed later ?
                // - etc.
                // 
                // Use the vaultMSIMgmtToken as the authorizationHeader in an ARM call like: 
                // GET https://management.azure.com/subscriptions/f75d8d8b-6735-4697-82e1-1a7a3ff0d5d4/resourceGroups/viveksipgtest/providers/Microsoft.DBforPostgreSQL/servers/viveksipgtest?api-version=2017-12-01 
                // Use the vaultMSIDataplaneToken as the authorizationHeader in any dataplane calls like:
                // GET https://testContainer.blob.core.windows.net/018a635f-f899-4344-9c19-b81f4455d900
                //
                // If your plugin does not rely on VaultMSI token for internal authN, use that.

                // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
                var response = new Response()
                {
                    Id = Request.Query["operationId"],
                    Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForProtectionEnum,
                    Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum,
                    StartTime = createdTime,
                    CreatedTime = createdTime,
                    EndTime = DateTime.UtcNow,
                    PurgeTime = DateTime.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new ValidateForProtectionStatus()  // Success case, dont need to return anything.
                };
                OperationsMap.UpdateOperation(Request.Query["operationId"], DateTime.UtcNow);

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }

            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForProtectionEnum), Formatting.Indented)); 
            }

        }

        /// <summary>
        /// Controller for the StartProtection verb
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:StartProtection")]
        public IActionResult StartProtection(StartProtectionRequest request)
        {
            var createdTime = DateTime.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StartProtectionEnum);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Repeat the same checks as done in ValidateForProtection.
                // If your source dataplane owns part of the schedules, please seek Help: https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/210451/Getting-Help

                // Perform any operations on your source dataplane here. 
                // e.g. Does your resource reqires toggling some property called: EnabledForBackup ? Set it now


                // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
                var response = new Response()
                {
                    Id = Request.Query["operationId"],
                    Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StartProtectionEnum,
                    Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum,
                    StartTime = createdTime,
                    CreatedTime = createdTime,
                    EndTime = DateTime.UtcNow,
                    PurgeTime = DateTime.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new StartProtectionStatus()  // Success case, dont need to return anything.
                };
                OperationsMap.UpdateOperation(Request.Query["operationId"], DateTime.UtcNow);

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StartProtectionEnum), Formatting.Indented));
            }

        }

        /// <summary>
        /// Controller for the StopProtection verb
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:StopProtection")]
        public IActionResult StopProtection(StopProtectionRequest request)
        {
            var createdTime = DateTime.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StopProtectionEnum);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);


                // Clear any state you toggled in StartProtection
                // e.g. Does your resource reqires toggling some property called: EnabledForBackup ? Unset it now 
                // If your source dataplane owns part of the schedules, please seek Help: https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/210451/Getting-Help

                // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
                var response = new Response()
                {
                    Id = Request.Query["operationId"],
                    Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StopProtectionEnum,
                    Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum,
                    StartTime = createdTime,
                    CreatedTime = createdTime,
                    EndTime = DateTime.UtcNow,
                    PurgeTime = DateTime.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new StopProtectionStatus() // Success case, dont need to return anything.
                };
                OperationsMap.UpdateOperation(Request.Query["operationId"], DateTime.UtcNow);

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StopProtectionEnum), Formatting.Indented));
            }
        }

        /// <summary>
        /// Controller for the ValidateForBackup verb
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:ValidateForBackup")]
        public IActionResult ValidateForBackup(ValidateForBackupRequest request)
        {
            var createdTime = DateTime.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StopProtectionEnum);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Get the DS and DSSet details from request
                Datasource ds = request.Datasource;
                DatasourceSet dsSet = request.DatasourceSet;

                // Get the vault MSI token. 
                string vaultMSIMgmtToken = request.DatasourceAccessToken.MgmtPlaneToken;
                string vaultMSIDataplaneToken = request.DatasourceAccessToken.DataPlaneToken;

                // Perform RBAC checks. Must check if the Vault MSI has the right RBAC setup on the datasource
                // https://docs.microsoft.com/en-us/rest/api/authorization/permissions/list-for-resource


                // Now do any validations on the datasource/datasourceSet (specific to each plugin):
                // - Can this datasource be backed up using this policy ?
                // - Is the datasource in a state where it can be backed up ? 
                // - Are any other networking pre-requisites met for backup to succeed ?
                // - etc.
                //
                // Use the vaultMSIMgmtToken as the authorizationHeader in an ARM call like: 
                // GET https://management.azure.com/subscriptions/f75d8d8b-6735-4697-82e1-1a7a3ff0d5d4/resourceGroups/viveksipgtest/providers/Microsoft.DBforPostgreSQL/servers/viveksipgtest?api-version=2017-12-01 
                // Use the vaultMSIDataplaneToken as the authorizationHeader in any dataplane calls like:
                // GET https://testContainer.blob.core.windows.net/018a635f-f899-4344-9c19-b81f4455d900
                //
                // If your plugin does not rely on VaultMSI token for internal authN, use that.

                // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
                var response = new Response()
                {
                    Id = Request.Query["operationId"],
                    Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForBackupEnum,
                    Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum,
                    StartTime = createdTime,
                    CreatedTime = createdTime,
                    EndTime = DateTime.UtcNow,
                    PurgeTime = DateTime.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new ValidateForBackupStatus()   // Success case, return  loopback context if required. Plugin specific.
                    {
                        LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                        {
                            Foo = "foo",
                            Bar = "bar",
                        })
                    }
                };
                OperationsMap.UpdateOperation(Request.Query["operationId"], DateTime.UtcNow);

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StopProtectionEnum), Formatting.Indented));
            }
        }

        /// <summary>
        /// Controller for the Backup verb
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:Backup")]
        public IActionResult Backup(BackupRequest request)
        {
            var createdTime = DateTime.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.BackupEnum);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Copy over the request headers and query params.
                Helper.CopyReqHeaderAndQueryParams(Request);

                // Dispatch the backup to an async Task
                Task.Run(() =>
                {
                    BackupInternal(request);
                });

                // Backup is typically Long running, so Async completion (LRO Status=Running)
                // The async Task will get completed later, and its terminal status will be available via the Poll GET.
                var response = new Response()
                {
                    Id = Request.Query["operationId"],
                    Kind = (Response.KindEnum?)Enum.Parse(typeof(Response.KindEnum), opDetails.OperationKind),
                    Status = (Response.StatusEnum?)Enum.Parse(typeof(Response.StatusEnum), opDetails.Status),
                    StartTime = opDetails.StartTime,
                    CreatedTime = opDetails.CreatedTime,
                    EndTime = opDetails.EndTime,
                    PurgeTime = opDetails.PurgeTime,
                    RunningResponse = new BackupStatus() // Running case, return  loopback context if required. Plugin specific.
                    {
                        LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                        {
                            Foo = "foo",
                            Bar = "bar",
                        })
                    }
                };

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.BackupEnum), Formatting.Indented));
            }

        }

        /// <summary>
        /// Controller for the Restore verb
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:Restore")]
        public IActionResult Restore(RestoreRequest request)
        {
            var createdTime = DateTime.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.RestoreEnum);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Copy over the request headers and query params.
                Helper.CopyReqHeaderAndQueryParams(Request);

                // Dispatch the restore to an async Task
                Task.Run(() =>
                {
                    RestoreInternal(request);
                });

                // Restore is typically Long running, so Async completion (LRO Status=Running)
                // The async Task will get completed later, and its terminal status will be available via the Poll GET.
                var response = new Response()
                {
                    Id = Request.Query["operationId"],
                    Kind = (Response.KindEnum?)Enum.Parse(typeof(Response.KindEnum), opDetails.OperationKind),
                    Status = (Response.StatusEnum?)Enum.Parse(typeof(Response.StatusEnum), opDetails.Status),
                    StartTime = opDetails.StartTime,
                    CreatedTime = opDetails.CreatedTime,
                    EndTime = opDetails.EndTime,
                    PurgeTime = opDetails.PurgeTime,
                    RunningResponse = new RestoreStatus() // Running case, return  loopback context if required. Plugin specific.
                    {
                        LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                        {
                            Foo = "foo",
                            Bar = "bar",
                        }),
                    }
                };

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.RestoreEnum), Formatting.Indented));
            }

        }

        /// <summary>
        /// Controller for polling:  GET https://pluginBaseUrl/operations/{operationId}
        /// </summary>
        /// <param name="operationId"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("/operations/{operationId}")]
        public IActionResult GetOperationStatus(string operationId)
        {
            Response response = null;
            HttpStatusCode code = HttpStatusCode.OK;

            _logger.LogInformation($"{Request.Headers["x-ms-correlation-request-id"]}  {Request.Headers["subscriptionid"]}   GET operationId:{operationId} ",
               Request.Headers["x-ms-correlation-request-id"],
               Request.Headers["subscriptionid"],
               operationId);

            // Get the operation's details from the OperationsMap
            OperationDetails opDetails = OperationsMap.GetOperation(operationId);
            if (opDetails != null)
            {
                Enum.TryParse(typeof(Response.StatusEnum), opDetails.Status, out var status);
                Enum.TryParse(typeof(Response.KindEnum), opDetails.OperationKind, out var kind);
                switch (status)
                {
                    case Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum:
                        {
                            response = new Response()
                            {
                                Id = operationId,
                                Kind = (Response.KindEnum?)kind,
                                Status = (Response.StatusEnum?)status,
                                StartTime = opDetails.StartTime,
                                CreatedTime = opDetails.CreatedTime,
                                EndTime = opDetails.EndTime,
                                PurgeTime = opDetails.PurgeTime,
                                SucceededResponse = new BackupStatus() // Succeeded case, return  loopback context if required. Plugin specific.
                                {
                                    LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                                    {
                                        Foo = "foo",
                                        Bar = "bar",
                                    })
                                }
                            };

                            code = HttpStatusCode.OK;
                        }
                        break;
                    case Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.RunningEnum:
                        {
                            response = new Response()
                            {
                                Id = operationId,
                                Kind = (Response.KindEnum?)kind,
                                Status = (Response.StatusEnum?)status,
                                StartTime = opDetails.StartTime,
                                CreatedTime = opDetails.CreatedTime,
                                EndTime = opDetails.EndTime,
                                PurgeTime = opDetails.PurgeTime,
                                RunningResponse = new BackupStatus() // Running case, return  loopback context if required. Plugin specific.
                                {
                                    LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                                    {
                                        Foo = "foo",
                                        Bar = "bar",
                                    })
                                }
                            };

                            code = HttpStatusCode.OK;
                        }
                        break;
                    case Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.FailedEnum:
                        {
                            response = new Response()
                            {
                                Id = operationId,
                                Kind = (Response.KindEnum?)kind,
                                Status = (Response.StatusEnum?)status,
                                StartTime = opDetails.StartTime,
                                CreatedTime = opDetails.CreatedTime,
                                EndTime = opDetails.EndTime,
                                PurgeTime = opDetails.PurgeTime,
                                FailedResponse = new BackupStatus() // Failed case, return  loopback context if required. Plugin specific. Add error
                                {
                                    Error = opDetails.OpError,
                                    LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                                    {
                                        Foo = "foo",
                                        Bar = "bar",
                                    })
                                }
                            };

                            code = (HttpStatusCode)opDetails.StatusCode;
                        }
                        break;
                    case Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.CancelledEnum:
                        {
                            response = new Response()
                            {
                                Id = operationId,
                                Kind = (Response.KindEnum?)kind,
                                Status = (Response.StatusEnum?)status,
                                StartTime = opDetails.StartTime,
                                CreatedTime = opDetails.CreatedTime,
                                EndTime = opDetails.EndTime,
                                PurgeTime = opDetails.PurgeTime,
                                CancelledResponse = new BackupStatus() // Cancelled case, return  loopback context if required. Plugin specific. Add error
                                {
                                    Error = opDetails.OpError,
                                    LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                                    {
                                        Foo = "foo",
                                        Bar = "bar",
                                    })
                                }
                            };

                            code = (HttpStatusCode)opDetails.StatusCode;
                        }
                        break;
                }
            }
            else
            {
                // Operation Id not found in the map
                code = HttpStatusCode.NotFound;
                response = new Response()
                {
                    Id = Request.Headers["operationId"],
                    /* Wont set these fields, as opDetails are lost:

                    Kind = ?,
                    Status = ?,
                    StartTime = ?,
                    CreatedTime = ?,
                    EndTime = ?,
                    PurgeTime = ?,
                    */
                    FailedResponse = new BaseStatus() // We dont know the kind, so must return BaseStatus
                    {
                        Error = new Error()
                        {
                            Code = "ErrorOperationIdNotFound"
                        },
                    }
                };
            }

            Response.Headers.Add("x-ms-request-id", Request.Headers["x-ms-correlation-request-id"]);

            // One of the httpStatusCodes set on the Operations + response body
            return StatusCode((int)code, JsonConvert.SerializeObject(response, Formatting.Indented));

        }

        #endregion

        #region InternalMethods
        /// <summary>
        /// Internal long running backup method
        /// </summary>
        /// <param name="request"></param>
        /// <param name="headers"></param>
        /// <param name="qparams"></param>
        private async void BackupInternal(BackupRequest request)
        {
            await Task.Delay(10 * 1000);
            // Get the DS and DSSet details from request
            Datasource ds = request.Datasource;
            DatasourceSet dsSet = request.DatasourceSet;

            // Get the vault MSI token. 
            string vaultMSIMgmtToken = request.DatasourceAccessToken.MgmtPlaneToken;
            string vaultMSIDataplaneToken = request.DatasourceAccessToken.DataPlaneToken;

            // Get the LoopbackContext from ValidateForBackup phase.
            // Its use is plugin specific.
            string loopBackCtx = request.LoopBackContext;
            if (!string.IsNullOrEmpty(loopBackCtx))
            {
                LoopBackMetadata loopbkMetadata = JsonConvert.DeserializeObject<LoopBackMetadata>(loopBackCtx);
            }

            // Now call the Backup API of your source dataplane (specific to each plugin):
            //
            // e.g. call some API in ARM: Use the vaultMSIMgmtToken as the authorizationHeader in an ARM call like: 
            // POST https://management.azure.com/subscriptions/f75d8d8b-6735-4697-82e1-1a7a3ff0d5d4/resourceGroups/viveksipgtest/providers/Microsoft.DBforPostgreSQL/servers/viveksipgtest/Snapshot?api-version=2017-12-01 
            // Use the vaultMSIDataplaneToken as the authorizationHeader in any dataplane calls like:
            // POST https://testContainer.blob.core.windows.net/018a635f-f899-4344-9c19-b81f4455d900/Snapshot  etc...
            //
            // If your plugin does not rely on VaultMSI token for internal authN, use that.

            // Assuming you get a handle to a stream at this point form your native Backup API. 
            // Using MockSourceDataplane here for illustration
            Stream backupStream = MockSourceDataplane.DoBackup();
            byte[] buffer = new byte[MockSourceDataplane.maxReadSize];
            await backupStream.ReadAsync(buffer, 0, MockSourceDataplane.maxReadSize);

            // Get PitMgr
            // Need to pass the headers to PitMgr so that the same can propogate to DPP-Target dataplane
            DiagContextServiceInfo diagCtx = new DiagContextServiceInfo()
            {
                SubscriptionId = new Guid(Helper.headers["subscriptionid"]),
                ResourceId = Helper.headers["resourceid"],
                TaskId = Helper.headers["resourceid"],
                RequestId = Helper.headers["x-ms-correlation-request-id"]
            };

            IPitManager pitMgr = PitManagerFactory.Instance.GetPitManager(request.RPCatalogInitializeParams, 
                request.DatastoreInitializeParams, logger: _logger, diagContext: diagCtx);

            // Create Pit.
            IPit pit = pitMgr.CreatePit(request.RPCatalogInitializeParams[VaultAndStoreInitializationParamsKeys.RecoveryPointId],
                PitFormatType.AzureStorageBlockBlobUnseekableStream,
                BackupType.Full);

            pit.InitializePitFormatWriter();

            StreamPitFormatWriter pitWriter = pit.PitFormatWriter as StreamPitFormatWriter;

            // Add a storage Unit
            pitWriter.AddStorageUnit("TestStorageUnit", 1);

            // Create and write to the stream.
            using (PassthroughStream targetStream = pitWriter.CreateStream(streamName))
            {
                Stream srcStream = MockSourceDataplane.DoBackup();
                byte[] bytes = new byte[MockSourceDataplane.maxReadSize];
                int bytesRead = srcStream.Read(bytes, 0, MockSourceDataplane.maxReadSize);
                targetStream.Write(bytes, 0, bytesRead);
            }

            // Set tags, policyInfo and metadata
            pit.TagInfo = new RetentionTagInfo()
            {
                TagName = "testTag",
                Version = "v1"
            };
            pit.PolicyInfo = new PolicyInfo()
            {
                PolicyName = "testPolicy",
                PolicyVersion = "v1"
            };
            pit.PluginMetadata = JsonConvert.SerializeObject(
                new PluginMetadata()
                {
                    Foo =   "Foo",
                    Bar = "Bar",
                }); 

            pit.EndTime = DateTimeOffset.UtcNow;

            // commit.
            pit.Commit();
            pitWriter.CleanupStorageUnits();

            _logger.LogInformation($"{Helper.headers["x-ms-correlation-request-id"]}  {Helper.headers["subscriptionid"]} Committed the Pit...",
                  Helper.headers["x-ms-correlation-request-id"],
                  Helper.headers["subscriptionid"]);

            // Update the operation
            OperationsMap.UpdateOperation(Helper.qparams["operationId"], DateTime.UtcNow);

            return;
        }

        /// <summary>
        /// Internal long running Restore method.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="headers"></param>
        /// <param name="qparams"></param>
        private async void RestoreInternal(RestoreRequest request)
        {
            // Get the source and target DS and DSSet details from request
            Datasource srcds = request.SourceDatasource;
            DatasourceSet srcDsSet = request.SourceDatasourceSet;
            Datasource tgtds = request.TargetDatasource;
            DatasourceSet tgtDsSet = request.TargetDatasourceSet;

            // Get the vault MSI token. 
            string vaultMSIMgmtToken = request.DatasourceAccessToken.MgmtPlaneToken;
            string vaultMSIDataplaneToken = request.DatasourceAccessToken.DataPlaneToken;

            // Get the LoopbackContext from ValidateForRestore phase.
            // Its use is plugin specific.
            string loopBackCtx = request.LoopBackContext;
            if (!string.IsNullOrEmpty(loopBackCtx))
            {
                LoopBackMetadata loopbkMetadata = JsonConvert.DeserializeObject<LoopBackMetadata>(loopBackCtx);
            }

            // Get PitMgr
            // Need to pass the headers to PitMgr so that the same can propogate to DPP-Target dataplane
            DiagContextServiceInfo diagCtx = new DiagContextServiceInfo()
            {
                SubscriptionId = new Guid(Helper.headers["subscriptionid"]),
                ResourceId = Helper.headers["resourceid"],
                TaskId = Helper.headers["resourceid"],
                RequestId = Helper.headers["x-ms-correlation-request-id"]
            };

            IPitManager pitMgr = PitManagerFactory.Instance.GetPitManager(request.RPCatalogInitializeParams,
                request.DatastoreInitializeParams, logger: _logger, diagContext: diagCtx);
          
            // open the pit in vault
            IPit committedPit = pitMgr.GetPit(request.RestoreToRPId);
           
            // Initialize the PitReader
            committedPit.InitializePitFormatReader();

            StreamPitFormatReader pitReader = committedPit.PitFormatReader as StreamPitFormatReader;

            // Assuming you wrote multiple streams - general case.
            foreach (var streamInfo in pitReader.StreamInfo)
            {
                int index = 0;
            
                foreach (var stream in streamInfo.Item2)
                {
                    // Get the stream - in this case, we created ONE stream named: "testStream"
                    using (PassthroughStream ptStream = pitReader.OpenStream(stream, _logger, index))
                    {
                        // Now call the Restore API of your source dataplane (specific to each plugin):
                        //
                        // e.g. call some API in ARM: Use the vaultMSIMgmtToken as the authorizationHeader in an ARM call like: 
                        // POST https://management.azure.com/subscriptions/f75d8d8b-6735-4697-82e1-1a7a3ff0d5d4/resourceGroups/viveksipgtest/providers/Microsoft.DBforPostgreSQL/servers/viveksipgtest/Restore?api-version=2017-12-01 
                        // Use the vaultMSIDataplaneToken as the authorizationHeader in any dataplane calls like:
                        // POST https://testContainer.blob.core.windows.net/018a635f-f899-4344-9c19-b81f4455d900/Restore  etc...
                        //
                        // If your plugin does not rely on VaultMSI token for internal authN, use that.

                        // Assuming you get a handle to a stream at this point form your native Restore API. 
                        // Using MockSourceDataplane here for illustration - this takes the stream directly.
                        MockSourceDataplane.DoRestore(ptStream);

                        // if the Restore API of your workload needs a buffer, use something like:
                        //byte[] buffer = new byte[MockSourceDataplane.maxReadSize];
                        //int bytesRead = ptStream.Read(buffer, 0, buffer.Length);
                        //string outputStr = Encoding.ASCII.GetString(buffer);

                        _logger.LogInformation($"{Helper.headers["x-ms-correlation-request-id"]}  {Helper.headers["subscriptionid"]} Completed restore...",
                              Helper.headers["x-ms-correlation-request-id"],
                              Helper.headers["subscriptionid"]);
                    }
                }
            }
            pitReader.CleanupStorageUnits();

            // Update the operation
            OperationsMap.UpdateOperation(Helper.qparams["operationId"], DateTime.UtcNow);

            return;
        }


        /// <summary>
        /// Test Internal method for async LRO
        /// </summary>
        private async void TestLROInternal()
        {
            await Task.Delay(4 * 1000);

            _logger.LogInformation($"{Helper.headers["x-ms-correlation-request-id"]}  {Helper.headers["subscriptionid"]} Test LRO completed...",
                Helper.headers["x-ms-correlation-request-id"],
                Helper.headers["subscriptionid"]);

            OperationsMap.UpdateOperation(Helper.qparams["operationId"], DateTime.UtcNow);
        }

        #endregion

        #region TestController
        /// <summary>
        /// Test Controller functionality
        /// </summary>
        public class TestPayload
        {
            public int X { get; set; }
            public int Y { get; set; }
            public TestPayload()
            {
                X = 0;
                Y = 0;
            }
        }
        /// <summary>
        /// Test Controller functionality: GET
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        [Route("/plugin:TestGet")]
        public IActionResult TestGet()
        {
            return Ok("Hello from Sample Plugin: GET !");
        }

        /// <summary>
        /// Test Controller functionality: POST
        /// </summary>
        /// <param name="p"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:TestPost")]

        public IActionResult TestPost(TestPayload p)
        {
            var createdTime = DateTime.UtcNow;

            try
            {
                _logger.LogInformation($"{Request.Headers["x-ms-correlation-request-id"]}  {Request.Headers["subscriptionid"]} Starting TestPost:  {createdTime}",
                    Request.Headers["x-ms-correlation-request-id"],
                    Request.Headers["subscriptionid"],
                    createdTime);

                TestPayload p1 = new TestPayload()
                {
                    X = p.X + 1,
                    Y = p.Y + 1,
                };

                // Add to the Operations Map
                var opDetails = new OperationDetails(DateTime.UtcNow, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.BackupEnum);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Copy over the request headers and query params.
                Helper.CopyReqHeaderAndQueryParams(Request);

                // Dispatch LRO on a async Task
                Task.Run(() =>
                {
                    TestLROInternal();
                });

                // Async completion - send Running response now.
                // The async Task will get completed later, and its terminal status will be available via the Poll GET.
                var response = new Response()
                {
                    Id = Request.Query["operationId"],
                    Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.BackupEnum,
                    Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.RunningEnum,
                    StartTime = createdTime,
                    CreatedTime = createdTime,
                    EndTime = null,
                    PurgeTime = null,
                    RunningResponse = new BackupStatus() // Running case, return LoopBack ctx
                    {
                        LoopBackContext = JsonConvert.SerializeObject(p1),
                    },
                };

                Response.Headers.Add("x-ms-request-id", Request.Headers["x-ms-correlation-request-id"]);
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {

                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForProtectionEnum), Formatting.Indented));
            }
        }
        #endregion
    }
}