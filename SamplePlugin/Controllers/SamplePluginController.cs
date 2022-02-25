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
using System.Text;
using Datasource = Microsoft.AzureBackup.DatasourcePlugin.Models.Datasource;
using DatasourceSet = Microsoft.AzureBackup.DatasourcePlugin.Models.DatasourceSet;
using Error = Microsoft.AzureBackup.DatasourcePlugin.Models.Error;
using ExecutionStatus = Microsoft.AzureBackup.DatasourcePlugin.Models.ExecutionStatus;
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
            var createdTime = DateTimeOffset.UtcNow;

            try
            {
                // Copy over the request headers and query params.
                Helper.CopyReqHeaderAndQueryParams(Request);
                Helper.AddResponseHeaders(Request, Response);

                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, OperationType.ValidateForProtection);
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
                var response = new Response(Request.Query["operationId"], OperationType.ValidateForProtection, ExecutionStatus.Succeeded, createdTime)
                {
                    StartTime = createdTime,
                    EndTime = DateTimeOffset.UtcNow,
                    PurgeTime = DateTimeOffset.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new ValidateForProtectionStatus()  // Success case, dont need to return anything.
                };
                OperationsMap.UpdateOperation(Request.Query["operationId"], DateTimeOffset.UtcNow);

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }

            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.ValidateForProtection), Formatting.Indented)); 
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
            var createdTime = DateTimeOffset.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, OperationType.StartProtection);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Repeat the same checks as done in ValidateForProtection.
                // If your source dataplane owns part of the schedules, please seek Help:
                // https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/226201/Getting-Help

                // Perform any operations on your source dataplane here. 
                // e.g. Does your resource reqires toggling some property called: EnabledForBackup ? Set it now


                // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
                var response = new Response(Request.Query["operationId"], OperationType.StartProtection, ExecutionStatus.Succeeded, createdTime)
                {
                    StartTime = createdTime,
                    EndTime = DateTimeOffset.UtcNow,
                    PurgeTime = DateTimeOffset.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new StartProtectionStatus()  // Success case, dont need to return anything.
                };
                OperationsMap.UpdateOperation(Request.Query["operationId"], DateTimeOffset.UtcNow);

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.StartProtection), Formatting.Indented));
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
            var createdTime = DateTimeOffset.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, OperationType.StopProtection);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);


                // Clear any state you toggled in StartProtection
                // e.g. Does your resource reqires toggling some property called: EnabledForBackup ? Unset it now 
                // If your source dataplane owns part of the schedules, please seek Help:
                // https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/226201/Getting-Help

                // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
                var response = new Response(Request.Query["operationId"], OperationType.StopProtection, ExecutionStatus.Succeeded, createdTime)
                {
                    StartTime = createdTime,
                    EndTime = DateTimeOffset.UtcNow,
                    PurgeTime = DateTimeOffset.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new StopProtectionStatus() // Success case, dont need to return anything.
                };
                OperationsMap.UpdateOperation(Request.Query["operationId"], DateTimeOffset.UtcNow);

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.StopProtection), Formatting.Indented));
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
            var createdTime = DateTimeOffset.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, OperationType.StopProtection);
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
                var response = new Response(Request.Query["operationId"], OperationType.ValidateForBackup, ExecutionStatus.Succeeded, createdTime)
                {
                    StartTime = createdTime,
                    EndTime = DateTimeOffset.UtcNow,
                    PurgeTime = DateTimeOffset.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new ValidateForBackupStatus()   // Success case, return  loopback context if required. Plugin specific.
                    {
                        LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                        {
                            Foo = "foo",
                            Bar = "bar",
                        })
                    }
                };
                OperationsMap.UpdateOperation(Request.Query["operationId"], DateTimeOffset.UtcNow);

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.StopProtection), Formatting.Indented));
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
            var createdTime = DateTimeOffset.UtcNow;
            /*
            byte[] buffer = new byte[20 * 1024];
            int bytesRead = Request.Body.ReadAsync(buffer, 0, buffer.Length).Result;
            string outputStr = Encoding.ASCII.GetString(buffer);
            BackupRequest request = JsonConvert.DeserializeObject<BackupRequest>(outputStr);
            */
            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, OperationType.Backup);
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
                var response = new Response(Request.Query["operationId"], opDetails.OperationKind, opDetails.Status, opDetails.CreatedTime)
                {
                    StartTime = opDetails.StartTime,
                    EndTime = opDetails.EndTime,
                    PurgeTime = opDetails.PurgeTime,
                    RunningResponse = new BackupStatus() // Running case, return  loopback context if required. Plugin specific.
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
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.Backup), Formatting.Indented));
            }

        }

        /// <summary>
        /// Controller for the Backup verb
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:CommitOrRollbackBackup")]
        public IActionResult CommitOrRollbackBackup(CommitorRollbackBackupRequest request)
        {
            var createdTime = DateTimeOffset.UtcNow;
            bool rollBack = false;
            
            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, OperationType.CommitOrRollbackBackup);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Copy over the request headers and query params.
                Helper.CopyReqHeaderAndQueryParams(Request);

                // Whether to commit or rollback ? Use loopback from previous stage to determine
                // Its use is plugin specific.
                string loopBackCtx = request.LoopBackContext;
                if (!string.IsNullOrEmpty(loopBackCtx))
                {
                    LoopBackMetadata loopbkMetadata = JsonConvert.DeserializeObject<LoopBackMetadata>(loopBackCtx);
                    // previous phase is reporting an error, so rollback.
                    rollBack = !string.IsNullOrEmpty(loopbkMetadata.ErrorCode);
                }

                if (rollBack)
                {
                    // Do rollback actions on SourceDataplane, if required.
                    // pit has been committed (or not) in the backu pphase itself.
                }
                else 
                {
                    // Do commit actions on SourceDataplane, if required.
                    // pit has been committed (or not) in the backu pphase itself.
                }

                // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
                var response = new Response(Request.Query["operationId"], opDetails.OperationKind, ExecutionStatus.Succeeded, opDetails.CreatedTime)
                {
                    StartTime = opDetails.StartTime,
                    EndTime = opDetails.EndTime,
                    PurgeTime = DateTimeOffset.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new CommitOrRollbackBackupStatus() 
                    {
                        // plugin may loopback this info from backup phase if it cannot get it from the source-dataplane now
                        DatasourceSizeInBytes = 1024,
                        DataTransferredInBytes = 1090,
                    },
                };

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.Backup), Formatting.Indented));
            }

        }

        /// <summary>
        /// Controller for the Restore verb
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:ValidateForRestore")]
        public IActionResult ValidateForRestore(ValidateForRestoreRequest request)
        {
            var createdTime = DateTimeOffset.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, OperationType.ValidateForRestore);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Get the DS and DSSet details from request
                Datasource srcDs = request.SourceDatasource;
                DatasourceSet srcDsSet = request.SourceDatasourceSet;
                Datasource tgtDs = request.TargetDatasource;
                DatasourceSet tgtDsSet = request.TargetDatasourceSet;

                // Get the vault MSI token. 
                string vaultMSIMgmtToken = request.DatasourceAccessToken.MgmtPlaneToken;
                string vaultMSIDataplaneToken = request.DatasourceAccessToken.DataPlaneToken;

                // Perform RBAC checks. Must check if the Vault MSI has the right RBAC setup on the target-datasource
                // https://docs.microsoft.com/en-us/rest/api/authorization/permissions/list-for-resource


                // Now do any validations on the datasource/datasourceSet (specific to each plugin):
                // - Can this datasource be restored with the params in this request ?
                // - Is the datasource in a state where it can be restored ? 
                // - Are any other networking pre-requisites met for restore to succeed ?
                // - etc.
                //
                // Use the vaultMSIMgmtToken as the authorizationHeader in an ARM call like: 
                // GET https://management.azure.com/subscriptions/f75d8d8b-6735-4697-82e1-1a7a3ff0d5d4/resourceGroups/viveksipgtest/providers/Microsoft.DBforPostgreSQL/servers/viveksipgtest?api-version=2017-12-01 
                // Use the vaultMSIDataplaneToken as the authorizationHeader in any dataplane calls like:
                // GET https://testContainer.blob.core.windows.net/018a635f-f899-4344-9c19-b81f4455d900
                //
                // If your plugin does not rely on VaultMSI token for internal authN, use that.

                // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
                var response = new Response(Request.Query["operationId"], OperationType.ValidateForRestore, ExecutionStatus.Succeeded, createdTime)
                {
                    StartTime = createdTime,
                    EndTime = DateTimeOffset.UtcNow,
                    PurgeTime = DateTimeOffset.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new ValidateForRestoreStatus()   // Success case, return  loopback context if required. Plugin specific.
                    {
                        LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                        {
                            Foo = "foo",
                            Bar = "bar",
                        })
                    }
                };
                OperationsMap.UpdateOperation(Request.Query["operationId"], DateTimeOffset.UtcNow);

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.ValidateForRestore), Formatting.Indented));
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
            var createdTime = DateTimeOffset.UtcNow;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, OperationType.Restore);
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
                var response = new Response(Request.Query["operationId"], (OperationType)Enum.Parse(typeof(OperationType), opDetails.OperationKind),
                    (ExecutionStatus)Enum.Parse(typeof(ExecutionStatus), opDetails.Status), opDetails.CreatedTime)
                {
                    StartTime = opDetails.StartTime,
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
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.Restore), Formatting.Indented));
            }

        }

        /// <summary>
        /// Controller for the CommitOrRollbackRestore verb
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("/plugin:CommitOrRollbackRestore")]
        public IActionResult CommitOrRollbackRestore(CommitOrRollbackRestoreRequest request)
        {
            var createdTime = DateTimeOffset.UtcNow;
            bool rollBack = false;

            try
            {
                // Add to the Operations Map
                var opDetails = new OperationDetails(createdTime, OperationType.CommitOrRollbackRestore);
                OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

                // Copy over the request headers and query params.
                Helper.CopyReqHeaderAndQueryParams(Request);

                // Whether to commit or rollback ? Use loopback from previous stage to determine
                // Its use is plugin specific.
                string loopBackCtx = request.LoopBackContext;
                if (!string.IsNullOrEmpty(loopBackCtx))
                {
                    LoopBackMetadata loopbkMetadata = JsonConvert.DeserializeObject<LoopBackMetadata>(loopBackCtx);
                    // previous phase is reporting an error, so rollback.
                    rollBack = !string.IsNullOrEmpty(loopbkMetadata.ErrorCode);
                }

                if (rollBack)
                {
                    // Do rollback actions on SourceDataplane, if required.
                }
                else
                {
                    // Do commit actions on SourceDataplane, if required.
                }

                // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
                var response = new Response(Request.Query["operationId"], opDetails.OperationKind, ExecutionStatus.Succeeded, opDetails.CreatedTime)
                {
                    StartTime = opDetails.StartTime,
                    EndTime = opDetails.EndTime,
                    PurgeTime = DateTimeOffset.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                    SucceededResponse = new CommitOrRollbackRestoreStatus()
                    {
                        // plugin may loopback this info from backup phase if it cannot get it from the source-dataplane now
                        DataTransferredInBytes = 1090,
                        OriginalDatasourceSizeInBytes = 1024 // this may be queries from the pit-metadata
                    },
                };

                // 202 + response body
                return StatusCode((int)HttpStatusCode.Accepted, JsonConvert.SerializeObject(response, Formatting.Indented));
            }
            catch (Exception ex)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.Backup), Formatting.Indented));
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
                //Enum.TryParse(typeof(ExecutionStatus), opDetails.Status, out var status);
                //Enum.TryParse(typeof(OperationType), opDetails.OperationKind, out var kind);
                var status = opDetails.Status;
                var kind = opDetails.OperationKind;

                if ((ExecutionStatus)status == ExecutionStatus.Succeeded)
                {
                    response = new Response(operationId, (OperationType)kind, (ExecutionStatus)status, opDetails.CreatedTime)
                    {
                        StartTime = opDetails.StartTime,
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
 
                else if ((ExecutionStatus)status ==  ExecutionStatus.Running)
                {
                    response = new Response(operationId, (OperationType)kind, (ExecutionStatus)status, opDetails.CreatedTime)
                    {
                        StartTime = opDetails.StartTime,
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

                else if ((ExecutionStatus)status ==  ExecutionStatus.Failed)
                {
                    response = new Response(operationId, (OperationType)kind, (ExecutionStatus)status, opDetails.CreatedTime)
                    {
                        StartTime = opDetails.StartTime,
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

                else if ((ExecutionStatus)status == ExecutionStatus.Cancelled)
                {
                    response = new Response(operationId, (OperationType)kind, (ExecutionStatus)status, opDetails.CreatedTime)
                    {
                        StartTime = opDetails.StartTime,
                        EndTime = opDetails.EndTime,
                        PurgeTime = opDetails.PurgeTime,
                        CanceledResponse = new BackupStatus() // Cancelled case, return  loopback context if required. Plugin specific. Add error
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
            }
            else
            {
                // Operation Id not found in the map
                code = HttpStatusCode.NotFound;
                response = new Response(operationId, OperationType.Backup, ExecutionStatus.NotStarted, DateTimeOffset.MinValue)
                {
                    /* Wont set these fields, as opDetails are lost:
                
                    Kind = ?,
                    Status = ?,
                    StartTime = ?,
                    CreatedTime = ?,
                    EndTime = ?,
                    PurgeTime = ?,
                    
                    StartTime = DateTimeOffset.MinValue,
                    CreatedTime = DateTimeOffset.MinValue,
                    PurgeTime = DateTimeOffset.MinValue,
                    EndTime = DateTimeOffset.MinValue,    
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
            OperationsMap.UpdateOperation(Helper.qparams["operationId"], DateTimeOffset.UtcNow);

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
            Datasource srcDs = request.SourceDatasource;
            DatasourceSet srcDsSet = request.SourceDatasourceSet;
            Datasource tgtDs = request.TargetDatasource;
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
            OperationsMap.UpdateOperation(Helper.qparams["operationId"], DateTimeOffset.UtcNow);

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

            OperationsMap.UpdateOperation(Helper.qparams["operationId"], DateTimeOffset.UtcNow);
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
            var createdTime = DateTimeOffset.UtcNow;

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
                var opDetails = new OperationDetails(DateTimeOffset.UtcNow, OperationType.Backup);
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
                var response = new Response(Request.Query["operationId"], OperationType.Backup, ExecutionStatus.Running, createdTime)
                {
                    StartTime = createdTime,
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

                return StatusCode((int)HttpStatusCode.InternalServerError, JsonConvert.SerializeObject(Helper.FormErrorResponse(ex, createdTime, OperationType.ValidateForProtection), Formatting.Indented));
            }
        }
        #endregion
    }
}