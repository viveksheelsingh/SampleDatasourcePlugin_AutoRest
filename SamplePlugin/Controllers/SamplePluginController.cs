using Microsoft.AspNetCore.Mvc;
using Microsoft.AzureBackup.DatasourcePlugin.Models;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;
using System.Net;
using System.Net.Http;

namespace SamplePlugin.Controllers
{
    
    [ApiController]
    public class SamplePluginController : ControllerBase
    {
        private int gcOffsetInHours = 8; // Operations will be purged 8 hours after their completion
        private readonly ILogger<SamplePluginController> _logger;

        /// <summary>
        /// Plugin specific implementation for maintaning the operations collection.
        /// This sample uses an in-memory dictionary. 
        /// For more details, please refer to: https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/220542/Idempotency-and-crash-scenarios
        /// </summary>
        internal Dictionary<string, OperationDetails> OperationsMap = new Dictionary<string, OperationDetails>();

        public SamplePluginController(ILogger<SamplePluginController> logger)
        {
            _logger = logger;
        }

        #region TestController
        /// <summary>
        /// Test Controller functionality
        /// </summary>
        public class Payload
        {
            public int X { get; set; }
            public int Y { get; set; }
            public Payload()
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

        public IActionResult TestPost(Payload p)
        {
            // Get the DS and DSSet details from request
            return Content("Hello from Sample Plugin: POST!" + p.X + p.Y);
            // return StatusCode(401, "Hello from Sample Plugin: POST!" + p.X + p.Y);
        }
        #endregion

        #region Plugin Verb Controllers

        [HttpPost]
        [Route("/plugin:ValidateForProtection")]
        public IActionResult ValidateForProtection(ValidateForProtectionRequest request)
        {
            var createdTime = DateTime.UtcNow;
            
            // Add to the Operations Map
            var opDetails = CreateOperationDetails(createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForProtectionEnum);
            AddToOperationsMap(Request.Query["operationId"], opDetails);

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
                Id = Request.Headers["operationId"],
                Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForProtectionEnum,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = DateTime.UtcNow,
                PurgeTime = DateTime.UtcNow.AddHours(gcOffsetInHours),
                SucceededResponse = new ValidateForProtectionStatus() // Success case, dont need to return anything.
            };

            return Accepted(response);

            // Error case - similar response is to be created for other Verbs.
            /*
            var errResponse = new Response()
            {
                Id = Request.Headers["operationId"],
                Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForProtectionEnum,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.FailedEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = DateTime.UtcNow,
                PurgeTime = DateTime.UtcNow.AddHours(gcOffsetInHours),
                SucceededResponse = new ValidateForProtectionStatus()
                {
                    Error = new Error()
                    {
                        Code = "erroCode",  // TODO: Give json error resource guidance
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
                    },
                },
            };

            return StatusCode((int)HttpStatusCode.InternalServerError, errResponse);
            */
        }


        [HttpPost]
        [Route("/plugin:StartProtection")]
        public IActionResult StartProtection(StartProtectionRequest request)
        {
            var createdTime = DateTime.UtcNow;

            // Repeat the same checks as done in ValidateForProtection.
            // If your source dataplane owns part of the schedules, please seek Help: https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/210451/Getting-Help

            // Perform any operations on your source dataplane here. 
            // e.g. Does your resource reqires toggling some property called: EnabledForBackup ? Set it now


            // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
            var response = new Response()
            {
                Id = Request.Headers["operationId"],
                Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StartProtectionEnum,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = DateTime.UtcNow,
                PurgeTime = DateTime.UtcNow.AddHours(gcOffsetInHours),
                SucceededResponse = new StartProtectionStatus()  // Success case, dont need to return anything.
            };

            return Accepted(response);
        }

        [HttpPost]
        [Route("/plugin:StopProtection")]
        public IActionResult StopProtection(StopProtectionRequest request)
        {
            var createdTime = DateTime.UtcNow;

            // Clear any state you toggled in StartProtection
            // e.g. Does your resource reqires toggling some property called: EnabledForBackup ? Unset it now 
            // If your source dataplane owns part of the schedules, please seek Help: https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/210451/Getting-Help

            // Success case - Syncronous completion (LRO reached terminal state - Status=Succeeded)
            var response = new Response()
            {
                Id = Request.Headers["operationId"],
                Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StopProtectionEnum,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = DateTime.UtcNow,
                PurgeTime = DateTime.UtcNow.AddHours(gcOffsetInHours),
                SucceededResponse = new StopProtectionStatus() // Success case, dont need to return anything.
            };

            return Accepted(response);
        }

        [HttpPost]
        [Route("/plugin:ValidateForBackup")]
        public IActionResult ValidateForBackup(ValidateForBackupRequest request)
        {
            var createdTime = DateTime.UtcNow;

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
                Id = Request.Headers["operationId"],
                Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForBackupEnum,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = DateTime.UtcNow,
                PurgeTime = DateTime.UtcNow.AddHours(gcOffsetInHours),
                SucceededResponse = new ValidateForBackupStatus()   // Success case, return  loopback context if required. Plugin specific.
                {
                    LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                    {
                        Foo = "foo",
                        Bar = "bar",
                    })
                }
            };

            return Accepted(response);
        }

        [HttpPost]
        [Route("/plugin:Backup")]
        public IActionResult Backup(BackupRequest request)
        {
            var createdTime = DateTime.UtcNow;

            // Get the DS and DSSet details from request
            Datasource ds = request.Datasource;
            DatasourceSet dsSet = request.DatasourceSet;

            // Get the vault MSI token. 
            string vaultMSIMgmtToken = request.DatasourceAccessToken.MgmtPlaneToken;
            string vaultMSIDataplaneToken = request.DatasourceAccessToken.DataPlaneToken;

            // Get the LoopbackContext from ValidateForBackup phase.
            // Its use is plugin specific.
            string loopBackCtx = request.LoopBackContext;

            // Now call the Backup API of your source dataplane (specific to each plugin):
            //
            // Use the vaultMSIMgmtToken as the authorizationHeader in an ARM call like: 
            // GET https://management.azure.com/subscriptions/f75d8d8b-6735-4697-82e1-1a7a3ff0d5d4/resourceGroups/viveksipgtest/providers/Microsoft.DBforPostgreSQL/servers/viveksipgtest?api-version=2017-12-01 
            // Use the vaultMSIDataplaneToken as the authorizationHeader in any dataplane calls like:
            // GET https://testContainer.blob.core.windows.net/018a635f-f899-4344-9c19-b81f4455d900
            //
            // If your plugin does not rely on VaultMSI token for internal authN, use that.

            // Assuming you get a handle to a stream at this point form your native Backup API.

            // Success case - Backup is typically Long running, so Async completion (LRO Status=Running)
            var response = new Response()
            {
                Id = Request.Headers["operationId"],
                Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.BackupEnum,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.RunningEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = null,
                PurgeTime = null,
                RunningResponse = new BackupStatus() // Running case, return  loopback context if required. Plugin specific.
                {
                    LoopBackContext = JsonConvert.SerializeObject(new LoopBackMetadata()
                    {
                        Foo = "foo",
                        Bar = "bar",
                    })
                }
            };

            return Accepted(response);
        }

        [HttpGet]
        [Route("/operations/{operationId}")]
        public IActionResult GetOperationStatus(string operationId)
        {
            Response response = null;
            HttpStatusCode code = HttpStatusCode.OK;

            // Get the operation's details from the OperationsMap
            OperationDetails opDetails = GetOperationDetails(operationId);
            Enum.TryParse(typeof(Response.StatusEnum), opDetails.Status, out var status);
            Enum.TryParse(typeof(Response.KindEnum), opDetails.OperationKind, out var kind);
            switch (status)
            {
                case Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum:
                    {
                        response = new Response()
                        {
                            Id = Request.Headers["operationId"],
                            Kind = (Response.KindEnum?)kind,
                            Status = (Response.StatusEnum?)status,
                            StartTime = opDetails.StartTime,
                            CreatedTime = opDetails.CreatedTime,
                            EndTime = opDetails.EndTime,
                            PurgeTime = opDetails.EndTime,
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
                            Id = Request.Headers["operationId"],
                            Kind = (Response.KindEnum?)kind,
                            Status = (Response.StatusEnum?)status,
                            StartTime = opDetails.StartTime,
                            CreatedTime = opDetails.CreatedTime,
                            EndTime = opDetails.EndTime,
                            PurgeTime = opDetails.EndTime,
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
                            Id = Request.Headers["operationId"],
                            Kind = (Response.KindEnum?)kind,
                            Status = (Response.StatusEnum?)status,
                            StartTime = opDetails.StartTime,
                            CreatedTime = opDetails.CreatedTime,
                            EndTime = opDetails.EndTime,
                            PurgeTime = opDetails.EndTime,
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
                            Id = Request.Headers["operationId"],
                            Kind = (Response.KindEnum?)kind,
                            Status = (Response.StatusEnum?)status,
                            StartTime = opDetails.StartTime,
                            CreatedTime = opDetails.CreatedTime,
                            EndTime = opDetails.EndTime,
                            PurgeTime = opDetails.EndTime,
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

            return StatusCode((int)code, response);
        }

        #endregion


        #region Helpers

        private OperationDetails CreateOperationDetails(DateTime createdTime, Response.KindEnum kind)
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
        /// <summary>
        /// Adds a new operation to the operations map.
        /// </summary>
        /// <param name="operationid"></param>
        private void AddToOperationsMap(string operationid, OperationDetails opDetails)
        {
            OperationsMap.Add(operationid, opDetails);
        }

        /// <summary>
        /// Get status of a given operation
        /// </summary>
        /// <param name="operationid"></param>
        /// <returns></returns>
        private OperationDetails GetOperationDetails(string operationid)
        {
            if (OperationsMap.TryGetValue(operationid, out var opDetails))
            {
                return opDetails;
            }
            else return null;
        }
        #endregion
    }
}