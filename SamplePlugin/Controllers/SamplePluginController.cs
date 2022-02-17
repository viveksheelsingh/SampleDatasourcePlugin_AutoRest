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

        private readonly ILogger<SamplePluginController> _logger;

        public SamplePluginController(ILogger<SamplePluginController> logger)
        {
            _logger = logger;
        }

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
            TestPayload p1 = new TestPayload()
            {
                X = p.X + 1,
                Y = p.Y + 1,
            };

            // Add to the Operations Map
            var opDetails = new OperationDetails(DateTime.UtcNow, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.BackupEnum);
            OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

            // Dispatch LRO on a async Task
            Task.Run(() =>
            {
                TestLROInternal(Request.Query["operationId"]);
            });

            // Async completion
            var response = new Response()
            {
                Id = Request.Query["operationId"],
                Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.BackupEnum,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.RunningEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = null,
                PurgeTime = null,
                SucceededResponse = new BackupStatus() // Success case, dont need to return anything.
                {
                    LoopBackContext = "testLoopbackCtx",
                },
            };
            return StatusCode(202, response);
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
                Id = Request.Headers["operationId"],
                Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForProtectionEnum,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.SucceededEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = DateTime.UtcNow,
                PurgeTime = DateTime.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                SucceededResponse = new ValidateForProtectionStatus() // Success case, dont need to return anything.
            };
            OperationsMap.UpdateOperation(Request.Query["operationId"], DateTime.UtcNow);

            // 202 + response body
            return Accepted(response);

            // Error case - similar response is to be created for other Verbs.
            /*
            Error err = new Error()
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
            };

            var errResponse = new Response()
            {
                Id = Request.Headers["operationId"],
                Kind = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.ValidateForProtectionEnum,
                Status = Microsoft.AzureBackup.DatasourcePlugin.Models.Response.StatusEnum.FailedEnum,
                StartTime = createdTime,
                CreatedTime = createdTime,
                EndTime = DateTime.UtcNow,
                PurgeTime = DateTime.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                SucceededResponse = new ValidateForProtectionStatus()
                {
                    Error = err,
                },
            };
            OperationsMap.UpdateOperation(Request.Query["operationId"], DateTime.UtcNow, HttpStatusCode.InternalServerError, err);
    

            // 500 + response body
            return StatusCode((int)HttpStatusCode.InternalServerError, errResponse);
            */

        }


        [HttpPost]
        [Route("/plugin:StartProtection")]
        public IActionResult StartProtection(StartProtectionRequest request)
        {
            var createdTime = DateTime.UtcNow;

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
                Id = Request.Headers["operationId"],
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
            return Accepted(response);
        }

        [HttpPost]
        [Route("/plugin:StopProtection")]
        public IActionResult StopProtection(StopProtectionRequest request)
        {
            var createdTime = DateTime.UtcNow;

            // Add to the Operations Map
            var opDetails = new OperationDetails(createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.StopProtectionEnum);
            OperationsMap.AddOperation(Request.Query["operationId"], opDetails);


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
                PurgeTime = DateTime.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                SucceededResponse = new StopProtectionStatus() // Success case, dont need to return anything.
            };
            OperationsMap.UpdateOperation(Request.Query["operationId"], DateTime.UtcNow);

            // 202 + response body
            return Accepted(response);
        }

        [HttpPost]
        [Route("/plugin:ValidateForBackup")]
        public IActionResult ValidateForBackup(ValidateForBackupRequest request)
        {
            var createdTime = DateTime.UtcNow;

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
                Id = Request.Headers["operationId"],
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
            return Accepted(response);
        }

        [HttpPost]
        [Route("/plugin:Backup")]
        public IActionResult Backup(BackupRequest request)
        {
            var createdTime = DateTime.UtcNow;

            // Add to the Operations Map
            var opDetails = new OperationDetails(createdTime, Microsoft.AzureBackup.DatasourcePlugin.Models.Response.KindEnum.BackupEnum);
            OperationsMap.AddOperation(Request.Query["operationId"], opDetails);

            // Dispatch the backup to an async Task
            Task.Run(() =>
            {
                BackupInternal(request, Request);
            });

            // Backup is typically Long running, so Async completion (LRO Status=Running)
            var response = new Response()
            {
                Id = Request.Headers["operationId"],
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
            return Accepted(response);

        }

        [HttpGet]
        [Route("/operations/{operationId}")]
        public IActionResult GetOperationStatus(string operationId)
        {
            Response response = null;
            HttpStatusCode code = HttpStatusCode.OK;

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
                                Id = operationId,
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
                                Id = operationId,
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
                                Id = operationId,
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

            // One of the httpStatusCodes set on the Operations + response body
            return StatusCode((int)code, response);

        }

        #endregion

        #region InternalMethods
        private async void BackupInternal(BackupRequest request, HttpRequest httpRequest)
        {
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

            // TBD: Add Pitmgr code here
            /*
              // Get PitMgr
            IPitManager pitMgr = PitManagerFactory.Instance.GetPitManager(PluginTestHelper.GetClientLibParams(customerData, store: false, isBvtd: true),
                PluginTestHelper.GetClientLibParams(customerData, store: true, isBvtd: true));

            // Create Pit.
            IPit pit = pitMgr.CreatePit(PluginTestHelper.GetClientLibParams(customerData)[VaultAndStoreInitializationParamsKeys.RecoveryPointId],
                PitFormatType.AzureStorageBlockBlobUnseekableStream,
                BackupType.Full);

            pit.InitializePitFormatWriter();

            StreamPitFormatWriter pitWriter = pit.PitFormatWriter as StreamPitFormatWriter;

            // Add a storage Unit
            pitWriter.AddStorageUnit("TestStorageUnit", 1);

            // Create and write to the stream.
            using (PassthroughStream stream = pitWriter.CreateStream("testStream"))
            {
                bytes = Encoding.ASCII.GetBytes(inputStr);
                stream.Write(bytes, 0, bytes.Length);
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
            pit.PluginMetadata = "testMetadata";
            pit.EndTime = DateTimeOffset.UtcNow;

            // commit.
            pit.Commit();
            
            //
            // =========== Recovery ============
            //

            // open the pit in vault
            IPit committedPit = pitMgr.GetPit(pit.PitId);
            Assert.IsTrue(committedPit.PitState == RecoveryPointState.COMMITTED);
            Assert.IsTrue(committedPit.PluginMetadata == committedPit.PluginMetadata);
            Console.WriteLine("pit state :" + committedPit.PitState);
            Assert.IsTrue(committedPit.PolicyInfo.PolicyName.Equals("testPolicy", StringComparison.InvariantCultureIgnoreCase) && committedPit.PolicyInfo.PolicyVersion.Equals("v1", StringComparison.InvariantCultureIgnoreCase));
            committedPit.InitializePitFormatReader();

            StreamPitFormatReader pitReader = committedPit.PitFormatReader as StreamPitFormatReader;


            foreach (var streamInfo in pitReader.StreamInfo)
            {
                int index = 0;
                PitManagerLoggerExtensions.Log(logger).LogInformation($"StorageUnit: {streamInfo.Item1} , Stream Count:{streamInfo.Item2.Count}");

                foreach (var stream in streamInfo.Item2)
                {
                    using (PassthroughStream ptStream = pitReader.OpenStream(stream, logger, index))
                    {
                        bytes = new byte[inputStr.Length];
                        int bytesRead = ptStream.Read(bytes, 0, bytes.Length);
                        string outputStr = Encoding.ASCII.GetString(bytes);

                        // Assert
                        Assert.IsTrue(ptStream.Length == bytes.Length, "Stream size not the same as input");
                        Assert.IsTrue(string.Equals(inputStr, outputStr, StringComparison.InvariantCulture), "Input string not equal to output string");
                    }

                }
            }
            pitReader.CleanupStorageUnits();
             */

            // Update the operation
            OperationsMap.UpdateOperation(httpRequest.Query["operationId"], DateTime.UtcNow);

            return;
        }

        /// <summary>
        /// Test Internal method for async LRO
        /// </summary>
        private async void TestLROInternal(string operationId)
        {
            await Task.Delay(4 * 1000);
            Console.WriteLine("Test LRO completed");
            OperationsMap.UpdateOperation(operationId, DateTime.UtcNow);
        }

        #endregion
    }
}