using Microsoft.AspNetCore.Mvc;
using Microsoft.AzureBackup.DatasourcePlugin.Models;
using Microsoft.Internal.AzureBackup.DataProtection.Common.Interface;
using Microsoft.Internal.AzureBackup.DataProtection.Common.PitManager;
using Microsoft.Internal.AzureBackup.DataProtection.Common.PitManager.PitManagerFactories;
using Microsoft.Internal.AzureBackup.DataProtection.Common.PitManager.PitManagerInterfaces;
using Microsoft.Internal.AzureBackup.DataProtection.PitManagerInterface;
using Microsoft.Internal.CloudBackup.Common.Diag;
using Newtonsoft.Json;
using NSubstitute;
using SamplePlugin.Controllers;
using System.Net;
using Error = Microsoft.AzureBackup.DatasourcePlugin.Models.Error;
using ExecutionStatus = Microsoft.AzureBackup.DatasourcePlugin.Models.ExecutionStatus;
using InnerError = Microsoft.AzureBackup.DatasourcePlugin.Models.InnerError;

namespace SamplePlugin
{
    public static class Helper
    {
        public static Dictionary <string, string> headers = new Dictionary<string, string>();
        public static Dictionary<string, string> qparams = new Dictionary<string, string>();

        // PitMgr mocks related
        public static IStreamPitFormatWriter fakeWriter = null;
        public static IStreamPitFormatReader fakeReader = null;
        public static IPitManager fakePitmgr = null;
        public static IPit fakePit = null;
        public static IPit dummyPit = null;
        public static IPitManagerFactory fakePitMgrFactory = null;
        public static MemoryStream memStream = new MemoryStream();
        public static FileStream fStream = null;
        public static PassthroughStream ptStream = null;

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

        public static Response FormErrorResponse(Exception ex, DateTimeOffset createdTime, OperationType kind)
        {
            Error err = new Error()
            {
                Code = ex.HResult.ToString(),  // TODO: Give json error resource guidance
                Message = ex.Message,
                RecommendedAction = "Some recommended action",
                InnerError = new InnerError()
                {
                    Code = "innerErrorCode",  // This will be a code that comes from your source-dataplane
                    /*AdditionalInfo = new Dictionary<string, string>()
                        {
                            // This message will also come from source data plane. Fill ths if you want to show this on the Portal.
                            // e.g. https://msazure.visualstudio.com/One/_wiki/wikis/DppDocumentation/210788/Error-Modelling
                            { "DetailedNonLocalisedMessage", "セッションはすでに存在します：StorageUnit6000C298-c804-c37e-9f9d-dc8ae1f5ef89_0にはまだ前のセッションがあります99892eb3bfbc4a718234f9be80676f06アクティブ" }
                        }*/
                },
            };

            var errResponse = new Response(qparams["operationId"], kind, ExecutionStatus.Failed, createdTime)
            {
                StartTime = createdTime,
                EndTime = DateTime.UtcNow,
                PurgeTime = DateTime.UtcNow.AddHours(OperationsMap.gcOffsetInHours),
                FailedResponse = FormFailedStatus(kind, err),
            };
            OperationsMap.UpdateOperation(qparams["operationId"], DateTime.UtcNow, HttpStatusCode.InternalServerError, err);

            return errResponse;
        }

        private static BaseStatus FormFailedStatus(OperationType kind, Error err)
        {
            if (kind == OperationType.ValidateForProtection)
            {
                return new ValidateForProtectionStatus()
                {
                    Error = err,
                };
            }
                   

            // other cases for other verbs...
            // case Kind.StartProtectionEnum: 
            // etc.
            else
            {
                return new BaseStatus()
                {
                    Error = err
                };
            }

            /*
            switch (kind)
            {
                case OperationType.ValidateForProtection:
                    {
                        return new ValidateForProtectionStatus()
                        {
                            Error = err,
                        };
                    }
                    break;

                // other cases for other verbs...
                // case Kind.StartProtectionEnum: 
                // etc.

                default:
                    {
                        return new BaseStatus()
                        {
                            Error = err
                        };
                    }
            }
            */
        }

        /// <summary>
        /// Setups up mocks for PitManager. Cant use reeal one as authN to real Azure Svcs wont be possible
        /// </summary>
        /// <param name="rpCatalogInitParams"></param>
        /// <param name="datastoreInitParams"></param>
        public static void SetupPitmgrMocks(IDictionary<string, string> rpCatalogInitParams, IDictionary<string, string> datastoreInitParams,
            ILogger logger, DiagContextServiceInfo diagCtx)
        {
            fakePitMgrFactory = Substitute.For<IPitManagerFactory>();
            fakeWriter = Substitute.For<IStreamPitFormatWriter>();
            fakeReader = Substitute.For<IStreamPitFormatReader>();
            fakePit = Substitute.For<IPit>();
            fakePitmgr = Substitute.For<IPitManager>();

            IPitManager pitMgr = PitManagerFactory.Instance.GetPitManager(rpCatalogInitParams, datastoreInitParams, logger, diagContext: diagCtx);
            // Create dummy Pit.
            dummyPit = pitMgr.CreatePit(rpCatalogInitParams[VaultAndStoreInitializationParamsKeys.RecoveryPointId],
                 PitFormatType.AzureStorageBlockBlobUnseekableStream,
                 BackupType.Full);

            fakePitMgrFactory.GetPitManager(Arg.Any<Dictionary<string, string>>(),
                Arg.Any<Dictionary<string, string>>(), Arg.Any<ILogger>(), Arg.Any<PitManagerType>(), Arg.Any<DiagContextServiceInfo>())
                .Returns(fakePitmgr);

            fakePitmgr.CreatePit(Arg.Any<string>(), Arg.Any<PitFormatType>(), Arg.Any<BackupType>())
                .Returns(fakePit);

            fakePitmgr.CreatePit(Arg.Any<string>(), Arg.Any<PitFormatType>(), Arg.Any<BackupType>(), Arg.Any<RetentionTagInfo>(),
                Arg.Any<Microsoft.Internal.AzureBackup.DataProtection.Common.Interface.PolicyInfo>(), Arg.Any<string>(), Arg.Any<DateTimeOffset>())
                .Returns(fakePit);

            fakePit.PitFormatWriter.Returns(fakeWriter);
            fakePit.PitFormatReader.Returns(fakeReader);

            fakePit.When(x => x.Commit()).Do(x => { });

            fakeWriter.When(x => x.AddStorageUnit(Arg.Any<string>(), Arg.Any<int>()))
                .Do(x => { });

            fakePitmgr.GetPit(Arg.Any<string>()).Returns(fakePit);

            fakePit.When(x => x.InitializePitFormatReader())
                .Do(x => { });

            fakeWriter.CreateStream(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<bool>())
                 .Returns(x => {
                     ptStream = new PassthroughStream(new FileStream(Path.Combine(".", @"bkpstream"), FileMode.Create));
                     logger.LogInformation("Mock--> Created a FileStream at: ", Path.Combine(".", @"bkpstream"));
                     return ptStream;
                 });

            fakeReader.OpenStream(Arg.Any<string>(), Arg.Any<ILogger>(), Arg.Any<int>(), Arg.Any<bool>())
                .Returns(x => {
                    ptStream = new PassthroughStream(new FileStream(Path.Combine(".", @"bkpstream"), FileMode.Open));
                    logger.LogInformation("Mock--> Reading a FileStream from: ", Path.Combine(".", @"bkpstream"));
                    return ptStream;
                });

            fakeReader.When(x => x.CleanupStorageUnits(Arg.Any<string>()))
                .Do(x => { });
            fakeWriter.When(x => x.CleanupStorageUnits(Arg.Any<string>()))
                .Do(x => { });

            // make the test use the fakePitMgr factory.
            // Once the factory is fake, everything underneath can be faked.
            SamplePluginController.pitmgrFactory = fakePitMgrFactory;

        }
    }
}
