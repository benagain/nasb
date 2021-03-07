﻿namespace NServiceBus.AzureFunctions.ServiceBus
{
    using System.Threading.Tasks;
    using Transport;

    class ManualPipelineInvocationInfrastructure : TransportReceiveInfrastructure
    {
        public ManualPipelineInvocationInfrastructure(PipelineInvoker pipelineInvoker) :
            base(() => pipelineInvoker,
                () => new NoOpQueueCreator(),
                () => Task.FromResult(StartupCheckResult.Success))
        {
        }
    }
}