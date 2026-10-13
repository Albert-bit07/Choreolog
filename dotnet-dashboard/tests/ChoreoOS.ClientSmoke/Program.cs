using ChoreoOS.Client;
using Choreoos.Management.V1;
using Grpc.Core;

if (args.Length != 1 || !int.TryParse(args[0], out var port))
{
    throw new ArgumentException("usage: ChoreoOS.ClientSmoke <management-port>");
}

using var connection = new ChoreoManagementConnection(port);
var api = connection.Client;
var deadline = DateTime.UtcNow.AddSeconds(10);

var initial = await api.GetNodeStatusAsync(new GetNodeStatusRequest(), deadline: deadline);
if (initial.Role != "leader" || !initial.Ready)
{
    throw new Exception("expected a ready single-node leader");
}

const string command =
    "schema=1;type=CREATE_CHOREOGRAPHY;cmd=smoke-create;choreo=opening;tick=0" +
    ";width=20000;depth=12000;overlap=forbidden;travel=5000";
var first = await api.SubmitCommandAsync(
    new SubmitCommandRequest { OperationId = "smoke-create", CanonicalCommand = command },
    deadline: deadline);
if (!first.Committed || first.Duplicate || first.CommittedIndex != 1)
{
    throw new Exception("initial command did not commit exactly once");
}
var retry = await api.SubmitCommandAsync(
    new SubmitCommandRequest { OperationId = "smoke-create", CanonicalCommand = command },
    deadline: deadline);
if (!retry.Committed || !retry.Duplicate || retry.CommittedIndex != first.CommittedIndex ||
    retry.CanonicalEvent != first.CanonicalEvent)
{
    throw new Exception("duplicate retry did not return the original receipt");
}

try
{
    await api.SubmitCommandAsync(
        new SubmitCommandRequest {
            OperationId = "smoke-create",
            CanonicalCommand = command.Replace("tick=0", "tick=1")
        }, deadline: deadline);
    throw new Exception("conflicting operation ID was accepted");
}
catch (RpcException error) when (error.StatusCode == StatusCode.AlreadyExists)
{
    if (!error.Trailers.Any(entry => entry.Key == "choreoos-error-code" &&
                                     entry.Value == "CommandConflict"))
    {
        throw new Exception("missing typed conflict error in gRPC trailers");
    }
}

var state = await api.GetStateAsync(new GetStateRequest { RequireLeader = true },
                                    deadline: deadline);
if (!state.Authoritative || state.AppliedIndex != 1 ||
    state.StateHash != first.StateHash || string.IsNullOrEmpty(state.CanonicalState))
{
    throw new Exception("state query did not reflect the committed command");
}

var events = await api.ListEventsAsync(new ListEventsRequest { PageSize = 1 },
                                       deadline: deadline);
if (events.Events.Count != 1 || events.Events[0].Index != 1 || events.HasMore)
{
    throw new Exception("committed event pagination failed");
}

var metrics = await api.GetMetricsAsync(new GetMetricsRequest(), deadline: deadline);
if (metrics.TransportFramesReceived == 0)
{
    throw new Exception("transport metrics did not advance");
}

var recovery = await api.GetRecoveryStatusAsync(new GetRecoveryStatusRequest(),
                                                 deadline: deadline);
if (recovery.CommitIndex != 1 || string.IsNullOrEmpty(recovery.RecoveryReport))
{
    throw new Exception("recovery metadata was unavailable");
}

try
{
    await api.InjectTestFaultAsync(
        new InjectTestFaultRequest { Fault = InjectTestFaultRequest.Types.Fault.PauseNode },
        deadline: deadline);
    throw new Exception("test fault was accepted without test_mode");
}
catch (RpcException error) when (error.StatusCode == StatusCode.PermissionDenied)
{
    // Production configuration forbids fault injection.
}

Console.WriteLine("C# -> gRPC -> C++ node management smoke test passed");
