using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using ChoreoOS.Client;
using Choreoos.Management.V1;
using Grpc.Core;

if (args.Length != 2)
{
    throw new ArgumentException("usage: ChoreoOS.ClusterSmoke <choreoos-node> <choreoos-management>");
}

var nodeBinary = Path.GetFullPath(args[0]);
var gatewayBinary = Path.GetFullPath(args[1]);
if (!File.Exists(nodeBinary) || !File.Exists(gatewayBinary))
{
    throw new FileNotFoundException("Both native test executables must exist");
}

var root = Path.Combine(Path.GetTempPath(), "choreoos-cluster-smoke-" +
                                               Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
var allocatedPorts = new HashSet<int>();

int AllocatePort()
{
    int port;
    do
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
    } while (!allocatedPorts.Add(port));
    return port;
}

var nodes = Enumerable.Range(0, 3)
    .Select(i => new RunningNode("node-" + (i + 1), AllocatePort(), AllocatePort(),
                                 Path.Combine(root, "node-" + (i + 1) + ".conf")))
    .ToArray();

foreach (var node in nodes)
{
    var peers = string.Join(",", nodes.Where(other => other != node)
                                      .Select(other => other.Id + "@127.0.0.1:" + other.TcpPort));
    File.WriteAllText(node.ConfigFile,
        $"id: {node.Id}\nleader: node-1\ndata: {Path.Combine(root, node.Id)}\n" +
        $"listen: 127.0.0.1:{node.TcpPort}\npeers: {peers}\n" +
        $"elections: true\nrng_seed: {11 + Array.IndexOf(nodes, node) * 7}\n" +
        "heartbeat_ms: 100\nelection_timeout_min: 6\nelection_timeout_max: 12\n");
}

static void Require(bool condition, string message)
{
    if (!condition)
    {
        throw new Exception(message);
    }
}

static async Task ExpectRpcAsync(Func<Task> call, StatusCode expected,
                                string? runtimeCode = null)
{
    try
    {
        await call();
    }
    catch (RpcException error) when (error.StatusCode == expected)
    {
        if (runtimeCode is not null && !error.Trailers.Any(
                entry => entry.Key == "choreoos-error-code" &&
                         entry.Value == runtimeCode))
        {
            throw new Exception("missing typed ChoreoOS error trailer " + runtimeCode);
        }
        return;
    }
    throw new Exception("expected gRPC status " + expected);
}

var connections = nodes.Select(n => new ChoreoManagementConnection(n.ManagementPort)).ToArray();
using var client = new ChoreoClusterClient(nodes.Select(n => n.ManagementPort));

async Task<(int Index, NodeStatus Status)> WaitForLeaderAsync(TimeSpan timeout)
{
    var until = DateTime.UtcNow.Add(timeout);
    while (DateTime.UtcNow < until)
    {
        for (var i = 0; i < nodes.Length; ++i)
        {
            if (!nodes[i].Alive)
            {
                continue;
            }

            try
            {
                var status = await connections[i].Client.GetNodeStatusAsync(
                    new GetNodeStatusRequest(), deadline: DateTime.UtcNow.AddSeconds(2));
                if (status.Role == "leader")
                {
                    return (i, status);
                }
            }
            catch (RpcException error) when (
                error.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded)
            {
                // Gateway or node is not available yet.
            }
        }
        await Task.Delay(150);
    }
    throw new TimeoutException("No elected leader before the failover test deadline");
}

async Task WaitForConvergenceAsync(int liveCount, ulong minIndex, TimeSpan timeout)
{
    var until = DateTime.UtcNow.Add(timeout);
    while (DateTime.UtcNow < until)
    {
        var statuses = await client.GetReachableStatusesAsync();
        if (statuses.Count == liveCount && statuses.All(status =>
                status.CommitIndex >= minIndex &&
                status.CommitIndex == statuses[0].CommitIndex &&
                status.StateHash == statuses[0].StateHash))
        {
            return;
        }
        await Task.Delay(200);
    }
    throw new TimeoutException("Replicas did not converge to one committed hash");
}

const string createCanonical =
    "schema=1;type=CREATE_CHOREOGRAPHY;cmd=hardening-create;choreo=opening;tick=0" +
    ";width=20000;depth=12000;overlap=forbidden;travel=5000";
var create = new SubmitCommandRequest
{
    OperationId = "hardening-create",
    CanonicalCommand = createCanonical
};

try
{
    foreach (var node in nodes)
    {
        node.Start(nodeBinary, gatewayBinary);
    }

    var firstLeader = await WaitForLeaderAsync(TimeSpan.FromSeconds(35));
    var first = await client.SubmitCommittedAsync(create, TimeSpan.FromSeconds(35));
    Require(first.Committed && !first.Duplicate && first.CommittedIndex > 0,
            "initial create command did not majority-commit");
    await WaitForConvergenceAsync(3, first.CommittedIndex, TimeSpan.FromSeconds(35));
    var identical = await client.SubmitCommittedAsync(create, TimeSpan.FromSeconds(15));
    Require(identical.Duplicate && identical.CommittedIndex == first.CommittedIndex &&
            identical.CanonicalEvent == first.CanonicalEvent,
            "same-command retry did not return the original receipt");

    var follower = Enumerable.Range(0, nodes.Length)
        .First(i => i != firstLeader.Index);
    await ExpectRpcAsync(async () =>
    {
        await connections[follower].Client.GetStateAsync(
            new GetStateRequest { RequireLeader = true },
            deadline: DateTime.UtcNow.AddSeconds(5));
    }, StatusCode.FailedPrecondition);
    await ExpectRpcAsync(async () =>
    {
        await connections[follower].Client.SubmitCommandAsync(
            create, deadline: DateTime.UtcNow.AddSeconds(5));
    }, StatusCode.FailedPrecondition, "NotLeader");
    var observed = await connections[follower].Client.GetStateAsync(
        new GetStateRequest { RequireLeader = false },
        deadline: DateTime.UtcNow.AddSeconds(5));
    Require(!observed.Authoritative, "follower incorrectly advertised authoritative reads");

    await ExpectRpcAsync(async () =>
    {
        await connections[firstLeader.Index].Client.SubmitCommandAsync(
            new SubmitCommandRequest {
                OperationId = "mismatched-operation-id",
                CanonicalCommand = createCanonical
            }, deadline: DateTime.UtcNow.AddSeconds(5));
    }, StatusCode.InvalidArgument);
    await ExpectRpcAsync(async () =>
    {
        await connections[firstLeader.Index].Client.ListEventsAsync(
            new ListEventsRequest { PageSize = 129 },
            deadline: DateTime.UtcNow.AddSeconds(5));
    }, StatusCode.InvalidArgument);
    await ExpectRpcAsync(async () =>
    {
        await connections[firstLeader.Index].Client.InjectTestFaultAsync(
            new InjectTestFaultRequest {
                Fault = InjectTestFaultRequest.Types.Fault.PauseNode
            }, deadline: DateTime.UtcNow.AddSeconds(5));
    }, StatusCode.PermissionDenied);

    // Verify a stable non-regressing cursor when the requested page is empty.
    var emptyPage = await connections[firstLeader.Index].Client.ListEventsAsync(
        new ListEventsRequest { AfterIndex = first.CommittedIndex + 1000, PageSize = 1 },
        deadline: DateTime.UtcNow.AddSeconds(5));
    Require(emptyPage.Events.Count == 0 && !emptyPage.HasMore &&
            emptyPage.NextAfterIndex == first.CommittedIndex + 1000,
            "empty event page regressed the index cursor");

    nodes[firstLeader.Index].Stop();
    var secondLeader = await WaitForLeaderAsync(TimeSpan.FromSeconds(40));
    Require(secondLeader.Index != firstLeader.Index, "old leader was not replaced");

    var afterElection = await client.SubmitCommittedAsync(create, TimeSpan.FromSeconds(35));
    Require(afterElection.Duplicate &&
            afterElection.CommittedIndex == first.CommittedIndex &&
            afterElection.CanonicalEvent == first.CanonicalEvent,
            "new leader did not replay the original committed outcome");

    await ExpectRpcAsync(async () =>
    {
        await client.SubmitCommittedAsync(new SubmitCommandRequest {
            OperationId = create.OperationId,
            CanonicalCommand = create.CanonicalCommand.Replace("tick=0", "tick=1")
        }, TimeSpan.FromSeconds(12));
    }, StatusCode.AlreadyExists, "CommandConflict");

    const string dancerCanonical =
        "schema=1;type=ADD_DANCER;cmd=hardening-dancer;choreo=opening;tick=1" +
        ";dancer=alice;x=1000;y=1000";
    var dancer = new SubmitCommandRequest {
        OperationId = "hardening-dancer",
        CanonicalCommand = dancerCanonical
    };
    var added = await client.SubmitCommittedAsync(dancer, TimeSpan.FromSeconds(35));
    Require(added.Committed && !added.Duplicate,
            "the new leader could not majority-commit after failover");
    await WaitForConvergenceAsync(2, added.CommittedIndex, TimeSpan.FromSeconds(35));

    // Remove the remaining follower so the cluster has no quorum. An
    // ambiguous outcome must not be misreported as an unsuccessful mutation.
    var downFollower = Enumerable.Range(0, nodes.Length)
        .First(i => nodes[i].Alive && i != secondLeader.Index);
    nodes[downFollower].Stop();
    var noQuorum = new SubmitCommandRequest {
        OperationId = "hardening-no-quorum",
        CanonicalCommand = "schema=1;type=ADD_DANCER;cmd=hardening-no-quorum;" +
                           "choreo=opening;tick=2;dancer=bob;x=3000;y=1000"
    };
    try
    {
        await client.SubmitCommittedAsync(noQuorum, TimeSpan.FromSeconds(5));
        throw new Exception("a minority unexpectedly reported a committed write");
    }
    catch (TimeoutException error) when (error.Message.Contains("unknown"))
    {
        // The operation may have reached the WAL but was not confirmed.
    }

    nodes[downFollower].Start(nodeBinary, gatewayBinary);
    _ = await WaitForLeaderAsync(TimeSpan.FromSeconds(40));
    var recovered = await client.SubmitCommittedAsync(noQuorum, TimeSpan.FromSeconds(40));
    Require(recovered.Committed, "the quorum did not recover after restart");
    var recoveredAgain = await client.SubmitCommittedAsync(noQuorum,
                                                            TimeSpan.FromSeconds(20));
    Require(recoveredAgain.Duplicate &&
            recoveredAgain.CommittedIndex == recovered.CommittedIndex &&
            recoveredAgain.CanonicalEvent == recovered.CanonicalEvent,
            "ambiguous retry appended a second event");

    // Restore the original leader. All three durable replicas must agree.
    nodes[firstLeader.Index].Start(nodeBinary, gatewayBinary);
    await WaitForConvergenceAsync(3, recovered.CommittedIndex, TimeSpan.FromSeconds(50));
    Console.WriteLine("Three-node gRPC failover, reconnect, conflict and quorum smoke passed");
}
finally
{
    foreach (var connection in connections)
    {
        connection.Dispose();
    }
    foreach (var node in nodes)
    {
        node.Dispose();
    }
    try
    {
        Directory.Delete(root, recursive: true);
    }
    catch (IOException)
    {
        // Do not mask the original integration failure if a socket/file is closing.
    }
}

internal sealed class RunningNode : IDisposable
{
    private Process? _node;
    private Process? _gateway;

    public RunningNode(string id, int tcpPort, int managementPort, string configFile)
    {
        Id = id;
        TcpPort = tcpPort;
        ManagementPort = managementPort;
        ConfigFile = configFile;
    }

    public string Id { get; }
    public int TcpPort { get; }
    public int ManagementPort { get; }
    public string ConfigFile { get; }
    public bool Alive => _node is { HasExited: false };

    public void Start(string nodeBinary, string gatewayBinary)
    {
        if (Alive)
        {
            throw new InvalidOperationException("node already running");
        }
        var nodeStart = new ProcessStartInfo(nodeBinary) {
            UseShellExecute = false, CreateNoWindow = true
        };
        nodeStart.ArgumentList.Add(ConfigFile);
        _node = Process.Start(nodeStart)
            ?? throw new InvalidOperationException("failed to launch native node");

        var gatewayStart = new ProcessStartInfo(gatewayBinary) {
            UseShellExecute = false, CreateNoWindow = true
        };
        gatewayStart.ArgumentList.Add(TcpPort.ToString());
        gatewayStart.ArgumentList.Add(ManagementPort.ToString());
        _gateway = Process.Start(gatewayStart)
            ?? throw new InvalidOperationException("failed to launch management gateway");
    }

    public void Stop()
    {
        foreach (var process in new[] { _gateway, _node })
        {
            if (process is null)
            {
                continue;
            }
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                if (!process.WaitForExit(10000))
                {
                    throw new TimeoutException("child process did not terminate");
                }
            }
            process.Dispose();
        }
        _gateway = null;
        _node = null;
    }

    public void Dispose() => Stop();
}
