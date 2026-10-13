using Choreoos.Management.V1;
using Grpc.Core;

namespace ChoreoOS.Client;

/// <summary>
/// Routes committed commands to a currently elected node on one local cluster.
/// On an ambiguous RPC failure the exact same request is retried, never
/// regenerated. This is a local development client, not remote authentication.
/// </summary>
public sealed class ChoreoClusterClient : IDisposable
{
    private readonly IReadOnlyList<ChoreoManagementConnection> _connections;

    public ChoreoClusterClient(IEnumerable<int> managementPorts)
    {
        ArgumentNullException.ThrowIfNull(managementPorts);
        var ports = managementPorts.Distinct().ToArray();
        if (ports.Length == 0)
        {
            throw new ArgumentException("At least one management port is required",
                                        nameof(managementPorts));
        }

        _connections = ports.Select(port => new ChoreoManagementConnection(port)).ToArray();
    }

    /// <summary>
    /// Queries all reachable nodes. Each status is only a local observation,
    /// not a quorum-confirmed, linearizable snapshot.
    /// </summary>
    public async Task<IReadOnlyList<NodeStatus>> GetReachableStatusesAsync(
        CancellationToken cancellationToken = default)
    {
        var statuses = new List<NodeStatus>();
        foreach (var connection in _connections)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var status = await connection.Client.GetNodeStatusAsync(
                    new GetNodeStatusRequest(),
                    deadline: DateTime.UtcNow.AddSeconds(2),
                    cancellationToken: cancellationToken);
                statuses.Add(status);
            }
            catch (RpcException error) when (IsTransient(error.StatusCode))
            {
                // A node or its local gRPC gateway may be offline.
            }
        }

        return statuses;
    }

    /// <summary>
    /// Submit a stable command ID and retry safely across leader changes.
    /// A timeout says the outcome is unknown. Callers must retain the same
    /// operation ID and canonical command before trying again.
    /// </summary>
    public async Task<SubmitCommandResponse> SubmitCommittedAsync(
        SubmitCommandRequest request,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.OperationId) ||
            string.IsNullOrWhiteSpace(request.CanonicalCommand))
        {
            throw new ArgumentException("A stable operation ID and canonical command are required",
                                        nameof(request));
        }
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }

        var end = DateTime.UtcNow.Add(timeout);
        while (DateTime.UtcNow < end)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var connection in _connections)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var callDeadline = DateTime.UtcNow.AddSeconds(2);
                if (callDeadline > end)
                {
                    callDeadline = end;
                }
                if (callDeadline <= DateTime.UtcNow)
                {
                    break;
                }

                try
                {
                    var status = await connection.Client.GetNodeStatusAsync(
                        new GetNodeStatusRequest(), deadline: callDeadline,
                        cancellationToken: cancellationToken);
                    if (status.Role != "leader")
                    {
                        continue;
                    }

                    var result = await connection.Client.SubmitCommandAsync(
                        request, deadline: callDeadline,
                        cancellationToken: cancellationToken);
                    if (!result.Committed)
                    {
                        throw new InvalidOperationException(
                            "The node returned success without a committed result");
                    }
                    return result;
                }
                catch (RpcException error) when (
                    IsTransient(error.StatusCode) ||
                    error.StatusCode == StatusCode.FailedPrecondition &&
                    error.Status.Detail.Contains("not leader", StringComparison.OrdinalIgnoreCase))
                {
                    // The leader may have stepped down or this write may
                    // already have committed. Never change request IDs.
                }
            }

            if (DateTime.UtcNow >= end)
            {
                break;
            }
            var remaining = end - DateTime.UtcNow;
            await Task.Delay(remaining < TimeSpan.FromMilliseconds(150)
                                 ? remaining : TimeSpan.FromMilliseconds(150),
                             cancellationToken);
        }

        throw new TimeoutException(
            $"Outcome for operation '{request.OperationId}' is unknown; " +
            "retry the exact same command with the same operation ID.");
    }

    private static bool IsTransient(StatusCode status) =>
        status is StatusCode.Unavailable or StatusCode.DeadlineExceeded;

    public void Dispose()
    {
        foreach (var connection in _connections)
        {
            connection.Dispose();
        }
    }
}
