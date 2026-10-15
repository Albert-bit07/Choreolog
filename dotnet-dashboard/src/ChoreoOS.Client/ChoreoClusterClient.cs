using System.Diagnostics;
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
        // Bound fan-out even when a caller supplies a large endpoint list.
        using var concurrency = new SemaphoreSlim(4);
        var polls = _connections.Select(async connection =>
        {
            await concurrency.WaitAsync(cancellationToken);
            try
            {
                return await connection.Client.GetNodeStatusAsync(
                    new GetNodeStatusRequest(),
                    deadline: DateTime.UtcNow.AddSeconds(2),
                    cancellationToken: cancellationToken);
            }
            catch (RpcException error) when (IsTransient(error.StatusCode))
            {
                return null;
            }
            finally
            {
                concurrency.Release();
            }
        });
        var statuses = await Task.WhenAll(polls);
        return statuses.Where(status => status is not null).Select(status => status!).ToArray();
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

        request = request.Clone(); // Freeze caller-owned data across ambiguous retries.
        var budget = Stopwatch.StartNew();
        while (budget.Elapsed < timeout)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var connection in _connections)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var remainingBudget = timeout - budget.Elapsed;
                if (remainingBudget <= TimeSpan.Zero) break;
                var callDuration = remainingBudget < TimeSpan.FromSeconds(2)
                    ? remainingBudget : TimeSpan.FromSeconds(2);
                // gRPC requires UTC deadlines; retry-budget arithmetic stays monotonic.
                var callDeadline = DateTime.UtcNow.Add(callDuration);

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
                    IsTransient(error.StatusCode) || IsLeaderRedirect(error))
                {
                    // The leader may have stepped down or this write may
                    // already have committed. Never change request IDs.
                }
            }

            if (budget.Elapsed >= timeout)
            {
                break;
            }
            var remaining = timeout - budget.Elapsed;
            if (remaining <= TimeSpan.Zero) break;
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

    private static bool IsLeaderRedirect(RpcException error) =>
        error.StatusCode == StatusCode.FailedPrecondition &&
        error.Trailers.Any(entry => entry.Key == "choreoos-error-code" &&
                                    entry.Value == "NotLeader");

    public void Dispose()
    {
        foreach (var connection in _connections)
        {
            connection.Dispose();
        }
    }
}
