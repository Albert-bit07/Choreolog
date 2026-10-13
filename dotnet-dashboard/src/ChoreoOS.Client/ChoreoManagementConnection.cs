using Grpc.Net.Client;
using Choreoos.Management.V1;

namespace ChoreoOS.Client;

/// <summary>
/// Local-only management transport. Run the native C++ gateway separately.
/// A leader may change; reuse operation IDs when retrying an uncertain commit.
/// </summary>
public sealed class ChoreoManagementConnection : IDisposable
{
    private readonly GrpcChannel _channel;

    public ChoreoManagementConnection(int port)
    {
        if (port is < 1 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(port));
        }

        _channel = GrpcChannel.ForAddress($"http://127.0.0.1:{port}");
        Client = new ChoreoManagement.ChoreoManagementClient(_channel);
    }

    public ChoreoManagement.ChoreoManagementClient Client { get; }

    public void Dispose() => _channel.Dispose();
}
