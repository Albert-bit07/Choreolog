# ChoreoOS typed management client

`ChoreoOS.Client.csproj` generates the gRPC C# client from
`proto/management/choreoos_management.proto` using `Grpc.Tools`.

Build with `dotnet build dotnet-dashboard/src/ChoreoOS.Client/ChoreoOS.Client.csproj`.

Run a three-node C++ cluster first. Build the C++ gateway with
`-DCHOREOOS_BUILD_MANAGEMENT_API=ON` and
`-DVCPKG_MANIFEST_FEATURES=management-grpc`, then start
`choreoos-management <node-port> <management-port>` for each node.
The gateway binds to loopback by design; it is not suitable for exposure
directly to a public network.

Use `new ChoreoOS.Client.ChoreoManagementConnection(50051)` and its
`Client` property to call the generated `ChoreoManagement` client.

Only mutations submitted to the leader and majority-committed are
successful. Treat timeouts as ambiguous outcomes and repeat with the
same operation ID and identical command. Do not treat any local read
as linearizable until a read-index/quorum barrier is implemented.
