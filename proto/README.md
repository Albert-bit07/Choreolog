# Protocol schemas

`internal/` will contain the versioned node-to-node Protocol Buffer schemas.
`management/` will contain the gRPC API shared by the C++ runtime and .NET
control plane. No protocol may serialize raw C++ object memory.
