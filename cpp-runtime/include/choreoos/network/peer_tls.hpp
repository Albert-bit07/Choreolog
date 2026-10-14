#pragma once

#include <boost/asio/ssl.hpp>
#include <memory>
#include <string>

#include "choreoos/runtime/config.hpp"

namespace choreoos::network {

[[nodiscard]] state::Result<std::unique_ptr<boost::asio::ssl::context>> make_peer_tls_context(
    const runtime::NodeConfig& config);

// Preserve OpenSSL chain, validity and purpose checks, then bind the one URI
// SAN urn:choreoos:node:<id> to configured membership and the dialed identity.
[[nodiscard]] bool verify_peer_certificate(bool preverified,
                                           boost::asio::ssl::verify_context& context,
                                           const runtime::NodeConfig& config,
                                           const std::string& expected_peer,
                                           std::string& verified_peer);

}  // namespace choreoos::network
