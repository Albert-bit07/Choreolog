#include "choreoos/network/peer_tls.hpp"

#include <openssl/x509v3.h>

#include <algorithm>
#include <optional>
#include <string_view>

#include "choreoos/state/types.hpp"

namespace choreoos::network {
namespace {

bool peer_purpose(X509* certificate) {
  const auto usage = X509_get_extended_key_usage(certificate);
  return (X509_get_extension_flags(certificate) & EXFLAG_XKUSAGE) != 0 &&
         (usage & (XKU_SSL_SERVER | XKU_SSL_CLIENT)) == (XKU_SSL_SERVER | XKU_SSL_CLIENT) &&
         X509_check_ca(certificate) == 0 &&
         X509_check_purpose(certificate, X509_PURPOSE_SSL_SERVER, 0) == 1 &&
         X509_check_purpose(certificate, X509_PURPOSE_SSL_CLIENT, 0) == 1;
}

std::optional<std::string> certificate_node_id(X509* certificate) {
  constexpr std::string_view prefix = "urn:choreoos:node:";
  std::unique_ptr<GENERAL_NAMES, decltype(&GENERAL_NAMES_free)> names(
      static_cast<GENERAL_NAMES*>(
          X509_get_ext_d2i(certificate, NID_subject_alt_name, nullptr, nullptr)),
      GENERAL_NAMES_free);
  if (!names) return std::nullopt;
  std::optional<std::string> identity;
  for (int i = 0; i < sk_GENERAL_NAME_num(names.get()); ++i) {
    const auto* name = sk_GENERAL_NAME_value(names.get(), i);
    if (name->type != GEN_URI) continue;
    const auto* uri = name->d.uniformResourceIdentifier;
    const auto length = ASN1_STRING_length(uri);
    if (length <= 0) return std::nullopt;
    const std::string_view value(reinterpret_cast<const char*>(ASN1_STRING_get0_data(uri)),
                                 static_cast<std::size_t>(length));
    if (!value.starts_with(prefix)) continue;
    const auto id = value.substr(prefix.size());
    if (identity || !state::NodeId::parse(id)) return std::nullopt;
    identity = std::string(id);
  }
  return identity;
}

state::Error invalid(const std::string& why) {
  return {state::ErrorCode::StoreError, "peer TLS: " + why};
}

}  // namespace

state::Result<std::unique_ptr<boost::asio::ssl::context>> make_peer_tls_context(
    const runtime::NodeConfig& config) {
  if (!config.peer_tls) return std::unique_ptr<boost::asio::ssl::context>{};
  try {
    const auto& tls = *config.peer_tls;
    auto context = std::make_unique<boost::asio::ssl::context>(boost::asio::ssl::context::tls);
    if (SSL_CTX_set_min_proto_version(context->native_handle(), TLS1_3_VERSION) != 1) {
      return invalid("TLS 1.3 is required");
    }
    context->set_options(boost::asio::ssl::context::no_compression);
    SSL_CTX_set_session_cache_mode(context->native_handle(), SSL_SESS_CACHE_OFF);
    SSL_CTX_set_options(context->native_handle(), SSL_OP_NO_TICKET);
    SSL_CTX_set_max_early_data(context->native_handle(), 0);
    SSL_CTX_set_verify_depth(context->native_handle(), 4);
    SSL_CTX_set_max_cert_list(context->native_handle(), 64u * 1024u);
    context->load_verify_file(tls.ca_file.string());
    context->use_certificate_chain_file(tls.certificate_file.string());
    // Daemons must fail rather than prompt for an encrypted key's password.
    context->set_password_callback(
        [](std::size_t, boost::asio::ssl::context::password_purpose) { return std::string{}; });
    context->use_private_key_file(tls.private_key_file.string(), boost::asio::ssl::context::pem);
    if (SSL_CTX_check_private_key(context->native_handle()) != 1)
      return invalid("certificate/key mismatch");
    auto* local = SSL_CTX_get0_certificate(context->native_handle());
    const auto id = certificate_node_id(local);
    if (!id || *id != config.id) return invalid("local certificate URI SAN must match the node ID");
    if (!peer_purpose(local)) {
      return invalid("local certificate must permit both server and client authentication");
    }
    STACK_OF(X509)* chain = nullptr;
    SSL_CTX_get0_chain_certs(context->native_handle(), &chain);
    std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)> checked(X509_STORE_CTX_new(),
                                                                            X509_STORE_CTX_free);
    if (!checked ||
        X509_STORE_CTX_init(checked.get(), SSL_CTX_get_cert_store(context->native_handle()), local,
                            chain) != 1 ||
        X509_verify_cert(checked.get()) != 1) {
      return invalid("local certificate chain or validity check failed");
    }
    context->set_verify_mode(boost::asio::ssl::verify_peer |
                             boost::asio::ssl::verify_fail_if_no_peer_cert);
    return context;
  } catch (const std::exception& error) {
    return invalid(error.what());
  }
}

bool verify_peer_certificate(bool preverified, boost::asio::ssl::verify_context& context,
                             const runtime::NodeConfig& config, const std::string& expected_peer,
                             std::string& verified_peer) {
  if (!preverified) return false;
  auto* native = context.native_handle();
  if (X509_STORE_CTX_get_error_depth(native) != 0) return true;
  auto* certificate = X509_STORE_CTX_get_current_cert(native);
  if (!peer_purpose(certificate)) return false;
  const auto id = certificate_node_id(certificate);
  if (!id || (!expected_peer.empty() && *id != expected_peer)) return false;
  const bool member = std::any_of(config.peers.begin(), config.peers.end(),
                                  [&](const auto& peer) { return peer.id == *id; });
  if (!member) return false;
  verified_peer = *id;
  return true;
}

}  // namespace choreoos::network
