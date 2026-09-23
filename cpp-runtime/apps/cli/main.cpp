// Single-node CLI over FileEngine.
// It only parses flags and prints results. Validation and hashing stay in
// choreoos::state so later nodes and Temporal Activities use the same rules.

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "choreoos/core/version.hpp"
#include "choreoos/protocol/client.hpp"
#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/state/store.hpp"

namespace {

using choreoos::state::ChoreographyId;
using choreoos::state::Command;
using choreoos::state::CommandId;
using choreoos::state::CommandType;
using choreoos::state::CreateChoreographyPayload;
using choreoos::state::CueId;
using choreoos::state::CuePayload;
using choreoos::state::DancerId;
using choreoos::state::DancerPayload;
using choreoos::state::Error;
using choreoos::state::Event;
using choreoos::state::FileEngine;
using choreoos::state::FormationId;
using choreoos::state::FormationPayload;
using choreoos::state::kCurrentSchemaVersion;
using choreoos::state::MusicalTick;
using choreoos::state::OverlapPolicy;
using choreoos::state::Position;
using choreoos::state::RemoveDancerPayload;
using choreoos::state::StageBounds;

struct Options {
  std::string output = "text";
  std::string command;
  std::vector<std::string> args;
};

void print_usage() {
  std::cout << "ChoreoOS CLI " << choreoos::core::version() << "\n\n"
            << "Usage:\n"
            << "  choreoos-cli [--output text|json] <command> --store DIR [flags]\n"
            << "  choreoos-cli [--output text|json] <command> --leader HOST:PORT [flags]\n\n"
            << "Commands:\n"
            << "  create            --id NAME --width-mm N --depth-mm N [--max-travel-mm N]\n"
            << "  add-dancer        --id NAME --x-mm N --y-mm N --count N\n"
            << "  remove-dancer     --id NAME --count N\n"
            << "  move              --id NAME --x-mm N --y-mm N --count N\n"
            << "  define-formation  --id NAME --members a,b --count N\n"
            << "  change-formation  --id NAME --members a,b --count N\n"
            << "  music             --id NAME --count N [--depends ID]\n"
            << "  lighting          --id NAME --count N [--depends ID]\n"
            << "  snapshot\n"
            << "  show              [--count N]\n"
            << "  events\n"
            << "  replay            [--count N]\n"
            << "  validate\n"
            << "  status            --leader HOST:PORT\n";
}

std::optional<std::string> flag(const std::vector<std::string>& args, std::string_view name) {
  for (std::size_t i = 0; i + 1 < args.size(); ++i) {
    if (args[i] == name) {
      return args[i + 1];
    }
  }
  return std::nullopt;
}

int fail(const Error& error, const std::string& output) {
  if (output == "json") {
    std::cout << "{\"ok\":false,\"error\":\"" << choreoos::state::error_code_name(error.code())
              << "\",\"message\":\"" << error.message() << "\"}\n";
  } else {
    std::cerr << error.to_string() << '\n';
  }
  return 1;
}

int fail(const std::string& message, const std::string& output) {
  return fail(Error{choreoos::state::ErrorCode::StoreError, message}, output);
}

ChoreographyId existing_or(const FileEngine& store, const std::string& fallback) {
  if (store.engine().state().id) {
    return *store.engine().state().id;
  }
  return ChoreographyId::parse(fallback).value();
}

std::string next_command_id(const FileEngine& store, const std::vector<std::string>& args) {
  if (const auto supplied = flag(args, "--command-id")) {
    return *supplied;
  }
  return "cmd-" + std::to_string(store.engine().state().last_applied.next().value());
}

std::vector<DancerId> parse_members(const std::string& raw) {
  std::vector<DancerId> members;
  std::istringstream stream{raw};
  std::string item;
  while (std::getline(stream, item, ',')) {
    members.push_back(DancerId::parse(item).value());
  }
  return members;
}

void print_stage(const choreoos::state::ChoreographyState& state) {
  if (!state.stage) {
    std::cout << "(empty)\n";
    return;
  }
  std::cout << "Choreography " << (state.id ? state.id->value() : "?") << '\n';
  std::cout << "Stage " << state.stage->width().mm() << "mm x " << state.stage->depth().mm()
            << "mm\n";
  std::cout << "Last applied " << state.last_applied.value() << '\n';
  std::cout << "Hash " << choreoos::state::state_hash(state) << '\n';
  for (const auto& [id, dancer] : state.dancers) {
    std::cout << "  " << id << (dancer.active ? "" : " (removed)") << " @ "
              << dancer.position.x().mm() << ',' << dancer.position.y().mm() << " mm\n";
  }
}

void print_json_state(const choreoos::state::ChoreographyState& state) {
  std::cout << "{\"ok\":true,\"hash\":\"" << choreoos::state::state_hash(state)
            << "\",\"last_applied\":" << state.last_applied.value() << ",\"canonical\":\"";
  for (char ch : choreoos::state::canonical_state(state)) {
    if (ch == '\n') {
      std::cout << "\\n";
    } else {
      std::cout << ch;
    }
  }
  std::cout << "\"}\n";
}

int submit(FileEngine& store, Command command, const std::string& output) {
  auto result = store.submit(std::move(command));
  if (!result) {
    return fail(result.error(), output);
  }
  if (output == "json") {
    std::cout << "{\"ok\":true,\"duplicate\":" << (result.value().duplicate ? "true" : "false")
              << ",\"index\":" << result.value().event.index.value() << ",\"hash\":\""
              << choreoos::state::state_hash(store.engine().state()) << "\"}\n";
  } else {
    std::cout << (result.value().duplicate ? "duplicate " : "committed ")
              << choreoos::state::event_type_name(result.value().event.type)
              << " index=" << result.value().event.index.value()
              << " hash=" << choreoos::state::state_hash(store.engine().state()) << '\n';
  }
  return 0;
}

struct LeaderEndpoint {
  std::string host;
  std::uint16_t port = 0;
};

std::optional<LeaderEndpoint> parse_leader(const std::string& text) {
  const auto colon = text.rfind(':');
  if (colon == std::string::npos) {
    return std::nullopt;
  }
  try {
    return LeaderEndpoint{text.substr(0, colon),
                          static_cast<std::uint16_t>(std::stoi(text.substr(colon + 1)))};
  } catch (...) {
    return std::nullopt;
  }
}

int run_remote(const Options& options, const LeaderEndpoint& leader) {
  if (options.command == "status" || options.command == "show") {
    choreoos::protocol::Frame frame;
    frame.type = choreoos::protocol::MessageType::StatusQuery;
    auto payload = choreoos::protocol::encode_status_query();
    if (!payload) {
      return fail(payload.error(), options.output);
    }
    frame.payload = payload.value();
    auto reply =
        choreoos::protocol::transact(leader.host, leader.port, frame, std::chrono::seconds(3));
    if (!reply) {
      return fail(reply.error(), options.output);
    }
    auto status = choreoos::protocol::decode_status_response(reply.value().payload);
    if (!status) {
      return fail(status.error(), options.output);
    }
    const auto& value = status.value();
    if (options.output == "json") {
      std::cout << "{\"ok\":true,\"id\":\"" << value.node_id << "\",\"role\":\"" << value.role
                << "\",\"term\":" << value.term << ",\"commit\":" << value.commit_index
                << ",\"hash\":\"" << value.state_hash << "\"}\n";
    } else {
      std::cout << value.node_id << " role=" << value.role << " term=" << value.term
                << " commit=" << value.commit_index << " last=" << value.last_log_index
                << " hash=" << value.state_hash << '\n';
      if (options.command == "show") {
        std::cout << value.canonical_state;
      }
    }
    return 0;
  }

  auto command = [&]() -> std::optional<Command> {
    const auto command_id = flag(options.args, "--command-id").value_or(options.command);
    auto parsed_id = CommandId::parse(command_id);
    if (!parsed_id) {
      return std::nullopt;
    }
    if (options.command == "create") {
      const auto id = flag(options.args, "--id");
      const auto width = flag(options.args, "--width-mm");
      const auto depth = flag(options.args, "--depth-mm");
      if (!id || !width || !depth) {
        return std::nullopt;
      }
      auto choreography = ChoreographyId::parse(*id);
      auto stage = StageBounds::from_mm(std::stoi(*width), std::stoi(*depth));
      auto tick = MusicalTick::from_count(0);
      if (!choreography || !stage || !tick) {
        return std::nullopt;
      }
      const auto travel = flag(options.args, "--max-travel-mm");
      return Command{
          parsed_id.value(),
          choreography.value(),
          CommandType::CreateChoreography,
          kCurrentSchemaVersion,
          tick.value(),
          CreateChoreographyPayload{choreography.value(), stage.value(), OverlapPolicy::Forbidden,
                                    travel ? std::stoi(*travel) : 5000}};
    }
    const auto show = flag(options.args, "--choreo").value_or("opening");
    auto choreography = ChoreographyId::parse(show);
    auto count_raw = flag(options.args, "--count");
    if (!choreography || !count_raw) {
      return std::nullopt;
    }
    auto tick = MusicalTick::from_count(std::stoll(*count_raw));
    if (!tick) {
      return std::nullopt;
    }
    if (options.command == "add-dancer" || options.command == "move") {
      const auto id = flag(options.args, "--id");
      const auto x = flag(options.args, "--x-mm");
      const auto y = flag(options.args, "--y-mm");
      if (!id || !x || !y) {
        return std::nullopt;
      }
      auto dancer = DancerId::parse(*id);
      auto position = Position::from_mm(std::stoi(*x), std::stoi(*y));
      if (!dancer || !position) {
        return std::nullopt;
      }
      return Command{
          parsed_id.value(),
          choreography.value(),
          options.command == "add-dancer" ? CommandType::AddDancer : CommandType::MoveDancer,
          kCurrentSchemaVersion,
          tick.value(),
          DancerPayload{dancer.value(), position.value()}};
    }
    return std::nullopt;
  }();
  if (!command) {
    return fail("remote command is missing flags or is not supported yet", options.output);
  }
  choreoos::protocol::ClientCommand body;
  body.canonical = choreoos::protocol::canonical_command(*command);
  choreoos::protocol::Frame frame;
  frame.type = choreoos::protocol::MessageType::ClientCommand;
  auto payload = choreoos::protocol::encode(body);
  if (!payload) {
    return fail(payload.error(), options.output);
  }
  frame.payload = payload.value();
  auto reply =
      choreoos::protocol::transact(leader.host, leader.port, frame, std::chrono::seconds(3));
  if (!reply) {
    return fail(reply.error(), options.output);
  }
  auto decoded = choreoos::protocol::decode_client_response(reply.value().payload);
  if (!decoded) {
    return fail(decoded.error(), options.output);
  }
  if (!decoded.value().ok) {
    return fail(decoded.value().error_code + ": " + decoded.value().error_message, options.output);
  }
  std::cout << (decoded.value().duplicate ? "duplicate" : "committed")
            << " index=" << decoded.value().index << " hash=" << decoded.value().state_hash << '\n';
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  std::vector<std::string> raw(argv + 1, argv + argc);
  if (raw.empty() || raw[0] == "help" || raw[0] == "--help") {
    print_usage();
    return 0;
  }

  std::size_t i = 0;
  if (i + 1 < raw.size() && raw[i] == "--output") {
    options.output = raw[i + 1];
    i += 2;
  }
  if (i >= raw.size()) {
    print_usage();
    return 2;
  }
  options.command = raw[i++];
  options.args.assign(raw.begin() + static_cast<std::ptrdiff_t>(i), raw.end());

  if (const auto leader = flag(options.args, "--leader")) {
    auto endpoint = parse_leader(*leader);
    if (!endpoint) {
      return fail("leader must look like HOST:PORT", options.output);
    }
    return run_remote(options, *endpoint);
  }

  if (options.command == "version") {
    std::cout << "choreoos-cli " << choreoos::core::version() << '\n';
    return 0;
  }

  const auto store_dir = flag(options.args, "--store");
  if (!store_dir) {
    return fail("missing --store DIR", options.output);
  }
  auto store = FileEngine::open(*store_dir);
  if (!store) {
    return fail(store.error(), options.output);
  }

  if (options.command == "snapshot") {
    if (auto saved = store.value().checkpoint(); !saved) {
      return fail(saved.error(), options.output);
    }
    if (options.output == "json") {
      std::cout << "{\"ok\":true,\"hash\":\""
                << choreoos::state::state_hash(store.value().engine().state()) << "\"}\n";
    } else {
      std::cout << "snapshot index=" << store.value().engine().state().last_applied.value()
                << " hash=" << choreoos::state::state_hash(store.value().engine().state()) << '\n';
    }
    return 0;
  }

  if (options.command == "show" || options.command == "replay" || options.command == "validate") {
    std::optional<choreoos::state::MusicalTick> through;
    if (const auto count_flag = flag(options.args, "--count")) {
      auto tick = MusicalTick::from_count(std::stoll(*count_flag));
      if (!tick) {
        return fail(tick.error(), options.output);
      }
      through = tick.value();
    }
    const auto replayed = store.value().engine().replay_through(through, std::nullopt);
    if (!replayed) {
      return fail(replayed.error(), options.output);
    }
    if (options.output == "json") {
      print_json_state(replayed.value());
    } else {
      print_stage(replayed.value());
      if (options.command == "validate") {
        std::cout << choreoos::state::canonical_state(replayed.value());
      }
    }
    return 0;
  }

  if (options.command == "events") {
    for (const auto& event : store.value().engine().events()) {
      std::cout << choreoos::state::canonical_event(event) << '\n';
    }
    return 0;
  }

  const std::string command_id = next_command_id(store.value(), options.args);
  auto parsed_command_id = CommandId::parse(command_id);
  if (!parsed_command_id) {
    return fail(parsed_command_id.error(), options.output);
  }

  if (options.command == "create") {
    const auto id = flag(options.args, "--id");
    const auto width = flag(options.args, "--width-mm");
    const auto depth = flag(options.args, "--depth-mm");
    if (!id || !width || !depth) {
      return fail("create requires --id --width-mm --depth-mm", options.output);
    }
    const auto travel = flag(options.args, "--max-travel-mm");
    auto choreography = ChoreographyId::parse(*id);
    auto stage = StageBounds::from_mm(std::stoi(*width), std::stoi(*depth));
    auto tick = MusicalTick::from_count(0);
    if (!choreography || !stage || !tick) {
      return fail("invalid create arguments", options.output);
    }
    Command command{
        parsed_command_id.value(),
        choreography.value(),
        CommandType::CreateChoreography,
        kCurrentSchemaVersion,
        tick.value(),
        CreateChoreographyPayload{choreography.value(), stage.value(), OverlapPolicy::Forbidden,
                                  travel ? std::stoi(*travel) : 5000}};
    return submit(store.value(), std::move(command), options.output);
  }

  auto choreography =
      existing_or(store.value(), flag(options.args, "--choreo").value_or("opening"));
  auto count_raw = flag(options.args, "--count");
  if (!count_raw && options.command != "events") {
    return fail("command requires --count", options.output);
  }
  auto tick = MusicalTick::from_count(count_raw ? std::stoll(*count_raw) : 0);
  if (!tick) {
    return fail(tick.error(), options.output);
  }

  if (options.command == "add-dancer" || options.command == "move") {
    const auto id = flag(options.args, "--id");
    const auto x = flag(options.args, "--x-mm");
    const auto y = flag(options.args, "--y-mm");
    if (!id || !x || !y) {
      return fail("dancer command requires --id --x-mm --y-mm", options.output);
    }
    auto dancer = DancerId::parse(*id);
    auto position = Position::from_mm(std::stoi(*x), std::stoi(*y));
    if (!dancer || !position) {
      return fail("invalid dancer position or id", options.output);
    }
    Command command{
        parsed_command_id.value(),
        choreography,
        options.command == "add-dancer" ? CommandType::AddDancer : CommandType::MoveDancer,
        kCurrentSchemaVersion,
        tick.value(),
        DancerPayload{dancer.value(), position.value()}};
    return submit(store.value(), std::move(command), options.output);
  }

  if (options.command == "remove-dancer") {
    const auto id = flag(options.args, "--id");
    if (!id) {
      return fail("remove-dancer requires --id", options.output);
    }
    auto dancer = DancerId::parse(*id);
    if (!dancer) {
      return fail(dancer.error(), options.output);
    }
    Command command{parsed_command_id.value(), choreography, CommandType::RemoveDancer,
                    kCurrentSchemaVersion,     tick.value(), RemoveDancerPayload{dancer.value()}};
    return submit(store.value(), std::move(command), options.output);
  }

  if (options.command == "define-formation" || options.command == "change-formation") {
    const auto id = flag(options.args, "--id");
    const auto members_raw = flag(options.args, "--members");
    if (!id || !members_raw) {
      return fail("formation command requires --id --members", options.output);
    }
    auto formation = FormationId::parse(*id);
    if (!formation) {
      return fail(formation.error(), options.output);
    }
    Command command{parsed_command_id.value(),
                    choreography,
                    options.command == "define-formation" ? CommandType::DefineFormation
                                                          : CommandType::ChangeFormation,
                    kCurrentSchemaVersion,
                    tick.value(),
                    FormationPayload{formation.value(), parse_members(*members_raw)}};
    return submit(store.value(), std::move(command), options.output);
  }

  if (options.command == "music" || options.command == "lighting") {
    const auto id = flag(options.args, "--id");
    if (!id) {
      return fail("cue command requires --id", options.output);
    }
    auto cue = CueId::parse(*id);
    if (!cue) {
      return fail(cue.error(), options.output);
    }
    std::optional<CueId> depends;
    if (const auto dep = flag(options.args, "--depends")) {
      auto parsed = CueId::parse(*dep);
      if (!parsed) {
        return fail(parsed.error(), options.output);
      }
      depends = parsed.value();
    }
    Command command{
        parsed_command_id.value(),
        choreography,
        options.command == "music" ? CommandType::TriggerMusicCue : CommandType::TriggerLightingCue,
        kCurrentSchemaVersion,
        tick.value(),
        CuePayload{cue.value(), depends}};
    return submit(store.value(), std::move(command), options.output);
  }

  print_usage();
  return 2;
}
