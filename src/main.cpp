#include <msquic.h>

#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

namespace {

struct ClientState {
    const QUIC_API_TABLE* api = nullptr;
    HQUIC configuration = nullptr;
    HQUIC connection = nullptr;
    std::string host;
    uint16_t port = 0;
    std::string payload;

    std::mutex mutex;
    std::condition_variable done_cv;
    bool done = false;
    int exit_code = 0;
};

struct StreamState {
    ClientState* client = nullptr;
    std::vector<uint8_t> payload;
};

void Finish(ClientState* state, int exit_code) {
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->done = true;
        state->exit_code = exit_code;
    }
    state->done_cv.notify_one();
}

const char* StatusToString(QUIC_STATUS status) {
    return QUIC_SUCCEEDED(status) ? "success" : "failure";
}

_IRQL_requires_max_(PASSIVE_LEVEL)
QUIC_STATUS
QUIC_API
StreamCallback(HQUIC stream, void* context, QUIC_STREAM_EVENT* event) {
    auto* stream_state = static_cast<StreamState*>(context);
    ClientState* client = stream_state->client;

    switch (event->Type) {
    case QUIC_STREAM_EVENT_START_COMPLETE:
        if (QUIC_FAILED(event->START_COMPLETE.Status)) {
            std::cerr << "stream start failed: 0x" << std::hex
                      << event->START_COMPLETE.Status << std::dec << '\n';
            client->api->StreamShutdown(
                stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
            return QUIC_STATUS_SUCCESS;
        }
        std::cout << "stream started, id=" << event->START_COMPLETE.ID << '\n';
        break;

    case QUIC_STREAM_EVENT_SEND_COMPLETE:
        std::cout << "request sent"
                  << (event->SEND_COMPLETE.Canceled ? " (canceled)" : "")
                  << '\n';
        break;

    case QUIC_STREAM_EVENT_RECEIVE:
        for (uint32_t i = 0; i < event->RECEIVE.BufferCount; ++i) {
            const QUIC_BUFFER& buffer = event->RECEIVE.Buffers[i];
            std::cout.write(reinterpret_cast<const char*>(buffer.Buffer),
                            buffer.Length);
        }
        std::cout.flush();
        if (event->RECEIVE.Flags & QUIC_RECEIVE_FLAG_FIN) {
            std::cout << "\npeer finished sending\n";
            client->api->ConnectionShutdown(
                client->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
        }
        break;

    case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
        std::cout << "peer closed send direction\n";
        client->api->ConnectionShutdown(
            client->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
        break;

    case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        std::cerr << "peer aborted send direction, error="
                  << event->PEER_SEND_ABORTED.ErrorCode << '\n';
        client->api->ConnectionShutdown(
            client->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
        break;

    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        client->api->StreamClose(stream);
        delete stream_state;
        break;

    default:
        break;
    }

    return QUIC_STATUS_SUCCESS;
}

bool StartStream(ClientState* client) {
    auto* stream_state = new StreamState;
    stream_state->client = client;
    stream_state->payload.assign(client->payload.begin(), client->payload.end());

    HQUIC stream = nullptr;
    QUIC_STATUS status = client->api->StreamOpen(
        client->connection,
        QUIC_STREAM_OPEN_FLAG_NONE,
        StreamCallback,
        stream_state,
        &stream);
    if (QUIC_FAILED(status)) {
        std::cerr << "StreamOpen failed: 0x" << std::hex << status << std::dec
                  << '\n';
        delete stream_state;
        return false;
    }

    QUIC_BUFFER buffer;
    buffer.Length = static_cast<uint32_t>(stream_state->payload.size());
    buffer.Buffer = stream_state->payload.data();

    status = client->api->StreamStart(stream, QUIC_STREAM_START_FLAG_IMMEDIATE);
    if (QUIC_FAILED(status)) {
        std::cerr << "StreamStart failed: 0x" << std::hex << status << std::dec
                  << '\n';
        client->api->StreamClose(stream);
        delete stream_state;
        return false;
    }

    status = client->api->StreamSend(
        stream, &buffer, 1, QUIC_SEND_FLAG_FIN, stream_state);
    if (QUIC_FAILED(status)) {
        std::cerr << "StreamSend failed: 0x" << std::hex << status << std::dec
                  << '\n';
        client->api->StreamShutdown(stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
        return false;
    }

    return true;
}

_IRQL_requires_max_(PASSIVE_LEVEL)
QUIC_STATUS
QUIC_API
ConnectionCallback(HQUIC connection, void* context, QUIC_CONNECTION_EVENT* event) {
    auto* client = static_cast<ClientState*>(context);

    switch (event->Type) {
    case QUIC_CONNECTION_EVENT_CONNECTED:
        std::cout << "connected to " << client->host << ':' << client->port
                  << '\n';
        if (!StartStream(client)) {
            client->api->ConnectionShutdown(
                connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
        }
        break;

    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        std::cerr << "transport shutdown: status=0x" << std::hex
                  << event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status << std::dec
                  << ", error="
                  << event->SHUTDOWN_INITIATED_BY_TRANSPORT.ErrorCode << '\n';
        break;

    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        std::cerr << "peer shutdown: error="
                  << event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode << '\n';
        break;

    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        std::cout << "connection shutdown complete"
                  << " (handshake="
                  << StatusToString(event->SHUTDOWN_COMPLETE.HandshakeCompleted
                                        ? QUIC_STATUS_SUCCESS
                                        : QUIC_STATUS_INTERNAL_ERROR)
                  << ")\n";
        client->api->ConnectionClose(connection);
        Finish(client, event->SHUTDOWN_COMPLETE.HandshakeCompleted ? 0 : 1);
        break;

    default:
        break;
    }

    return QUIC_STATUS_SUCCESS;
}

bool ParsePort(const char* value, uint16_t* port) {
    try {
        const unsigned long parsed = std::stoul(value);
        if (parsed == 0 || parsed > 65535) {
            return false;
        }
        *port = static_cast<uint16_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

void Usage(const char* argv0) {
    std::cerr << "usage: " << argv0
              << " <host> <port> [alpn] [payload]\n"
              << "example: " << argv0
              << " localhost 4433 sample \"hello from msquic\\n\"\n";
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3 || argc > 5) {
        Usage(argv[0]);
        return 2;
    }

    ClientState state;
    state.host = argv[1];
    if (!ParsePort(argv[2], &state.port)) {
        std::cerr << "invalid port: " << argv[2] << '\n';
        return 2;
    }

    const std::string alpn = argc >= 4 ? argv[3] : "sample";
    state.payload = argc >= 5 ? argv[4] : "hello from msquic\n";

    QUIC_STATUS status = MsQuicOpen2(&state.api);
    if (QUIC_FAILED(status)) {
        std::cerr << "MsQuicOpen2 failed: 0x" << std::hex << status << std::dec
                  << '\n';
        return 1;
    }

    QUIC_REGISTRATION_CONFIG registration_config;
    registration_config.AppName = "mosque";
    registration_config.ExecutionProfile = QUIC_EXECUTION_PROFILE_LOW_LATENCY;

    HQUIC registration = nullptr;
    status = state.api->RegistrationOpen(&registration_config, &registration);
    if (QUIC_FAILED(status)) {
        std::cerr << "RegistrationOpen failed: 0x" << std::hex << status
                  << std::dec << '\n';
        MsQuicClose(state.api);
        return 1;
    }

    QUIC_BUFFER alpn_buffer;
    alpn_buffer.Length = static_cast<uint32_t>(alpn.size());
    alpn_buffer.Buffer = reinterpret_cast<uint8_t*>(
        const_cast<char*>(alpn.data()));

    QUIC_SETTINGS settings {};
    settings.IdleTimeoutMs = 10000;
    settings.IsSet.IdleTimeoutMs = TRUE;

    status = state.api->ConfigurationOpen(
        registration,
        &alpn_buffer,
        1,
        &settings,
        sizeof(settings),
        nullptr,
        &state.configuration);
    if (QUIC_FAILED(status)) {
        std::cerr << "ConfigurationOpen failed: 0x" << std::hex << status
                  << std::dec << '\n';
        state.api->RegistrationClose(registration);
        MsQuicClose(state.api);
        return 1;
    }

    QUIC_CREDENTIAL_CONFIG credential_config {};
    credential_config.Type = QUIC_CREDENTIAL_TYPE_NONE;
    credential_config.Flags =
        QUIC_CREDENTIAL_FLAG_CLIENT |
        QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;

    status = state.api->ConfigurationLoadCredential(
        state.configuration, &credential_config);
    if (QUIC_FAILED(status)) {
        std::cerr << "ConfigurationLoadCredential failed: 0x" << std::hex
                  << status << std::dec << '\n';
        state.api->ConfigurationClose(state.configuration);
        state.api->RegistrationClose(registration);
        MsQuicClose(state.api);
        return 1;
    }

    status = state.api->ConnectionOpen(
        registration, ConnectionCallback, &state, &state.connection);
    if (QUIC_FAILED(status)) {
        std::cerr << "ConnectionOpen failed: 0x" << std::hex << status
                  << std::dec << '\n';
        state.api->ConfigurationClose(state.configuration);
        state.api->RegistrationClose(registration);
        MsQuicClose(state.api);
        return 1;
    }

    status = state.api->ConnectionStart(
        state.connection,
        state.configuration,
        QUIC_ADDRESS_FAMILY_UNSPEC,
        state.host.c_str(),
        state.port);
    if (QUIC_FAILED(status)) {
        std::cerr << "ConnectionStart failed: 0x" << std::hex << status
                  << std::dec << '\n';
        state.api->ConnectionClose(state.connection);
        state.api->ConfigurationClose(state.configuration);
        state.api->RegistrationClose(registration);
        MsQuicClose(state.api);
        return 1;
    }

    {
        std::unique_lock<std::mutex> lock(state.mutex);
        state.done_cv.wait(lock, [&state] { return state.done; });
    }

    state.api->ConfigurationClose(state.configuration);
    state.api->RegistrationClose(registration);
    MsQuicClose(state.api);
    return state.exit_code;
}
