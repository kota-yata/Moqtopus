#include "moq/codec.h"

#include "codec/internal.h"

namespace moq::codec
{

    ByteBuffer encode_setup(std::string authority, std::string path)
    {
        ByteBuffer payload;
        if (!path.empty())
        {
            internal::append_setup_option_bytes(
                payload, codec::SetupOption::Path, codec::SetupOption::None, path);
        }
        if (!authority.empty())
        {
            internal::append_setup_option_bytes(
                payload, codec::SetupOption::Authority,
                path.empty() ? codec::SetupOption::None : codec::SetupOption::Path, authority);
        }
        internal::append_setup_option_bytes(
            payload, codec::SetupOption::MoqtImplementation,
            authority.empty() ? (path.empty() ? codec::SetupOption::None : codec::SetupOption::Path)
                              : codec::SetupOption::Authority,
            "moqtopus/0.1.0");

        ByteBuffer stream_bytes;
        write_varint(stream_bytes, kSetupStreamType);
        append_control_message(stream_bytes, kMessageSetup, payload);
        return stream_bytes;
    }

    bool decode_setup(const ByteBuffer &payload, std::string &error)
    {
        internal::Cursor cursor{payload};
        if (!internal::skip_key_value_pairs(cursor))
        {
            error = "invalid SETUP options";
            return false;
        }
        return true;
    }

} // namespace moq::codec
