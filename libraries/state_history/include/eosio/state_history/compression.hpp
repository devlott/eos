#pragma once

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/restrict.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/types.hpp>
#include <fc/io/cfile.hpp>
#include <fc/io/raw.hpp>

namespace bio = boost::iostreams;

namespace fc {
/// adapt fc::datastream or fc::cfile to boost iostreams Sink Concept
template <typename STREAM>
struct data_sink {
   typedef char          char_type;
   typedef bio::sink_tag category;

   STREAM& ds;
   size_t  write(const char* buf, size_t n) {
      ds.write(buf, n);
      return n;
   }
};

/// adapt boost iostreams filtering_ostreambuf to fc::datastream::write interface
class zlib_compress_ostream {
   bio::filtering_ostreambuf strm;

 public:
   template <typename STREAM>
   zlib_compress_ostream(STREAM& ds)
       : strm(bio::zlib_compressor() | data_sink<STREAM>{ds}) {}

   bool write(const char* buf, size_t n) { return strm.sputn(buf, n) == n; }
};

class cfile_data_source {
 private:
   cfile& file;

 public:
   typedef char            char_type;
   typedef bio::source_tag category;

   cfile_data_source(cfile& f)
       : file(f) {}

   size_t read(char* buf, size_t len) {
      file.read(buf, len);
      return len;
   }
};

class zlib_decompress_istream {
   bio::filtering_ostreambuf strm;

 public:
   template <typename SOURCE>
   zlib_decompress_istream(SOURCE& src)
       : strm(bio::zlib_decompressor() | src) {}

   size_t read(char* buf, size_t len) { return strm.sgetn(buf, len); }
};

template <typename STREAM>
class restriced_data_source {
   STREAM& strm;
   size_t  remaining;

 public:
   typedef char            char_type;
   typedef bio::source_tag category;

   restriced_data_source(STREAM& st, size_t sz)
       : strm(st)
       , remaining(sz) {}
   size_t read(char* buf, size_t n) {
      EOS_ASSERT(n <= remaining, fc::out_of_range_exception, "read datastream over by ${v}",
              ("v", remaining - n));
      remaining -= n;
      return strm.read(buf, n);
   }
};

template <typename STREAM>
restriced_data_source<STREAM> make_restricted_source(STREAM& strm, size_t len) {
   return restriced_data_source<STREAM>(strm, len);
}

} // namespace fc

namespace eosio {
namespace state_history {

using chain::bytes;

bytes zlib_compress_bytes(const bytes& in);
bytes zlib_decompress(const bytes& in);

inline void seek(fc::cfile& file, uint64_t position) { file.seek(position); }
inline void seek(fc::datastream<char*>& ds, uint64_t position) { ds.seekp(position); }

template <typename STREAM>
struct length_writer {
   STREAM&  strm;
   uint64_t start_pos = 0;

   length_writer(STREAM& f)
       : strm(f) {
      uint32_t len = 0;
      strm.write((const char*)&len, sizeof(len));
      start_pos = strm.tellp();
   }

   ~length_writer() {
      uint64_t end_pos = strm.tellp();
      uint32_t len     = end_pos - start_pos;
      seek(strm, start_pos - sizeof(len));
      strm.write((char*)&len, sizeof(len));
      seek(strm, end_pos);
   }
};

template <typename T>
bool is_empty(const T&) {
   return false;
}
template <typename T>
bool is_empty(const std::vector<T>& obj) {
   return obj.empty();
}

template <typename STREAM, typename T>
void zlib_pack(STREAM& strm, const T& object) {
   if (is_empty(object)) {
      fc::raw::pack(strm, uint32_t(0));
   } else {
      length_writer<STREAM>     len_writer(strm);
      fc::zlib_compress_ostream compressed_strm(strm);
      fc::raw::pack(compressed_strm, object);
   }
}

template <typename STREAM, typename T>
void zlib_unpack(STREAM& strm, T& obj) {
   uint32_t len;
   fc::raw::unpack(strm, len);
   if (len > 0) {
      fc::zlib_decompress_istream<STREAM> decompress_strm(make_restricted_source(strm, len));
      fc::raw::unpack(decompress_strm, obj);
   }
}

template <typename Object>
void zlib_unpack(const char* buffer, fc::datastream<const char*>& ds, Object& obj) {
   uint32_t len;
   fc::raw::unpack(ds, len);

   if (len == 0)
      return;

   EOS_ASSERT(ds.remaining() >= len, fc::out_of_range_exception, "read datastream over by ${v}",
              ("v", ds.remaining() - len));

   bytes                     decompressed;
   bio::filtering_ostreambuf strm(bio::zlib_decompressor() | bio::back_inserter(decompressed));
   bio::write(strm, buffer + ds.tellp(), len);
   bio::close(strm);
   ds.skip(len);

   fc::datastream<const char*> decompressed_ds(decompressed.data(), decompressed.size());
   fc::raw::unpack(decompressed_ds, obj);
}

} // namespace state_history
} // namespace eosio
