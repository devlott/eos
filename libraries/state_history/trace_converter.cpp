#include <boost/convert.hpp>
#include <eosio/state_history/compression.hpp>
#include <eosio/state_history/serialization.hpp>
#include <eosio/state_history/trace_converter.hpp>
extern const char* state_history_plugin_abi;

namespace eosio {
namespace state_history {

using eosio::chain::packed_transaction;
using eosio::chain::state_history_exception;
using prunable_data_type = packed_transaction::prunable_data_type;

bool is_onblock(const transaction_trace_ptr& p) {
   if (p->action_traces.size() != 1)
      return false;
   auto& act = p->action_traces[0].act;
   if (act.account != eosio::chain::config::system_account_name || act.name != N(onblock) ||
       act.authorization.size() != 1)
      return false;
   auto& auth = act.authorization[0];
   return auth.actor == eosio::chain::config::system_account_name &&
          auth.permission == eosio::chain::config::active_name;
}

void trace_converter::add_transaction(const transaction_trace_ptr& trace, const packed_transaction_ptr& transaction) {
   if (trace->receipt) {
      if (is_onblock(trace))
         onblock_trace.emplace(trace, transaction);
      else if (trace->failed_dtrx_trace)
         cached_traces[trace->failed_dtrx_trace->id] = augmented_transaction_trace{trace, transaction};
      else
         cached_traces[trace->id] = augmented_transaction_trace{trace, transaction};
   }
}

namespace {

std::vector<augmented_transaction_trace> prepare_traces(trace_converter&       converter,
                                                        const block_state_ptr& block_state) {
   std::vector<augmented_transaction_trace> traces;
   if (converter.onblock_trace)
      traces.push_back(*converter.onblock_trace);
   for (auto& r : block_state->block->transactions) {
      transaction_id_type id;
      if (r.trx.contains<transaction_id_type>())
         id = r.trx.get<transaction_id_type>();
      else
         id = r.trx.get<packed_transaction>().id();
      auto it = converter.cached_traces.find(id);
      EOS_ASSERT(it != converter.cached_traces.end() && it->second.trace->receipt, state_history_exception,
                 "missing trace for transaction ${id}", ("id", id));
      traces.push_back(it->second);
   }
   converter.cached_traces.clear();
   converter.onblock_trace.reset();
   return traces;
}

template <typename Lambda>
void for_each_packed_transaction(const eosio::state_history::augmented_transaction_trace& obj, const Lambda& lambda) {
   auto& trace = *obj.trace;
   if (trace.failed_dtrx_trace) {
      for_each_packed_transaction(
          eosio::state_history::augmented_transaction_trace{trace.failed_dtrx_trace, obj.packed_trx}, lambda);
   }
   bool include_packed_trx = obj.packed_trx && !trace.failed_dtrx_trace;
   if (include_packed_trx) {
      lambda(*obj.packed_trx);
   }
}

/// used to traverse every packed_transaction inside traces before the pruned_data has been serialized
template <typename Lambda>
void for_each_packed_transaction(const std::vector<eosio::state_history::augmented_transaction_trace>& traces,
                                 const Lambda&                                                         lambda) {
   for (const auto& v : traces) {
      for_each_packed_transaction(v, lambda);
   }
}

prunable_data_type prune(const prunable_data_type& obj) {
   return obj.prunable_data.visit(
       chain::overloaded{[](const prunable_data_type::none& elem) -> prunable_data_type { return {elem}; },
                         [&obj](const auto& elem) -> prunable_data_type {
                            if (elem.signatures.empty() && elem.context_free_segments.empty())
                               return {elem};
                            return obj.prune_all();
                         }});
}

BOOST_DECLARE_HAS_MEMBER(has_context_free_segments, context_free_segments);

template <typename STREAM>
void pack(STREAM& strm, const prunable_data_type& obj) {
   fc::raw::pack(strm, static_cast<uint8_t>(obj.prunable_data.which()));
   obj.prunable_data.visit(
       eosio::chain::overloaded{[&strm](const prunable_data_type::none& data) { fc::raw::pack(strm, data); },
                                [&strm](const auto& data) {
                                   fc::raw::pack(strm, data.signatures);
                                   zlib_pack(strm, data.context_free_segments);
                                }});
}

template <typename T, std::enable_if_t<!has_context_free_segments<T>::value, int> = 0>
void unpack(const char*, fc::datastream<const char*>& ds, T& obj) {
   fc::raw::unpack(ds, obj);
}

template <typename T, std::enable_if_t<has_context_free_segments<T>::value, int> = 0>
void unpack(const char* read_buffer, fc::datastream<const char*>& ds, T& t) {
   fc::raw::unpack(ds, t.signatures);
   zlib_unpack(read_buffer, ds, t.context_free_segments);
}

void unpack(const char* read_buffer, fc::datastream<const char*>& ds, prunable_data_type& prunable) {
   uint8_t tag;
   fc::raw::unpack(ds, tag);
   prunable.prunable_data.set_which(tag);
   prunable.prunable_data.visit([read_buffer, &ds](auto& v) { unpack(read_buffer, ds, v); });
}

/// used to traverse each trace along with its associated unpacked prunable_data
template <typename Visitor>
void visit_deserialized_trace(const char* read_buffer, fc::datastream<const char*>& ds, transaction_trace& trace,
                              Visitor&& visitor) {
   auto& trace_v0 = trace.get<transaction_trace_v0>();
   if (trace_v0.failed_dtrx_trace.size()) {
      // failed_dtrx_trace have at most one element because it is encoded as an optional
      visit_deserialized_trace(read_buffer, ds, trace_v0.failed_dtrx_trace[0].recurse, std::forward<Visitor>(visitor));
   }
   if (trace_v0.partial) {
      prunable_data_type prunable;
      unpack(read_buffer, ds, prunable);
      visitor(trace_v0, prunable);
   }
}

struct restore_partial {
   partial_transaction_v0& ptrx;
   void                    operator()(prunable_data_type::full_legacy& data) const {
      ptrx.signatures        = std::move(data.signatures);
      ptrx.context_free_data = std::move(data.context_free_segments);
   }
   void operator()(prunable_data_type::none& data) const {}
   void operator()(prunable_data_type::partial& data) const {
      EOS_ASSERT(false, state_history_exception, "Not implemented");
   }
   void operator()(prunable_data_type::full& data) const {
      ptrx.signatures        = std::move(data.signatures);
      ptrx.context_free_data = std::move(data.context_free_segments);
   }
};

struct trace_pruner {
   char*    buffer; /// the address to the traces entry payload, data == read_strm._start == write_strm._start
   uint64_t last_read_pos;
   std::vector<transaction_id_type>& ids;       /// the transaction ids to be pruned
   fc::datastream<const char*>&      read_strm; /// read_strm and write_strm share the same underlying buffer
   fc::datastream<char*>             write_strm;
   uint64_t change_position = 0; /// when it's nonzero, represents the offset to data where the content has been changed

   trace_pruner(bytes& entry_payload, fc::datastream<const char*>& rds, std::vector<transaction_id_type>& ids)
       : buffer(entry_payload.data())
       , last_read_pos(rds.tellp())
       , ids(ids)
       , read_strm(rds)
       , write_strm(buffer, entry_payload.size()) {
      write_strm.skip(last_read_pos);
   }

   /// This member function prunes each trace by overriding the input buffer with the pruned content.
   /// It relies on the fact that the serialized pruned data won't be larger than its un-pruned counterpart
   /// which is subsequently based on:
   ///   1) the presence of context free data requires the presence of signatures;
   ///   2) prune() would never convert it to prunable_data::none when both the context free data and
   ///      signature are empty, which is different from the behavior in prunable_data::prune_all().
   void operator()(transaction_trace_v0& trace, prunable_data_type& incoming_prunable) {
      auto itr = std::find(ids.begin(), ids.end(), trace.id);
      if (itr != ids.end()) {
         // the incoming trace matches the one of ids to be pruned
         if (change_position == 0)
            change_position = write_strm.tellp();
         pack(write_strm, prune(incoming_prunable));
         ids.erase(itr);
      } else if (change_position == 0) {
         // no change so far, skip the original serialized prunable_data bytes
         write_strm.skip(read_strm.tellp() - last_read_pos);
      } else {
         // change detected,  shift the original serialized prunable_data bytes to new location
         write_strm.write(buffer + last_read_pos, read_strm.tellp() - last_read_pos);
      }
      last_read_pos = read_strm.tellp();
   }

   /// @returns the pair of start and end offset to buffer where the content has been changed.
   std::pair<uint64_t, uint64_t> changed_region() {
      if (change_position == 0)
         return {0, 0};
      return {change_position, write_strm.tellp()};
   }
};

void pack_unprunable(fc::cfile& file, const chainbase::database& db, bool trace_debug_mode, uint32_t version,
                     const std::vector<augmented_transaction_trace>& traces) {
   zlib_pack(file, make_history_context_wrapper(
                       db, trace_receipt_context{.debug_mode = trace_debug_mode, .version = version}, traces));
}

} // namespace

void trace_converter::pack(fc::cfile& file, const chainbase::database& db, bool trace_debug_mode,
                           const block_state_ptr& block_state, uint32_t version) {

   auto traces = prepare_traces(*this, block_state);

   if (version == 0) {
      pack_unprunable(file, db, trace_debug_mode, version, traces);
   } else {
      // In version 1 of SHiP traces log disk format, it log entry consists of 3 parts.
      //  1. an uint32_t length for the total byte count of part 2 and 3.
      //  2. a zlib compressed unprunable section contains the serialization of the vector of traces excluding
      //     the prunable_data data (i.e. signatures and context free data)
      //  3. a prunable section contains the serialization of the vector of prunable_data, where all the contained
      //     context_free_segments are zlib compressed.
      length_writer<fc::cfile> len_writer(file);
      pack_unprunable(file, db, trace_debug_mode, version, traces);
      for_each_packed_transaction(
          traces, [&file](const chain::packed_transaction& pt) { state_history::pack(file, pt.get_prunable_data()); });
   }
}

bytes trace_converter::to_traces_bin_v0(const bytes& entry_payload, uint32_t version) {
   if (version == 0)
      return zlib_decompress(entry_payload);
   else {
      fc::datastream<const char*> strm(entry_payload.data(), entry_payload.size());

      std::vector<transaction_trace> traces;
      zlib_unpack(entry_payload.data(), strm, traces);
      for (auto& trace : traces) {
         visit_deserialized_trace(entry_payload.data(), strm, trace,
                                  [](transaction_trace_v0& trace, prunable_data_type& prunable_data) {
                                     auto& ptrx = trace.partial->get<partial_transaction_v0>();
                                     prunable_data.prunable_data.visit(restore_partial{ptrx});
                                  });
      }

      return fc::raw::pack(traces);
   }
}

std::pair<uint64_t, uint64_t> trace_converter::prune_traces(bytes& entry_payload, uint32_t version,
                                                            std::vector<transaction_id_type>& ids) {
   EOS_ASSERT(version > 0, state_history_exception, "state history log version 0 does not support trace pruning");
   fc::datastream<const char*> read_strm(entry_payload.data(), entry_payload.size());
   std::vector<transaction_trace> traces;
   zlib_unpack(entry_payload.data(), read_strm, traces);

   auto prune_trace = trace_pruner(entry_payload, read_strm, ids);
   for (auto& trace : traces) {
      visit_deserialized_trace(entry_payload.data(), read_strm, trace, prune_trace);
   }
   return prune_trace.changed_region();
}

} // namespace state_history
} // namespace eosio
