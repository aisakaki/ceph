// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <optional>
#include <vector>
#include <utility>
#include <functional>

#include <boost/intrusive_ptr.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"

#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/device.h"

namespace crimson::os::seastore {
class Journal;

template <typename F>
auto repeat_eagain(F &&f) {
  return seastar::do_with(
    std::forward<F>(f),
    [](auto &f)
  {
    return crimson::repeat([&f] {
      return std::invoke(f
      ).safe_then([] {
        return seastar::stop_iteration::yes;
      }).handle_error(
        [](const crimson::ct_error::eagain &e) {
          return seastar::stop_iteration::no;
        },
        crimson::ct_error::pass_further_all{}
      );
    });
  });
}

/**
 * TransactionManager
 *
 * Abstraction hiding reading and writing to persistence.
 * Exposes transaction based interface with read isolation.
 */
class TransactionManager : public ExtentCallbackInterface {
public:
  TransactionManager(
    JournalRef journal,
    CacheRef cache,
    LBAManagerRef lba_manager,
    ExtentPlacementManagerRef &&epm,
    BackrefManagerRef&& backref_manager);

  /// Writes initial metadata to disk
  using mkfs_ertr = base_ertr;
  mkfs_ertr::future<> mkfs();

  /// Reads initial metadata from disk
  using mount_ertr = base_ertr;
  mount_ertr::future<> mount();

  /// Closes transaction_manager
  using close_ertr = base_ertr;
  close_ertr::future<> close();

  /// Resets transaction
  void reset_transaction_preserve_handle(Transaction &t) {
    return cache->reset_transaction_preserve_handle(t);
  }

  /**
   * get_pin
   *
   * Get the logical pin at offset
   */
  using get_pin_iertr = LBAManager::get_mapping_iertr;
  using get_pin_ret = LBAManager::get_mapping_iertr::future<LBAPinRef>;
  get_pin_ret get_pin(
    Transaction &t,
    laddr_t offset) {
    LOG_PREFIX(TransactionManager::get_pin);
    SUBTRACET(seastore_tm, "{}", t, offset);
    return lba_manager->get_mapping(t, offset);
  }

  /**
   * get_pins
   *
   * Get logical pins overlapping offset~length
   */
  using get_pins_iertr = LBAManager::get_mappings_iertr;
  using get_pins_ret = get_pins_iertr::future<lba_pin_list_t>;
  get_pins_ret get_pins(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::get_pins);
    SUBDEBUGT(seastore_tm, "{}~{}", t, offset, length);
    return lba_manager->get_mappings(
      t, offset, length);
  }

  /**
   * pin_to_extent
   *
   * Get extent mapped at pin.
   */
  using pin_to_extent_iertr = base_iertr;
  template <typename T>
  using pin_to_extent_ret = pin_to_extent_iertr::future<
    TCachedExtentRef<T>>;
  template <typename T>
  pin_to_extent_ret<T> pin_to_extent(
    Transaction &t,
    LBAPinRef pin) {
    LOG_PREFIX(TransactionManager::pin_to_extent);
    SUBTRACET(seastore_tm, "getting extent {}", t, *pin);
    static_assert(is_logical_type(T::TYPE));
    using ret = pin_to_extent_ret<T>;
    auto &pref = *pin;
    return cache->get_extent<T>(
      t,
      pref.get_val(),
      pref.get_length(),
      [this, pin=std::move(pin)](T &extent) mutable {
	assert(!extent.has_pin());
	assert(!extent.has_been_invalidated());
	assert(!pin->has_been_invalidated());
	extent.set_pin(std::move(pin));
	lba_manager->add_pin(extent.get_pin());
      }
    ).si_then([FNAME, &t](auto ref) mutable -> ret {
      SUBTRACET(seastore_tm, "got extent -- {}", t, *ref);
      assert(ref->has_actual_data());
      return pin_to_extent_ret<T>(
	interruptible::ready_future_marker{},
	std::move(ref));
    });
  }

  /**
   * pin_to_extent_by_type
   *
   * Get extent mapped at pin.
   */
  using pin_to_extent_by_type_ret = pin_to_extent_iertr::future<
    LogicalCachedExtentRef>;
  pin_to_extent_by_type_ret pin_to_extent_by_type(
      Transaction &t,
      LBAPinRef pin,
      extent_types_t type) {
    LOG_PREFIX(TransactionManager::pin_to_extent_by_type);
    SUBTRACET(seastore_tm, "getting extent {} type {}", t, *pin, type);
    assert(is_logical_type(type));
    auto &pref = *pin;
    return cache->get_extent_by_type(
      t,
      type,
      pref.get_val(),
      pref.get_key(),
      pref.get_length(),
      [this, pin=std::move(pin)](CachedExtent &extent) mutable {
        auto &lextent = static_cast<LogicalCachedExtent&>(extent);
        assert(!lextent.has_pin());
        assert(!lextent.has_been_invalidated());
        assert(!pin->has_been_invalidated());
        lextent.set_pin(std::move(pin));
        lba_manager->add_pin(lextent.get_pin());
      }
    ).si_then([FNAME, &t](auto ref) {
      SUBTRACET(seastore_tm, "got extent -- {}", t, *ref);
      return pin_to_extent_by_type_ret(
	interruptible::ready_future_marker{},
	std::move(ref->template cast<LogicalCachedExtent>()));
    });
  }

  /**
   * read_extent
   *
   * Read extent of type T at offset~length
   */
  using read_extent_iertr = get_pin_iertr;
  template <typename T>
  using read_extent_ret = read_extent_iertr::future<
    TCachedExtentRef<T>>;
  template <typename T>
  read_extent_ret<T> read_extent(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::read_extent);
    SUBTRACET(seastore_tm, "{}~{}", t, offset, length);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset, length] (auto pin) {
      if (length != pin->get_length() || !pin->get_val().is_real()) {
        SUBERRORT(seastore_tm,
            "offset {} len {} got wrong pin {}",
            t, offset, length, *pin);
        ceph_assert(0 == "Should be impossible");
      }
      return this->pin_to_extent<T>(t, std::move(pin));
    });
  }

  /**
   * read_extent
   *
   * Read extent of type T at offset
   */
  template <typename T>
  read_extent_ret<T> read_extent(
    Transaction &t,
    laddr_t offset) {
    LOG_PREFIX(TransactionManager::read_extent);
    SUBTRACET(seastore_tm, "{}", t, offset);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset] (auto pin) {
      if (!pin->get_val().is_real()) {
        SUBERRORT(seastore_tm,
            "offset {} got wrong pin {}",
            t, offset, *pin);
        ceph_assert(0 == "Should be impossible");
      }
      return this->pin_to_extent<T>(t, std::move(pin));
    });
  }

  /// Obtain mutable copy of extent
  LogicalCachedExtentRef get_mutable_extent(Transaction &t, LogicalCachedExtentRef ref) {
    LOG_PREFIX(TransactionManager::get_mutable_extent);
    auto ret = cache->duplicate_for_write(
      t,
      ref)->cast<LogicalCachedExtent>();
    if (!ret->has_pin()) {
      SUBDEBUGT(seastore_tm,
	"duplicating extent for write -- {} -> {}",
	t,
	*ref,
	*ret);
      ret->set_pin(ref->get_pin().duplicate());
    } else {
      SUBTRACET(seastore_tm,
	"extent is already duplicated -- {}",
	t,
	*ref);
      assert(ref->is_pending());
      assert(&*ref == &*ret);
    }
    return ret;
  }


  using ref_iertr = LBAManager::ref_iertr;
  using ref_ret = ref_iertr::future<unsigned>;

  /// Add refcount for ref
  ref_ret inc_ref(
    Transaction &t,
    LogicalCachedExtentRef &ref);

  /// Add refcount for offset
  ref_ret inc_ref(
    Transaction &t,
    laddr_t offset);

  /// Remove refcount for ref
  ref_ret dec_ref(
    Transaction &t,
    LogicalCachedExtentRef &ref);

  /// Remove refcount for offset
  ref_ret dec_ref(
    Transaction &t,
    laddr_t offset);

  /// remove refcount for list of offset
  using refs_ret = ref_iertr::future<std::vector<unsigned>>;
  refs_ret dec_ref(
    Transaction &t,
    std::vector<laddr_t> offsets);

  /**
   * alloc_extent
   *
   * Allocates a new block of type T with the minimum lba range of size len
   * greater than laddr_hint.
   */
  using alloc_extent_iertr = LBAManager::alloc_extent_iertr;
  template <typename T>
  using alloc_extent_ret = alloc_extent_iertr::future<TCachedExtentRef<T>>;
  template <typename T>
  alloc_extent_ret<T> alloc_extent(
    Transaction &t,
    laddr_t laddr_hint,
    extent_len_t len,
    placement_hint_t placement_hint = placement_hint_t::HOT) {
    LOG_PREFIX(TransactionManager::alloc_extent);
    SUBTRACET(seastore_tm, "{} len={}, placement_hint={}, laddr_hint={}",
              t, T::TYPE, len, placement_hint, laddr_hint);
    ceph_assert(is_aligned(laddr_hint, epm->get_block_size()));
    auto ext = cache->alloc_new_extent<T>(
      t,
      len,
      placement_hint,
      INIT_GENERATION);
    return lba_manager->alloc_extent(
      t,
      laddr_hint,
      len,
      ext->get_paddr()
    ).si_then([ext=std::move(ext), laddr_hint, &t, FNAME](auto &&ref) mutable {
      ext->set_pin(std::move(ref));
      SUBDEBUGT(seastore_tm, "new extent: {}, laddr_hint: {}", t, *ext, laddr_hint);
      return alloc_extent_iertr::make_ready_future<TCachedExtentRef<T>>(
	std::move(ext));
    });
  }

  std::optional<ceph::bufferptr> get_remapped_buffer(
    Transaction &t,
    paddr_t paddr) {
    LOG_PREFIX(TransactionManager::get_remapped_buffer);
    SUBDEBUGT(seastore_tm, "paddr: {}", t, paddr);
    return cache->get_remapped_buffer(t, paddr);
  }

  /**
   * to_write_addr_info_t
   *
   * The meaning of laddr, paddr, bp is the laddr, paddr, buffer of original
   * extent, which will be remapped to new extents.
   * The meaning of existing_laddr is that the new extent's laddr.
   * The meaning of existing_paddr is that the new extent to be
   * written is the part of exising extent on the disk. existing_paddr
   * must be absolute.
   */
  struct to_write_addr_info_t {
    /// laddr of original extent
    std::optional<laddr_t> laddr;
    /// paddr of original extent
    std::optional<paddr_t> paddr;
    /// laddr of existing extent, or new data extent laddr
    std::optional<laddr_t> existing_laddr;
    /// paddr of existing extent
    std::optional<paddr_t> existing_paddr;
    /// origin extent retirement behaviors
    bool to_retire = false;

    /// content bufferptr of original extent
    std::optional<ceph::bufferptr> bp;

    extent_len_t get_existing_loffset() const {
      return *existing_laddr - *laddr;
    }

    void set_bp(std::optional<ceph::bufferptr> &&bp) {
      this->bp = std::move(bp);
    }

    std::optional<ceph::bufferptr> get_bp() const {
      return bp;
    }

    to_write_addr_info_t(const to_write_addr_info_t &) = default;
    to_write_addr_info_t(to_write_addr_info_t &&) = default;

    /// used when extent_to_write_t.type != EXISTING
    to_write_addr_info_t(laddr_t addr) : existing_laddr(addr) {}

    /// used when extent_to_write_t.type == EXISTING
    to_write_addr_info_t(laddr_t laddr, paddr_t paddr,
      laddr_t existing_laddr, paddr_t existing_paddr, bool to_retire)
       : laddr(laddr), paddr(paddr), existing_laddr(existing_laddr),
      existing_paddr(existing_paddr), to_retire(to_retire) {}
  };

  /**
   * map_existing_extent
   *
   * Retire original extents according to to_retire recorded in addr_info,
   * which is retirement strategy generated in overwrite_plan_t and allocates
   * a new extent at given existing_paddr that must be absolute and fill
   * the extent with existing data if there is.
   * The common usage is that map extent to multiple new extents with neccesary
   * original extents retirement according to information in addr_info,
   * including laddr, paddr, buffer of original extents and the new laddr,
   * paddr of existing extents.
   *
   * Get buffer of original extent outside the map_existing_extent and store it in
   * addr_info is aim to avoid the situation that both lext and rext split and in
   * the same original extent, then we will loss the ref to original extent's
   * buffer after lext remapping with original extent retire.
   * The control flow might be: retire lext(may not) - allocate new lext(may not) -
   * retire rext(may not, maybe the same as last one) - allocate new rext(may not)
   * - remove extents in do_removals(may not), so in order to avoid retiring 
   * remapped extents or duplicate retirement, the retrement strategy might
   * be bother. It is generated in overwrite_plan_t and pass to addr_info
   * here.
   */
  using map_existing_extent_iertr =
    alloc_extent_iertr::extend_ertr<Device::read_ertr>;
  template <typename T>
  using map_existing_extent_ret =
    map_existing_extent_iertr::future<TCachedExtentRef<T>>;
  template <typename T>
  map_existing_extent_ret<T> map_existing_extent(
    Transaction &t,
    to_write_addr_info_t addr_info,
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::map_existing_extent);
    auto laddr = *(addr_info.laddr);
    auto paddr = *(addr_info.paddr);
    auto existing_laddr = *(addr_info.existing_laddr);
    auto existing_paddr = *(addr_info.existing_paddr);
    auto offset = addr_info.get_existing_loffset();
    auto o_bp = addr_info.get_bp();

    // FIXME: existing_paddr can be absolute and pending
    ceph_assert(existing_paddr.is_absolute());

    SUBDEBUGT(seastore_tm, " laddr: {} paddr: {} existing_laddr: {}, "
              "existing_paddr: {} length: {}",
	      t, laddr, paddr, existing_laddr, existing_paddr, length);

    std::optional<ceph::bufferptr> bp = std::nullopt;
    if (o_bp.has_value()) {
      bp = ceph::bufferptr(
        *o_bp,
        offset,
        length);
    }

    // ExtentPlacementManager::alloc_new_extent will make a new
    // (relative/temp) paddr, so make extent directly
    auto ext = CachedExtent::make_cached_extent_ref<T>(std::move(bp));
    ext->init(CachedExtent::extent_state_t::EXIST_CLEAN,
	      existing_paddr,
	      PLACEMENT_HINT_NULL,
	      NULL_GENERATION);

    if (!addr_info.to_retire) {
      t.add_fresh_extent(ext);
      return lba_manager->alloc_extent(
        t,
        existing_laddr,
        length,
        existing_paddr
      ).si_then([this, ext = std::move(ext), existing_laddr,
        length, existing_paddr](auto &&ref) {
        assert(ref->get_key() == existing_laddr);
        assert(ref->get_val() == existing_paddr);
        assert(ref->get_length() == length);
        ext->set_pin(std::move(ref));
        return map_existing_extent_iertr::make_ready_future<TCachedExtentRef<T>>
	  (std::move(ext));
      });
    } else {
      return dec_ref(t, laddr
      ).si_then([this, &t, ext = std::move(ext), existing_laddr, length,
        existing_paddr](auto result) {
        t.add_fresh_extent(ext);
        return lba_manager->alloc_extent(
          t,
          existing_laddr,
          length,
          existing_paddr
        ).si_then([this, ext = std::move(ext), existing_laddr,
          length, existing_paddr](auto &&ref) {
          assert(ref->get_key() == existing_laddr);
          assert(ref->get_val() == existing_paddr);
          assert(ref->get_length() == length);
          ext->set_pin(std::move(ref));
          return map_existing_extent_iertr::make_ready_future<TCachedExtentRef<T>>
	    (std::move(ext));
        });
      });
    }
  }


  using reserve_extent_iertr = alloc_extent_iertr;
  using reserve_extent_ret = reserve_extent_iertr::future<LBAPinRef>;
  reserve_extent_ret reserve_region(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) {
    LOG_PREFIX(TransactionManager::reserve_region);
    SUBDEBUGT(seastore_tm, "len={}, laddr_hint={}", t, len, hint);
    ceph_assert(is_aligned(hint, epm->get_block_size()));
    return lba_manager->alloc_extent(
      t,
      hint,
      len,
      P_ADDR_ZERO);
  }

  /* alloc_extents
   *
   * allocates more than one new blocks of type T.
   */
   using alloc_extents_iertr = alloc_extent_iertr;
   template<class T>
   alloc_extents_iertr::future<std::vector<TCachedExtentRef<T>>>
   alloc_extents(
     Transaction &t,
     laddr_t hint,
     extent_len_t len,
     int num) {
     LOG_PREFIX(TransactionManager::alloc_extents);
     SUBDEBUGT(seastore_tm, "len={}, laddr_hint={}, num={}",
               t, len, hint, num);
     return seastar::do_with(std::vector<TCachedExtentRef<T>>(),
       [this, &t, hint, len, num] (auto &extents) {
       return trans_intr::do_for_each(
                       boost::make_counting_iterator(0),
                       boost::make_counting_iterator(num),
         [this, &t, len, hint, &extents] (auto i) {
         return alloc_extent<T>(t, hint, len).si_then(
           [&extents](auto &&node) {
           extents.push_back(node);
         });
       }).si_then([&extents] {
         return alloc_extents_iertr::make_ready_future
                <std::vector<TCachedExtentRef<T>>>(std::move(extents));
       });
     });
  }

  /**
   * submit_transaction
   *
   * Atomically submits transaction to persistence
   */
  using submit_transaction_iertr = base_iertr;
  submit_transaction_iertr::future<> submit_transaction(Transaction &);

  /**
   * flush
   *
   * Block until all outstanding IOs on handle are committed.
   * Note, flush() machinery must go through the same pipeline
   * stages and locks as submit_transaction.
   */
  seastar::future<> flush(OrderingHandle &handle);

  /*
   * ExtentCallbackInterface
   */

  /// weak transaction should be type READ
  TransactionRef create_transaction(
      Transaction::src_t src,
      const char* name,
      bool is_weak=false) final {
    return cache->create_transaction(src, name, is_weak);
  }

  using ExtentCallbackInterface::submit_transaction_direct_ret;
  submit_transaction_direct_ret submit_transaction_direct(
    Transaction &t,
    std::optional<journal_seq_t> seq_to_trim = std::nullopt) final;

  using ExtentCallbackInterface::get_next_dirty_extents_ret;
  get_next_dirty_extents_ret get_next_dirty_extents(
    Transaction &t,
    journal_seq_t seq,
    size_t max_bytes) final;

  using ExtentCallbackInterface::rewrite_extent_ret;
  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent,
    rewrite_gen_t target_generation,
    sea_time_point modify_time) final;

  using ExtentCallbackInterface::get_extents_if_live_ret;
  get_extents_if_live_ret get_extents_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t paddr,
    laddr_t laddr,
    extent_len_t len) final;

  /**
   * read_root_meta
   *
   * Read root block meta entry for key.
   */
  using read_root_meta_iertr = base_iertr;
  using read_root_meta_bare = std::optional<std::string>;
  using read_root_meta_ret = read_root_meta_iertr::future<
    read_root_meta_bare>;
  read_root_meta_ret read_root_meta(
    Transaction &t,
    const std::string &key) {
    return cache->get_root(
      t
    ).si_then([&key, &t](auto root) {
      LOG_PREFIX(TransactionManager::read_root_meta);
      auto meta = root->root.get_meta();
      auto iter = meta.find(key);
      if (iter == meta.end()) {
        SUBDEBUGT(seastore_tm, "{} -> nullopt", t, key);
	return seastar::make_ready_future<read_root_meta_bare>(std::nullopt);
      } else {
        SUBDEBUGT(seastore_tm, "{} -> {}", t, key, iter->second);
	return seastar::make_ready_future<read_root_meta_bare>(iter->second);
      }
    });
  }

  /**
   * update_root_meta
   *
   * Update root block meta entry for key to value.
   */
  using update_root_meta_iertr = base_iertr;
  using update_root_meta_ret = update_root_meta_iertr::future<>;
  update_root_meta_ret update_root_meta(
    Transaction& t,
    const std::string& key,
    const std::string& value) {
    LOG_PREFIX(TransactionManager::update_root_meta);
    SUBDEBUGT(seastore_tm, "seastore_tm, {} -> {}", t, key, value);
    return cache->get_root(
      t
    ).si_then([this, &t, &key, &value](RootBlockRef root) {
      root = cache->duplicate_for_write(t, root)->cast<RootBlock>();

      auto meta = root->root.get_meta();
      meta[key] = value;

      root->root.set_meta(meta);
      return seastar::now();
    });
  }

  /**
   * read_onode_root
   *
   * Get onode-tree root logical address
   */
  using read_onode_root_iertr = base_iertr;
  using read_onode_root_ret = read_onode_root_iertr::future<laddr_t>;
  read_onode_root_ret read_onode_root(Transaction &t) {
    return cache->get_root(t).si_then([&t](auto croot) {
      LOG_PREFIX(TransactionManager::read_onode_root);
      laddr_t ret = croot->get_root().onode_root;
      SUBTRACET(seastore_tm, "{}", t, ret);
      return ret;
    });
  }

  /**
   * write_onode_root
   *
   * Write onode-tree root logical address, must be called after read.
   */
  void write_onode_root(Transaction &t, laddr_t addr) {
    LOG_PREFIX(TransactionManager::write_onode_root);
    SUBDEBUGT(seastore_tm, "{}", t, addr);
    auto croot = cache->get_root_fast(t);
    croot = cache->duplicate_for_write(t, croot)->cast<RootBlock>();
    croot->get_root().onode_root = addr;
  }

  /**
   * read_collection_root
   *
   * Get collection root addr
   */
  using read_collection_root_iertr = base_iertr;
  using read_collection_root_ret = read_collection_root_iertr::future<
    coll_root_t>;
  read_collection_root_ret read_collection_root(Transaction &t) {
    return cache->get_root(t).si_then([&t](auto croot) {
      LOG_PREFIX(TransactionManager::read_collection_root);
      auto ret = croot->get_root().collection_root.get();
      SUBTRACET(seastore_tm, "{}~{}",
                t, ret.get_location(), ret.get_size());
      return ret;
    });
  }

  /**
   * write_collection_root
   *
   * Update collection root addr
   */
  void write_collection_root(Transaction &t, coll_root_t cmroot) {
    LOG_PREFIX(TransactionManager::write_collection_root);
    SUBDEBUGT(seastore_tm, "{}~{}",
              t, cmroot.get_location(), cmroot.get_size());
    auto croot = cache->get_root_fast(t);
    croot = cache->duplicate_for_write(t, croot)->cast<RootBlock>();
    croot->get_root().collection_root.update(cmroot);
  }

  extent_len_t get_block_size() const {
    return epm->get_block_size();
  }

  store_statfs_t store_stat() const {
    return epm->get_stat();
  }

  ~TransactionManager();

private:
  friend class Transaction;

  CacheRef cache;
  LBAManagerRef lba_manager;
  JournalRef journal;
  ExtentPlacementManagerRef epm;
  BackrefManagerRef backref_manager;

  WritePipeline write_pipeline;

  rewrite_extent_ret rewrite_logical_extent(
    Transaction& t,
    LogicalCachedExtentRef extent);

  submit_transaction_direct_ret do_submit_transaction(
    Transaction &t,
    ExtentPlacementManager::dispatch_result_t dispatch_result,
    std::optional<journal_seq_t> seq_to_trim = std::nullopt);

public:
  // Testing interfaces
  auto get_epm() {
    return epm.get();
  }

  auto get_lba_manager() {
    return lba_manager.get();
  }

  auto get_backref_manager() {
    return backref_manager.get();
  }

  auto get_cache() {
    return cache.get();
  }
  auto get_journal() {
    return journal.get();
  }
};
using TransactionManagerRef = std::unique_ptr<TransactionManager>;

TransactionManagerRef make_transaction_manager(
    Device *primary_device,
    const std::vector<Device*> &secondary_devices,
    bool is_test);
}
