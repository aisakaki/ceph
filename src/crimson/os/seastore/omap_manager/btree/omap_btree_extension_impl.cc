// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <string.h>
#include "include/buffer.h"
#include "include/byteorder.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_extension_impl.h"
#include "seastar/core/thread.hh"

SET_SUBSYS(seastore_omap);

namespace crimson::os::seastore::omap_manager {

check_status_ret
check_status(
  omap_context_t oc,
  OMapNodeRef e)
{
  LOG_PREFIX(BtreeOMapManager::check_status);
  DEBUGT("this: {}", oc.t, *this);
  if (e->extent_is_below_min()) {
    return check_status_ret(
      interruptible::ready_future_marker{},
      mutation_result_t(mutation_status_t::NEED_MERGE,
      std::nullopt, this));
  } else if (e->extent_is_overflow()) {
      return e->make_split_children(oc
      ).si_then([this, oc](auto tuple) {
          return check_status_ret(
            interruptible::ready_future_marker{},
            mutation_result_t(mutation_status_t::WAS_SPLIT, tuple, 
            std::nullopt));
      });
  } else {
      return check_status_ret(
        interruptible::ready_future_marker{},
        mutation_result_t(mutation_status_t::SUCCESS,
        std::nullopt, std::nullopt));
  }
}

merge_right_to_left_ret
merge_right_to_left(
  OMapInnerNodeRef l_ext,
  OMapInnerNodeRef r_ext,
  OMapInnerNode::internal_iterator_t l_it,
  OMapInnerNode::internal_iterator_t r_it,
  OMapNodeRef nl_ext,
  OMapNodeRef nr_ext,
  omap_context_t oc)
{
  //might cause r overflow, must check_status finally.
  LOG_PREFIX(BtreeOMapManager::merge_right_to_left);
  DEBUGT("merge {}(parent: {}) to {} (parent: {})",
    oc.t, *nr_ext, *r_ext, *nl_ext, *l_ext);
  assert(l_ext->is_pending() && r_ext->is_pending());

  if (nl_ext->can_merge(nr_ext)) {
    DEBUGT("make_full_merge l {} r {}", oc.t, *nl_ext, *nr_ext);
    return nl_ext->make_full_merge(oc, nr_ext
    ).si_then([l_it, r_it, l_ext, r_ext, nl_ext, nr_ext, oc]
              (auto &&replacement) {
      LOG_PREFIX(BtreeOMapManager::merge_right_to_left);
      DEBUGT("to update parent: {}", oc.t, *l_ext);
      l_ext->journal_inner_update(
        l_it,
        replacement->get_laddr(),
        l_ext->maybe_get_delta_buffer());
      DEBUGT("to remove from parent: {}", oc.t, *r_ext);
      r_ext->journal_inner_remove(r_it, r_ext->maybe_get_delta_buffer());
      //retire extent
      std::vector<laddr_t> dec_laddrs {nl_ext->get_laddr(), nr_ext->get_laddr()};
      --(oc.t.get_omap_tree_stats().extents_num_delta);
      return dec_ref(oc, dec_laddrs);
    });
  } else {
      return nl_ext->make_balanced(oc, nr_ext
      ).si_then([l_it, r_it, l_ext, r_ext, nl_ext, nr_ext, oc](auto tuple) {
        LOG_PREFIX(BtreeOMapManager::merge_right_to_left);
        DEBUGT("balance r {} to l {} and update parents: {}, {}", oc.t,
          *nl_ext, *nr_ext, *l_ext, *r_ext);
        auto [replacement_nl, replacement_nr, replacement_pivot] = tuple;
        l_ext->journal_inner_update(
          l_it,
          replacement_nl->get_laddr(),
          l_ext->maybe_get_delta_buffer());
        //[TODO] do not real insert it.
        r_ext->journal_inner_remove(r_it, r_ext->maybe_get_delta_buffer());
        r_ext->journal_inner_insert(
          r_it,
          replacement_nr->get_laddr(),
          replacement_pivot,
          r_ext->maybe_get_delta_buffer());
        ++(oc.t.get_omap_tree_stats().extents_num_delta);
        std::vector<laddr_t> dec_laddrs{nl_ext->get_laddr(), nr_ext->get_laddr()};
        return dec_ref(oc, dec_laddrs);
      });
  }
}

rm_key_range_ret
rm_key_range(
  omap_context_t oc,
  std::list<laddr_t> &&layerq,
  const int depth,
  const std::string &start,
  const std::string &last)
{
  LOG_PREFIX(BtreeOMapManager::rm_key_range);
  DEBUGT("start: {}, last: {}", oc.t, start, last);
  location_t l_loc, r_loc, ll_loc, rr_loc;
  std::vector<OMapInnerNodeRef> middleq;
  std::vector<laddr_t> nlayerq;
  std::vector<OMapInnerNodeRef> extents;

  return seastar::do_with(
    std::move(layerq),
    std::move(l_loc),
    std::move(r_loc),
    std::move(ll_loc),
    std::move(rr_loc),
    std::move(middleq),
    std::move(nlayerq),
    std::move(extents),
    start,
    last,
    depth,
    [oc](
      auto &layerq,
      auto &l_loc,
      auto &r_loc,
      auto &ll_loc,
      auto &rr_loc,
      auto &middleq,
      auto &nlayerq,
      auto &extents,
      auto &start,
      auto &last,
      auto &depth
    )
  {
    return trans_intr::do_for_each(
      layerq.begin(),
      layerq.end(),
      [&, oc](laddr_t laddr) {
      return omap_load_extent(oc, laddr, depth
      ).si_then([&](auto extent) {
        extents.push_back(extent->template cast<OMapInnerNode>());
        });
      }).si_then([&, oc] {
      LOG_PREFIX(BtreeOMapManager::rm_key_range);
      //[TODO] check or spearate logic here.
      auto ext_it = --extents.end();
      while (ext_it >= extents.begin()) {
        auto extent = *ext_it;
        OMapInnerNode::iterator key_it = --extent->iter_end();
        for (; key_it >= extent->iter_begin(); key_it--) {
          if (!r_loc) {
            if (last >= key_it->get_key()) {
              auto r_ext_mut = oc.tm.get_mutable_extent(oc.t, extent
                )->template cast<OMapInnerNode>();
              *ext_it = r_ext_mut;
              auto r_it_mut = r_ext_mut->iter_idx(key_it->get_index());
              r_loc = std::make_pair(r_ext_mut, r_it_mut);
	            
              //find candicates
              if (key_it + 1 != extent->iter_end()) {
                rr_loc = std::make_pair(r_ext_mut, r_it_mut + 1);
              } else if (ext_it + 1 != extents.end()) {
                auto rr_ext_mut = oc.tm.get_mutable_extent(oc.t, *(ext_it + 1)
                  )->template cast<OMapInnerNode>();
                auto rr_it_mut = rr_ext_mut->iter_begin();
                rr_loc = std::make_pair(rr_ext_mut, rr_it_mut);
                nlayerq.push_back(rr_it_mut->get_val()); //add nrr
              } else {
                rr_loc = std::nullopt;
              }
              nlayerq.push_back(r_it_mut->get_val()); //add nr
            }
          } else if (!l_loc) {
            nlayerq.push_back(key_it->get_val()); //add middle, add nl
            if (start >= key_it->get_key()) {
              auto l_ext_mut = oc.tm.get_mutable_extent(oc.t, extent
                )->template cast<OMapInnerNode>();
              *ext_it = l_ext_mut;
              auto l_it_mut = l_ext_mut->iter_idx(key_it->get_index());
              l_loc = std::make_pair(l_ext_mut, l_it_mut);

              if (key_it != extent->iter_begin()) {
                ll_loc = std::make_pair(l_ext_mut, l_it_mut - 1);
              } else if (ext_it != extents.begin()) {
                auto ll_ext_mut = oc.tm.get_mutable_extent(oc.t, *(ext_it - 1)
                  )->template cast<OMapInnerNode>();
                auto ll_it_mut = ll_ext_mut->iter_end() - 1;
                ll_loc = std::make_pair(ll_ext_mut, ll_it_mut);
                nlayerq.push_back(ll_it_mut->get_val()); //add nll
              } else {
                ll_loc = std::nullopt;
              }
	          }
	        }
	      }
	      ext_it--;
      }
      //[TODO] realize directly retiring in !l_loc situation.? is it possible?
      if (!l_loc) {
        DEBUGT("range start: {} < min key: {}.",
          oc.t, start, extents[0]->iter_begin());
        auto l_ext_mut = oc.tm.get_mutable_extent(oc.t, extents[0]
          )->template cast<OMapInnerNode>();
        extents[0] = l_ext_mut;
        auto l_it_mut = l_ext_mut->iter_begin();
        l_loc = std::make_pair(l_ext_mut, l_it_mut);
        ll_loc = std::nullopt;
      }
      if (!r_loc) {
        DEBUGT("range last: {} < min key: {}, rm stopped.", oc.t, last,
          extents[0]->iter_begin().get_key());
        return rm_key_range_iertr::now();
      }
      std::reverse(nlayerq.begin(), nlayerq.end());
      DEBUGT("depth: {}, left bound: {}, right bound: {},\
        left candicate: {}, right candicate: {}", oc.t, depth,
        l_loc->second.get_key(),
        r_loc->second.get_key(),
        ll_loc ? ll_loc->second.get_key() : "NONE",
        rr_loc ? rr_loc->second.get_key() : "NONE");
      //To save memory, retire the middle extents before recursing into next
      //layer. [TODO] We can realize retiring extent one by one just after 
      //loading it and reading necessary information.
      //[TODO] when range start is the begin key of a extent, we can set this 
      //bound as middle extents and directly retire it.
      auto l_ext = l_loc->first;
      auto r_ext = r_loc->first;
      get_middle(l_ext, r_ext, extents, middleq);
      DEBUGT("depth: {}, middle nodes num: {}", oc.t, depth, middleq.size());
      return trans_intr::do_for_each(
        middleq.begin(),
        middleq.end(),
        [&, oc](auto extent) {
        return dec_ref(oc, extent->get_laddr()
        ).si_then([oc] {
          --(oc.t.get_omap_tree_stats().extents_num_delta);
        });
      }).si_then([&, oc] {
        depth--;
        if (depth > 1) {
          return rm_key_range(oc, std::move(nlayerq), depth, start, last);
        } else {
          LOG_PREFIX(BtreeOMapManager::rm_key_range);
          assert(depth == 1);
          //[FIXME]PROBLEM : WHEN REMOVE LEAFNODE KEY, WE SHOULD:
          //--(oc.t.get_omap_tree_stats().num_erases);
          //BUT that cannot realize when we don't load middle nodes.
          //HOW can i know size of extents without loading it?
          //or, we don't count key erase from extents that directly retire?
          laddr_t nl_laddr = l_loc->second.get_val();
          laddr_t nr_laddr = r_loc->second.get_val();
          std::vector<laddr_t> middleq_laddr;
          get_middle(nl_laddr, nr_laddr, nlayerq, middleq_laddr);
          DEBUGT("depth: {}, leaf nodes layer, middle nodes num: {}"
            , oc.t, depth, middleq_laddr.size());
          OMapLeafNodeRef nl_ext, nr_ext;
          return seastar::do_with(
            std::move(middleq_laddr),
            nl_laddr,
            nr_laddr,
            nl_ext,
            nr_ext,
            [&, oc](
              auto &middleq_laddr,
              auto &nl_laddr,
              auto &nr_laddr,
              auto &nl_ext,
              auto &nr_ext) mutable
          {
            //retire middle extents
            return trans_intr::do_for_each(
              middleq_laddr.begin(),
              middleq_laddr.end(),
              [oc](laddr_t laddr) {
              return dec_ref(oc, laddr
              ).si_then([oc] {
                --(oc.t.get_omap_tree_stats().extents_num_delta);
              });
            }).si_then([&, oc] {
              //find and rm keys in [nl_it , nr_it] in bound leafnodes.
              return omap_load_extent(oc, nl_laddr, depth
              ).si_then([&](auto extent) {
                LOG_PREFIX(BtreeOMapManager::rm_key_range);
                nl_ext = oc.tm.get_mutable_extent(
                  oc.t, extent)->template cast<OMapLeafNode>();
                if (nl_laddr == nr_laddr) {
                  auto nl_it = nl_ext->string_lower_bound(start);
                  auto nr_it = nl_ext->string_upper_bound(last) - 1;
                  DEBUGT("depth: {}, leaf nodes layer, rm all target keys: {} ~ {} \
                    in one leaf node", oc.t, depth,
                    nl_it->get_key(),
                    nr_it->get_key());
                  nl_ext->journal_leaf_range_remove(
                    nl_it, nr_it + 1,
                    nl_ext->maybe_get_delta_buffer());
                } else {
                  return omap_load_extent(oc, nr_laddr, depth
                  ).si_then([&](auto extent) {
                    LOG_PREFIX(BtreeOMapManager::rm_key_range);
                    nr_ext = oc.tm.get_mutable_extent(
                      oc.t, extent)->template cast<OMapLeafNode>();
                    auto nl_it = nl_ext->string_lower_bound(start);
                    auto nr_it = nr_ext->string_upper_bound(last) - 1;
                    DEBUGT("depth: {}, leaf nodes layer, rm target keys: {} ~ {} \
                      in left bound leaf node, rm target keys: {} ~ {} in right \
                      bound leaf node.", oc.t, depth,
                      nl_it->get_key(), (nl_ext->iter_end()-1)->get_key(),
                      nr_ext->iter_begin()->get_key(), nr_it->get_key());
                    nl_ext->journal_leaf_range_remove(
                      nl_it, nl_ext->iter_end(),
                      nl_ext->maybe_get_delta_buffer());
                    nr_ext->journal_leaf_range_remove(
                      nr_ext->iter_begin(), nr_it + 1,
                      nr_ext->maybe_get_delta_buffer());
                  });
                }
              });
            }).si_then([&, oc] {
              std::optional<mutation_result_t> ml, mr;
              ml = check_status(oc, nl_ext);
              mr = nl_laddr == nr_laddr ? std::nullopt : check_status(oc, nr_ext);
              return rm_key_range_ret(
                interruptible::ready_future_marker{},
                std::make_tuple(std::nullopt, ml, mr, std::nullopt));
            });
	        });
	      }
      }).si_then(
        [&, oc](auto nlayer_mresult) {
        LOG_PREFIX(BtreeOMapManager::rm_key_range);
        //rm keys in (l_it , r_it) in this layer's bound
        //l_it, r_it will be adjusted later so will not be erased now
        assert(l_loc && r_loc);
        assert(depth > 1);
        auto [l_ext, l_it] = *l_loc;
        auto [r_ext, r_it] = *r_loc;
        if (l_ext == r_ext) {
          if (r_it - l_it > 1) {
            DEBUGT("depth: {}, rm middle keys: {} ~ {} in one node", oc.t, depth,
              (l_it + 1)->get_key(),
              (r_it - 1)->get_key());
            l_ext->journal_inner_range_remove(
              l_it + 1, r_it,
              l_ext->maybe_get_delta_buffer());
              r_loc->second = r_it = l_it + 1;
            //update location
            if (rr_loc) {
              rr_loc->second = rr_loc->first->iter_idx((r_it + 1)->get_index());
              assert(rr_loc->second < rr_loc->first.iter_end());
            }
          } else {
            DEBUGT("depth: {}, no middle keys in one node", oc.t, depth);
          }
        } else {
          DEBUGT("depth: {}, rm middle keys: {} ~ {} in left bound node, \
            rm middle keys: {} ~ {} in right bound node.", oc.t, depth,
            (l_it + 1)->get_key(), (l_ext->iter_end() - 1)->get_key(),
            r_ext->iter_begin()->get_key(), (r_it - 1)->get_key());
          l_ext->journal_inner_range_remove(
            l_it + 1, l_ext->iter_end(),
            l_ext->maybe_get_delta_buffer());
          r_ext->journal_inner_range_remove(
            r_ext->iter_begin(), r_it,
            r_ext->maybe_get_delta_buffer());
          l_loc->second = l_it = l_ext->iter_end() - 1;
          r_loc->second = r_it = r_ext->iter_begin();
          //update location
          if (ll_loc) {
            ll_loc->second = ll_loc->first->iter_idx((l_it - 1)->get_index());
            assert(ll_loc->second >= ll_loc->first.iter_begin());
          }
          if (rr_loc) {
            rr_loc->second = rr_loc->first->iter_idx((r_it + 1)->get_index());
            assert(rr_loc->second < rr_loc->first.iter_end());
          }
        }

        //[TODO, here. start adjust]
        //adjust next layer bounds according to the mresults.
        auto [mnll, mnl, mnr, mnrr] = nlayer_mresult;
        return seastar::do_with(
          std::move(nl_mresult),
          std::move(nr_mresult),
          [&, oc](auto &nl_mresult, auto &nr_mresult) { 
          return [&, oc] {
 
          }().si_then([&, oc] {
          
          });
        });


      });
    });
  }); //end do_with
}

}
