// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string.h>

#include "include/buffer.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/omap_manager/btree/string_kv_node_layout.h"
#include "crimson/os/seastore/omap_manager/btree/omap_types.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node.h"

namespace crimson::os::seastore::omap_manager {

using OMapLeafNodeRef = OMapLeafNode::OMapLeafNodeRef;
using OMapInnerNodeRef = OMapInnerNode::OMapInnerNodeRef;
using mutation_result_t = OMapNode::mutation_result_t;

using location_t = std::optional<
  std::pair<
    OMapInnerNodeRef, //mutable
    OMapInnerNode::internal_iterator_t>>; //iter in mutbale node

using check_status_iertr = OMapManager::base_iertr;
using check_status_ret = check_status_iertr::future<mutation_result_t>;
check_status_ret check_status(
  omap_context_t oc,
  OMapNodeRef e);

using merge_right_to_left_iertr = base_iertr;
using merge_right_to_left_ret = merge_right_to_left_iertr::future<>;
merge_right_to_left_ret merge_right_to_left(
  OMapInnerNodeRef l_ext, //mutable
  OMapInnerNodeRef r_ext, //mutable
  OMapInnerNode::internal_iterator_t l_it,
  OMapInnerNode::internal_iterator_t r_it,
  OMapNodeRef nl_ext,
  OMapNodeRef nr_ext,
  omap_context_t oc);

using layer_mresult_t = std::tuple<
  std::optional<mutation_result_t>, //mll
  std::optional<mutation_result_t>, //ml
  std::optional<mutation_result_t>, //mr
  std::optional<mutation_result_t>>; //mrr

using rm_key_range_iertr = OMapManager::base_iertr;
using rm_key_range_ret = rm_key_range_iertr::future<layer_mresult_t>;
rm_key_range_ret rm_key_range(
  omap_context_t oc,
  std::vector<laddr_t> &&layerq,
  const int depth,
  const std::string &start,
  const std::string &last);

}
