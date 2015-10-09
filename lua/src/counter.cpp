#include "xlua.hpp"
#include "xlua_prototypes.hpp"
#include <hpx/include/performance_counters.hpp>

namespace hpx {

bool discover_callback(table_ptr tp,hpx::performance_counters::counter_info const& c,hpx::error_code& ec) {
  table_ptr subtp{new table_inner()};
  (subtp->t)["fullname_"].var = c.fullname_;
  (subtp->t)["helptext_"].var = c.helptext_;
  (subtp->t)["unit_of_measure_"].var = c.unit_of_measure_;
  (subtp->t)["version_"].var = c.version_;
  (subtp->t)["type_"].var = c.type_;
  (subtp->t)["status_"].var = c.status_;
  (tp->t)[tp->size++].var = subtp;
  return true;
}

int xlua_get_counter(lua_State *L) {
  if(lua_isstring(L,-1)) {
    table_ptr& tp = *(table_ptr *)lua_touserdata(L,-1);
    hpx::performance_counters::counter_info c;
    c.fullname_ = lua_tostring(L,-1);
    new_locality(L);
    hpx::naming::id_type *fnc = (hpx::naming::id_type *)lua_touserdata(L,-1);
    *fnc = hpx::performance_counters::get_counter(c);
  } else if(cmp_meta(L,-1,table_metatable_name)) {
    table_ptr& tp = *(table_ptr *)lua_touserdata(L,-1);
    hpx::performance_counters::counter_info c;
    c.fullname_ = boost::get<std::string>((tp->t)["fullname_"].var);
    new_locality(L);
    hpx::naming::id_type *fnc = (hpx::naming::id_type *)lua_touserdata(L,-1);
    *fnc = hpx::performance_counters::get_counter(c);
    return 1;
  }
  return 0;
}

int xlua_get_value(lua_State *L) {
  if(lua_gettop(L)==1 && cmp_meta(L,-1,locality_metatable_name)) {
    hpx::naming::id_type *fnc = (hpx::naming::id_type *)lua_touserdata(L,-1);
    hpx::performance_counters::counter_value value =
      hpx::performance_counters::stubs::performance_counter::get_value(*fnc);
    lua_pop(L,lua_gettop(L));
    new_table(L);
    table_ptr& tp = *(table_ptr *)lua_touserdata(L,-1);
    (tp->t)["time_"].var = value.time_;
    (tp->t)["count_"].var = value.count_;
    (tp->t)["value_"].var = value.value_;
    (tp->t)["scaling_"].var = value.scaling_;
    (tp->t)["status_"].var = value.status_;
    (tp->t)["scale_inverse_"].var = value.scale_inverse_;
    return 1;
  }
  return 0;
}

int xlua_start(lua_State *L) {
  if(lua_gettop(L)==1 && cmp_meta(L,-1,locality_metatable_name)) {
    hpx::naming::id_type *fnc = (hpx::naming::id_type *)lua_touserdata(L,-1);
    bool res = hpx::performance_counters::stubs::performance_counter::start(*fnc);
    lua_pushboolean(L,res);
    return 1;
  }
  return 0;
}

int xlua_stop(lua_State *L) {
  if(lua_gettop(L)==1 && cmp_meta(L,-1,locality_metatable_name)) {
    hpx::naming::id_type *fnc = (hpx::naming::id_type *)lua_touserdata(L,-1);
    bool res = hpx::performance_counters::stubs::performance_counter::start(*fnc);
    lua_pushboolean(L,res);
    return 1;
  }
  return 0;
}

int discover(lua_State *L) {
  new_table(L);
  table_ptr& tp = *(table_ptr *)lua_touserdata(L,-1);
  tp->size = 1;
  hpx::performance_counters::discover_counter_types(boost::bind(discover_callback,tp,_1,_2));
  return 1;
}

}
