#include <hpx/hpx.hpp>
#include "xlua.hpp"
#include "xlua_prototypes.hpp"

namespace hpx
{

int create_component(lua_State *L);

inline bool is_bytecode(const std::string& s) {
  return s.size() > 4 && s[0] == 27 && s[1] == 'L' && s[2] == 'u' && s[3] == 'a';
}

struct lua_component
  : hpx::components::simple_component_base<lua_component>
{
  table_ptr tp{new table_inner};

  ptr_type get(std::string name) {
    ptr_type pt{new std::vector<Holder>()};
    pt->push_back((tp->t)[name]);
    return pt;
  }

  HPX_DEFINE_COMPONENT_DIRECT_ACTION(lua_component,get);

  ptr_type set(std::string name,Holder h) {
    ptr_type pt{new std::vector<Holder>()};
    (tp->t)[name] = h;
    return pt;
  }

  HPX_DEFINE_COMPONENT_DIRECT_ACTION(lua_component,set);

  ptr_type call(closure_ptr cp,ptr_type ptargs);

  HPX_DEFINE_COMPONENT_DIRECT_ACTION(lua_component,call);
};

struct lua_client
  : hpx::components::client_base<lua_client, lua_component>
{
  typedef hpx::components::client_base<lua_client, lua_component> base_type;

  lua_client()
  {}

  lua_client(hpx::id_type const & id)
    : base_type(id)
  {}

  lua_client(hpx::future<hpx::id_type> && id)
    : base_type(std::move(id))
  {}

  hpx::future<ptr_type> get(std::string name)
  {
    lua_component::get_action act;
    return hpx::async(act, get_id(), name);
  }

  hpx::future<ptr_type> call(closure_ptr cp,ptr_type ptargs)
  {
    lua_component::call_action act;
    return hpx::async(act, get_id(), cp, ptargs);
  }

  hpx::future<ptr_type> set(std::string name,Holder h) {
    lua_component::set_action act;
    return hpx::async(act, get_id(), name, h);
  }
};

hpx::future<ptr_type> lua_aux_client::get(std::string name)
{
  lua_component::get_action act;
  return hpx::async(act, id, name);
}

hpx::future<ptr_type> lua_aux_client::call(closure_ptr cp,ptr_type ptargs)
{
  lua_component::call_action act;
  return hpx::async(act, id, cp, ptargs);
}

hpx::future<ptr_type> lua_aux_client::set(std::string name,Holder h) {
  lua_component::set_action act;
  return hpx::async(act, id, name, h);
}

ptr_type lua_component::call(closure_ptr cp,ptr_type ptargs) {
  ptr_type pt{new std::vector<Holder>};
  bool found = false;
  if(is_bytecode(cp->code.data)) {
    found = true;
  } else {
    Holder hbyte = (tp->t)[cp->code.data];
    if(hbyte.var.which() == Holder::bytecode_t) {
      cp->code = boost::get<Bytecode>(hbyte.var);
      found = true;
    } else if(hbyte.var.which() == Holder::closure_t) {
      cp = boost::get<closure_ptr>(hbyte.var);
      found = true;
    } else {
      std::cout << "Which = " << hbyte.var.which() << " code=" << cp->code.data << std::endl;
    }
  }
  if(found) {
    LuaEnv lenv;
    lua_State *L = lenv.get_state();
    lua_pop(L,lua_gettop(L));
    lua_load(L,(lua_Reader)lua_read,(void *)&cp->code.data,0,"b");
    new_table(L);
    table_ptr *ntp = (table_ptr *)lua_touserdata(L,-1);
    *ntp = tp;
    for(auto it=ptargs->begin();it != ptargs->end();++it) {
      it->unpack(L);
    }
    if(lua_pcall(L,lua_gettop(L)-1,10,0) != 0) {
      SHOW_ERROR(L);
    }
    while(lua_gettop(L)>1 && lua_isnil(L,-1))
      lua_pop(L,1);
    int nargs = lua_gettop(L);
    for(int i=1;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      pt->push_back(h);
    }
  }
  return pt;
}

int new_component(lua_State *L) {
  size_t nbytes = sizeof(lua_aux_client); 
  char *mem = (char *)lua_newuserdata(L, nbytes);
  luaL_setmetatable(L,lua_client_metatable_name);
  lua_aux_client *lcp = new (mem) lua_aux_client();
  return 1;
}

int create_component(lua_State *L) {
  if(cmp_meta(L,-1,locality_metatable_name)) {
    locality_type *loc = (locality_type *)lua_touserdata(L,-1);
    lua_pop(L,1);
    new_component(L);
    lua_aux_client *lcp = (lua_aux_client *)lua_touserdata(L,-1);
    lua_client lc = hpx::new_<lua_client>(*loc);
    lcp->id = lc.get_id();
    return lua_gettop(L);
  }
  return 0;
}

int hpx_component_clean(lua_State *L) {
    if(cmp_meta(L,-1,lua_client_metatable_name)) {
      lua_aux_client *fnc = (lua_aux_client *)lua_touserdata(L,-1);
      //dtor(fnc);
    }
    return 0;
}

int lua_client_get(lua_State *L) {
    if(lua_isstring(L,-1) && cmp_meta(L,-2,lua_client_metatable_name)) {
      lua_aux_client *lcp = (lua_aux_client *)lua_touserdata(L,-2);
      std::string key = lua_tostring(L,-1);
      lua_pop(L,2);
      new_future(L);
      future_type *fc =
        (future_type *)lua_touserdata(L,-1);
      *fc = lcp->get(key);
      return lua_gettop(L);
    }
    return 0;
}

int lua_client_getid(lua_State *L) {
    if(cmp_meta(L,-1,lua_client_metatable_name)) {
      lua_aux_client *lcp = (lua_aux_client *)lua_touserdata(L,-1);
      lua_pop(L,lua_gettop(L));
      new_locality(L);
      hpx::naming::id_type *tp = (hpx::naming::id_type *)lua_touserdata(L,-1);
      *tp = lcp->id;
      return 1;
    }
    return 0;
}

int lua_client_call(lua_State *L) {
    if(cmp_meta(L,1,lua_client_metatable_name)) {
      lua_aux_client *lcp = (lua_aux_client *)lua_touserdata(L,1);
      closure_ptr cp{new Closure()};
      if(lua_isstring(L,2)) {
        cp->code.data = lua_tostring(L,2);
      } else if(lua_isfunction(L,2)) {
        lua_pushvalue(L,2);
        lua_dump(L,(lua_Writer)lua_write,&cp->code.data);
        lua_pop(L,1);
      }
      ptr_type pt{new std::vector<Holder>};
      int nargs = lua_gettop(L);
      for(int i=3;i<=nargs;i++) {
        Holder h;
        h.pack(L,i);
        pt->push_back(h);
      }
      lua_pop(L,lua_gettop(L));
      new_future(L);
      future_type *fc =
        (future_type *)lua_touserdata(L,-1);
      *fc = lcp->call(cp,pt);
      return 1;
    }
    return 0;
}

int lua_client_set(lua_State *L) {
    if(lua_isstring(L,-2) && cmp_meta(L,-3,lua_client_metatable_name)) {
      lua_aux_client *lcp = (lua_aux_client *)lua_touserdata(L,-3);
      std::string key = lua_tostring(L,-2);
      Holder h;
      h.pack(L,-1);
      future_type ff = lcp->set(key,h);
      lua_pop(L,lua_gettop(L));
      new_future(L);
      future_type *fc =
        (future_type *)lua_touserdata(L,-1);
      *fc = ff;
      return 1;
    }
    return 0;
}

int lua_client_name(lua_State *L) {
  lua_pushstring(L,lua_client_metatable_name);
  return 1;
}

int open_component(lua_State *L) {
    static const struct luaL_Reg component_meta_funcs [] = {
        {"Get",&lua_client_get},
        {"GetId",&lua_client_getid},
        {"Set",&lua_client_set},
        {"Name",&lua_client_name},
        {"Call",&lua_client_call},
        {NULL,NULL},
    };

    static const struct luaL_Reg component_funcs [] = {
        {"new", &create_component},
        {NULL, NULL}
    };

    luaL_newlib(L,component_funcs);

    luaL_newmetatable(L,lua_client_metatable_name);
    luaL_newlib(L, component_meta_funcs);
    lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_component_clean);
    lua_settable(L,-3);

    lua_pop(L,1);

    return 1;
}

}

typedef hpx::components::simple_component<hpx::lua_component> lua_component_type;
HPX_REGISTER_COMPONENT(lua_component_type,lua_component);

HPX_REGISTER_ACTION(hpx::lua_component::get_action);
HPX_REGISTER_ACTION(hpx::lua_component::set_action);
HPX_REGISTER_ACTION(hpx::lua_component::call_action);
