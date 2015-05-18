#include "xlua.hpp"
#include <hpx/lcos/broadcast.hpp>

#define CHECK_STRING(INDEX,NAME) \
  if(!lua_isstring(L,INDEX)) { \
    luai_writestringerror("Argument to '%s' is not a string ",NAME);\
    return 0; \
  }

namespace hpx {

LuaEnv::LuaEnv() {
  ptr = get_lua_ptr();
  if(ptr->busy) {
    std::cout << "Busy" << std::endl;
    abort();
  }
  ptr->busy = true;
  L = ptr->get_state();
}
LuaEnv::~LuaEnv() {
  ptr->busy = false;
  set_lua_ptr(ptr);
}

const char *future_metatable_name = "hpx_future";
const char *guard_metatable_name = "hpx_guard";
const char *locality_metatable_name = "hpx_locality";

guard_type global_guarded{new Guard()};

std::ostream& operator<<(std::ostream& o,const Holder& h) {
  if(h.var.which() == Holder::num_t)
    return o << "Num(" << boost::get<double>(h.var) << ")";
  if(h.var.which() == Holder::str_t)
    return o << "Str(" << boost::get<std::string>(h.var) << ")";
  if(h.var.which() == Holder::fut_t)
    return o << "Fut()";
  if(h.var.which() == Holder::table_t)
    return o << "Table()";
  if(h.var.which() == Holder::ptr_t)
    return o << "Vector()";
  return o << "Empty()";
}

//--- Transfer lua bytecode to/from a std:string
int lua_write(lua_State *L,const char *str,unsigned long len,luaL_Buffer *buf) {
    luaL_addlstring(buf,(const char *)str,len);
    return 0;
}

const char *lua_read(lua_State *L,void *data,size_t *size) {
    std::string *rbuf = (std::string*)data;
    (*size) = rbuf->size();
    return rbuf->c_str();
}

//--- Debugging utility, print the Lua stack
std::ostream& show_stack(std::ostream& o,lua_State *L,int line,bool recurse) {
    int n = lua_gettop(L);
    if(!recurse)
      o << "RESTACK:n=" << n << std::endl;
    else
      o << "STACK:n=" << n << " line=" << line << std::endl;
    for(int i=1;i<=n;i++) {
        if(lua_isnil(L,i)) OUT(i,"nil");
        else if(lua_isnumber(L,i)) OUT(i,lua_tonumber(L,i));
        else if(lua_isstring(L,i)) OUT(i,lua_tostring(L,i));
        else if(lua_isboolean(L,i)) OUT(i,lua_toboolean(L,i));
        else if(lua_isfunction(L,i)) OUT(i,"function");
        else if(lua_iscfunction(L,i)) OUT(i,"c-function");
        else if(lua_isthread(L,i)) OUT(i,"thread");
        else if(lua_isuserdata(L,i)) OUT(i,"userdata");
        else if(lua_istable(L,i)) {
          o << i << "] table" << std::endl;
          if(recurse) {
            int nn = lua_gettop(L);
            lua_pushvalue(L,i);
            lua_pushnil(L);
            while(lua_next(L,-2) != 0) {
              lua_pushvalue(L,-2);
              std::string value = "?";
              if(lua_isnumber(L,-2)) {
                value = "num: ";
                value += lua_tostring(L,-2);
              } else if(lua_isstring(L,-2)) {
                value = "string: ";
                value += lua_tostring(L,-2);
              } else if(lua_isboolean(L,-2)) {
                value = "bool: ";
                value += lua_tostring(L,-2);
              } else if(lua_iscfunction(L,-2)) {
                value = "c-function";
              }
              const char *key = lua_tostring(L,-1);
              lua_pop(L,2);
              o << "  key=" << key << " value=" << value << std::endl;
            }
            if(lua_gettop(L) > nn)
              lua_pop(L,lua_gettop(L)-nn);
          }
        } else OUT(i,"other");
    }
    if(!recurse)
      o << "END-RESTACK" << std::endl;
    else
      o << "END-STACK" << std::endl;
    o << std::endl;
    return o;
}

//--- Synchronization for the function registry process
std::map<std::string,std::string> function_registry;

#include <hpx/util/thread_specific_ptr.hpp>
struct lua_interpreter_tag {};
hpx::util::thread_specific_ptr<
    LuaHolder,
    lua_interpreter_tag
> lua_ptr;

//--- Methods for getting/setting the Lua ptr. Ensures
//--- that no two user threads has the same Lua VM.
Lua *get_lua_ptr() {
    LuaHolder *h = lua_ptr.get();
    if(h == nullptr)
      lua_ptr.reset(h = new LuaHolder());
    Lua *lua = h->held;
    if(lua == nullptr) {
        lua = new Lua();
    } else {
      h->held = nullptr;
    }
    return lua;
}

void set_lua_ptr(Lua *lua) {
  LuaHolder *h = lua_ptr.get();
  if(h == nullptr)
    lua_ptr.reset(h = new LuaHolder());
  Lua *l = h->held;
  if(l == nullptr) {
    h->held = lua;
  } else {
    delete lua;
  }
}

template<typename T>
void dtor(T *t) {
  t->T::~T();
}

//---future data structure---//

int new_future(lua_State *L) {
    size_t nbytes = sizeof(future_type); 
    char *mem = (char *)lua_newuserdata(L, nbytes);
    luaL_setmetatable(L,future_metatable_name);
    new (mem) future_type(); // initialize with placement new
    return 1;
}

int hpx_future_clean(lua_State *L) {
    if(luaL_checkudata(L,-1,future_metatable_name) != nullptr) {
      future_type *fnc = (future_type *)lua_touserdata(L,-1);
      dtor(fnc);
    }
    return 1;
}

int hpx_future_get(lua_State *L) {
  if(luaL_checkudata(L,-1,future_metatable_name) != nullptr) {
    future_type *fnc = (future_type *)lua_touserdata(L,-1);
    ptr_type result = fnc->get();
    for(auto i=result->begin();i!=result->end();++i) {
      i->unpack(L);
    }
  }
  return 1;
}

ptr_type luax_async2(
    string_ptr fname,
    ptr_type args);

int luax_wait_all(lua_State *L) {
  int nargs = lua_gettop(L);
  std::vector<future_type> v;
  for(int i=1;i<=nargs;i++) {
    if(luaL_checkudata(L,i,future_metatable_name) != nullptr) {
      future_type *fnc = (future_type *)lua_touserdata(L,1);
      v.push_back(*fnc);
    }
  }

  new_future(L);
  future_type *fc =
    (future_type *)lua_touserdata(L,-1);

  hpx::wait_all(v);

  return 1;
}

ptr_type get_when_any_result(hpx::when_any_result< std::vector< future_type > > result) {
  ptr_type p{new std::vector<Holder>()};
  Holder h;
  h.var = result.index;
  p->push_back(h);
  return p;
}

int luax_when_any(lua_State *L) {
  int nargs = lua_gettop(L);
  std::vector<future_type> v;
  for(int i=1;i<=nargs;i++) {
    if(lua_istable(L,i) && nargs==1) {
      int top = lua_gettop(L);
      lua_pushvalue(L,i);
      lua_pushnil(L);
      int n = 0;
      while(lua_next(L,-2)) {
        lua_pushvalue(L,-2);
        n++;
        const int ix = -2;
        if(luaL_checkudata(L,ix,future_metatable_name) == nullptr) {
          luai_writestringerror("Argument %d to when_any() is not a future ",n);
          return 0;
        }
        future_type *fnc = (future_type *)lua_touserdata(L,ix);
        v.push_back(*fnc);
        lua_pop(L,2);
      }
      if(lua_gettop(L) > top)
        lua_pop(L,lua_gettop(L)-top);
    } else if(luaL_checkudata(L,i,future_metatable_name) != nullptr) {
      future_type *fnc = (future_type *)lua_touserdata(L,i);
      v.push_back(*fnc);
    }
  }

  new_future(L);
  future_type *fc =
    (future_type *)lua_touserdata(L,-1);

  hpx::future< hpx::when_any_result< std::vector< future_type > > > result = hpx::when_any(v);
  *fc = result.then(hpx::util::unwrapped(boost::bind(get_when_any_result,_1)));

  return 1;
}

int hpx_future_then(lua_State *L) {
  if(luaL_checkudata(L,1,future_metatable_name) != nullptr) {
    future_type *fnc = (future_type *)lua_touserdata(L,1);
    
    CHECK_STRING(2,"Future:Then()")
    string_ptr fname(new string_wrap(lua_tostring(L,2)));

    // Package up the arguments
    ptr_type args(new std::vector<Holder>());
    int nargs = lua_gettop(L);
    for(int i=3;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(args);
    }

    new_future(L);
    future_type *fc =
      (future_type *)lua_touserdata(L,-1);
    *fc = fnc->then(boost::bind(luax_async2,fname,args));
  }
  return 1;
}

int open_future(lua_State *L) {
    static const struct luaL_Reg future_meta_funcs [] = {
        {"Get",&hpx_future_get},
        {"Then",&hpx_future_then},
        {NULL,NULL},
    };

    static const struct luaL_Reg future_funcs [] = {
        {"new", &new_future},
        {NULL, NULL}
    };

    luaL_newlib(L,future_funcs);

    luaL_newmetatable(L,future_metatable_name);
    luaL_newlib(L, future_meta_funcs);
    lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_future_clean);
    lua_settable(L,-3);
    lua_pop(L,1);

    return 1;
}

//---guard structure--//

int new_guard(lua_State *L) {
  size_t nbytes = sizeof(guard_type);
  char *guard = (char *)lua_newuserdata(L,nbytes);
  luaL_setmetatable(L,guard_metatable_name);
  new (guard) guard_type(new Guard());
  return 1;
}

int hpx_guard_clean(lua_State *L) {
    if(luaL_checkudata(L,-1,guard_metatable_name) != nullptr) {
      guard_type *fnc = (guard_type *)lua_touserdata(L,-1);
      dtor(fnc);
    }
    return 1;
}

int open_guard(lua_State *L) {
    static const struct luaL_Reg guard_meta_funcs [] = {
        {NULL,NULL},
    };

    static const struct luaL_Reg guard_funcs [] = {
        {"new", &new_guard},
        {NULL, NULL}
    };

    luaL_newlib(L,guard_funcs);

    luaL_newmetatable(L,guard_metatable_name);
    luaL_newlib(L, guard_meta_funcs);
    lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_guard_clean);
    lua_settable(L,-3);
    lua_pop(L,1);

    return 1;
}

//---locality structure--//

int new_locality(lua_State *L) {
  size_t nbytes = sizeof(locality_type);
  char *locality = (char *)lua_newuserdata(L,nbytes);
  luaL_setmetatable(L,locality_metatable_name);
  new (locality) locality_type();
  return 1;
}

int find_here(lua_State *L) {
  new_locality(L);
  locality_type *loc = (locality_type*)lua_touserdata(L,-1);
  *loc = hpx::find_here();
  return 1;
}

int root_locality(lua_State *L) {
  new_locality(L);
  locality_type *loc = (locality_type*)lua_touserdata(L,-1);
  *loc = hpx::find_root_locality();
  return 1;
}

int all_localities(lua_State *L) {
  std::vector<hpx::naming::id_type> all_localities = hpx::find_all_localities();
  lua_createtable(L,all_localities.size(),0); 
  int n = 1;
  for(auto i = all_localities.begin();i != all_localities.end();++i) {
    lua_pushnumber(L,n);
    new_locality(L);
    locality_type *loc = (locality_type*)lua_touserdata(L,-1);
    *loc = *i;
    lua_settable(L,-3);
    n++;
  }
  return 1;
}

int remote_localities(lua_State *L) {
  std::vector<hpx::naming::id_type> remote_localities = hpx::find_all_localities();
  lua_createtable(L,remote_localities.size(),0); 
  int n = 1;
  for(auto i = remote_localities.begin();i != remote_localities.end();++i) {
    lua_pushnumber(L,n);
    new_locality(L);
    locality_type *loc = (locality_type*)lua_touserdata(L,-1);
    *loc = *i;
    lua_settable(L,-3);
    n++;
  }
  return 1;
}

int loc_str(lua_State *L) {
  std::ostringstream msg;
  int n = lua_gettop(L);
  for(int i=1;i<=n;i++) {
    if(lua_isstring(L,i)) {
      msg << lua_tostring(L,i);
    } else if(lua_isuserdata(L,i) && luaL_checkudata(L,i,locality_metatable_name) != nullptr) {
      locality_type *loc = (locality_type*)lua_touserdata(L,i);
      msg << *loc;
    } else {
      return 0;
    }
  }
  lua_pop(L,n);
  lua_pushstring(L,msg.str().c_str());
  return 1;
}

int hpx_locality_clean(lua_State *L) {
    if(luaL_checkudata(L,-1,locality_metatable_name) != nullptr) {
      locality_type *fnc = (locality_type *)lua_touserdata(L,-1);
      dtor(fnc);
    }
    return 1;
}

int open_locality(lua_State *L) {
    static const struct luaL_Reg locality_meta_funcs [] = {
        {"str",&loc_str},
        {NULL,NULL},
    };

    static const struct luaL_Reg locality_funcs [] = {
        {"new", &new_locality},
        {"find_here", &find_here},
        {"find_all_localities", &all_localities},
        {"find_root_locality", &root_locality},
        {"find_remote_localities", &remote_localities},
        {NULL, NULL}
    };

    luaL_newlib(L,locality_funcs);

    luaL_newmetatable(L,locality_metatable_name);
    luaL_newlib(L, locality_meta_funcs);
    lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_locality_clean);
    lua_settable(L,-3);

    lua_pushstring(L,"__concat");
    lua_pushcfunction(L,loc_str);
    lua_settable(L,-3);

    lua_pop(L,1);

    return 1;
}

//--- Alternative implementation of when_all
#define WHEN_ALL hpx::when_all
//#define WHEN_ALL my_when_all

template<typename Future>
void my_when_all2(
    hpx::lcos::local::promise<std::vector<Future> > *pr,
    std::vector<Future>& futs,
    int index)
{
  const int n = futs.size();
  for(;index < n;index++) {
    if(!futs[index].is_ready()) {
      auto shared_state = hpx::lcos::detail::get_shared_state(futs[index]);
      auto f = hpx::util::bind(my_when_all2<Future>,pr,futs,index+1);
      shared_state->set_on_completed(f);
      return;
    }
  }
  pr->set_value(futs);
  delete pr;
}

template<typename Future>
hpx::future<std::vector<Future> > my_when_all(
    std::vector<Future>& futs)
{
  const int n = futs.size();
  for(int index=0;index < n;index++) {
    if(!futs[index].is_ready()) {
      auto p = new hpx::lcos::local::promise<std::vector<Future> >();
      auto fut = p->get_future();
      auto shared_state = hpx::lcos::detail::get_shared_state(futs[index]);
      auto f = hpx::util::bind(my_when_all2<Future>,p,futs,index+1);
      shared_state->set_on_completed(f);
      return fut;
    }
  }
  return make_ready_future(futs);
}

template<typename Future>
hpx::future<std::vector<Future> > my_when_all(
    std::vector<Future>&& futs_)
{
  std::vector<Future> futs;
  futs.swap(futs_);
  const int n = futs.size();
  for(int index=0;index < n;index++) {
    if(!futs[index].is_ready()) {
      auto p = new hpx::lcos::local::promise<std::vector<Future> >();
      auto fut = p->get_future();
      auto shared_state = hpx::lcos::detail::get_shared_state(futs[index]);
      auto f = hpx::util::bind(my_when_all2<Future>,p,futs,index+1);
      shared_state->set_on_completed(f);
      return fut;
    }
  }
  return make_ready_future(futs);
}

//--- Use these methods to process function inputs
boost::shared_ptr<std::vector<ptr_type> > realize_when_all_inputs_step2(ptr_type args,std::vector<future_type> results) {
  boost::shared_ptr<std::vector<ptr_type> > results_step2(new std::vector<ptr_type>());
  for(auto i=results.begin();i != results.end();++i) {
    results_step2->push_back(i->get());
  }
  return results_step2;
}

hpx::future<boost::shared_ptr<std::vector<ptr_type> > > realize_when_all_inputs(ptr_type args) {
  std::vector<future_type> futs;
  for(auto i=args->begin();i != args->end();++i) {
    int w = i->var.which();
    if(w == Holder::fut_t) {
      futs.push_back(boost::get<future_type>(i->var));
    }
  }
  hpx::future<std::vector<future_type> > result = WHEN_ALL(futs);
  return result.then(hpx::util::unwrapped(boost::bind(realize_when_all_inputs_step2,args,_1)));
}

//--- Use these methods to process function outputs
ptr_type realize_when_all_outputs_step2(ptr_type args,std::vector<future_type> results) {
  auto j = results.begin();
  for(auto i=args->begin();i != args->end();++i) {
    int w = i->var.which();
    if(w == Holder::fut_t) {
      i->var = j->get();
      j++;
    }
  }
  return args;
}

future_type realize_when_all_outputs(ptr_type args) {
  std::vector<future_type> futs;
  for(auto i=args->begin();i != args->end();++i) {
    int w = i->var.which();
    if(w == Holder::fut_t) {
      futs.push_back(boost::get<future_type>(i->var));
    }
  }
  hpx::future<std::vector<future_type> > result = WHEN_ALL(std::move(futs));
  return result.then(hpx::util::unwrapped(boost::bind(realize_when_all_outputs_step2,args,_1)));
}

//--- Handle dataflow calling from Lua
ptr_type luax_dataflow2(
    string_ptr fname,
    ptr_type args,
    boost::shared_ptr<std::vector<ptr_type> > futs) {

  ptr_type answers(new std::vector<Holder>());

  {
    LuaEnv lenv;

    lua_State *L = lenv.get_state();

    bool found = false;

    lua_getglobal(L,fname->c_str());
    if(lua_isfunction(L,-1)) {
      found = true;
    }

    if(!found) {
      if(function_registry.find(*fname) == function_registry.end()) {
        std::cout << "Function '" << *fname << "' is not defined." << std::endl;
        return answers;
      }

      std::string bytecode = function_registry[*fname];
      if(lua_load(L,(lua_Reader)lua_read,(void *)&bytecode,fname->c_str(),"b") != 0) {
        std::cout << "Error in function: '" << *fname << "' size=" << bytecode.size() << std::endl;
        SHOW_ERROR(L);
        return answers;
      }

      lua_setglobal(L,fname->c_str());
    }

    int n = lua_gettop(L);
    if(n > 0)
      lua_pop(L,n);

    // Push data from the concrete values and ready futures onto the Lua stack
    auto f = futs->begin();
    for(auto i=args->begin();i!=args->end();++i) {
      int w = i->var.which();
      if(w == Holder::fut_t) {
        for(auto j=(*f)->begin();j != (*f)->end();++j)
          j->unpack(L);
        f++;
      } else {
        i->unpack(L);
      }
    }

    lua_getglobal(L,fname->c_str());
    lua_insert(L,1);

    //std::ostringstream msg;
    //show_stack(msg,L,__LINE__);
    // Provide a maximum number output args
    const int max_output_args = 10;
    if(lua_pcall(L,args->size(),max_output_args,0) != 0) {
      //std::cout << msg.str();
      SHOW_ERROR(L);
      return answers;
    }

    // Trim stack
    int nargs = lua_gettop(L);
    while(nargs > 0 && lua_isnil(L,-1)) {
      lua_pop(L,1);
      nargs--;
    }

    for(int i=1;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(answers);
    }
    lua_pop(L,nargs);
  }

  return answers;
}

//--- Handle dataflow calling from Lua
ptr_type luax_async2(
    string_ptr fname,
    ptr_type args) {
  ptr_type answers(new std::vector<Holder>());

  {
    LuaEnv lenv;

    lua_State *L = lenv.get_state();

    bool found = false;

    lua_getglobal(L,fname->c_str());
    if(lua_isfunction(L,-1)) {
      found = true;
    }

    if(!found) {
      if(function_registry.find(*fname) == function_registry.end()) {
        std::cout << "Function '" << *fname << "' is not defined." << std::endl;
        return answers;
      }

      std::string bytecode = function_registry[*fname];
      if(lua_load(L,(lua_Reader)lua_read,(void *)&bytecode,fname->c_str(),"b") != 0) {
        std::cout << "Error in function: '" << *fname << "' size=" << bytecode.size() << std::endl;
        SHOW_ERROR(L);
        return answers;
      }

      lua_setglobal(L,fname->c_str());
    }

    int n = lua_gettop(L);
    if(n > 0)
      lua_pop(L,n);

    // Push data from the concrete values and ready futures onto the Lua stack
    for(auto i=args->begin();i!=args->end();++i) {
      i->unpack(L);
    }

    lua_getglobal(L,fname->c_str());
    lua_insert(L,1);

    //std::ostringstream msg;
    //show_stack(msg,L,__LINE__);
    // Provide a maximum number output args
    const int max_output_args = 10;
    if(lua_pcall(L,args->size(),max_output_args,0) != 0) {
      //std::cout << msg.str();
      SHOW_ERROR(L);
      return answers;
    }

    // Trim stack
    int nargs = lua_gettop(L);
    while(nargs > 0 && lua_isnil(L,-1)) {
      lua_pop(L,1);
      nargs--;
    }

    for(int i=1;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(answers);
    }
    lua_pop(L,nargs);
  }

  return answers;
}

//--- Realize futures in inputs, call dataflow function, realize futures in outputs
future_type luax_dataflow(
    string_ptr fname,
    ptr_type args) {
    // wait for all futures in input
    hpx::future<boost::shared_ptr<std::vector<ptr_type> > > f1 = realize_when_all_inputs(args);
    // pass values of all futures along with args
    future_type f2 = f1.then(hpx::util::unwrapped(boost::bind(luax_dataflow2,fname,args,_1)));
    // clean all futures out of returns
    return f2.then(hpx::util::unwrapped(boost::bind(realize_when_all_outputs,_1)));
}

int remote_reg(std::map<std::string,std::string> registry);

}

HPX_PLAIN_ACTION(hpx::luax_dataflow,luax_dataflow_action);
HPX_PLAIN_ACTION(hpx::luax_async2,luax_async_action);
HPX_PLAIN_ACTION(hpx::remote_reg,remote_reg_action);
HPX_REGISTER_BROADCAST_ACTION_DECLARATION(remote_reg_action);
HPX_REGISTER_BROADCAST_ACTION(remote_reg_action);

namespace hpx {

int luax_run_guarded(lua_State *L) {
  int n = lua_gettop(L);
  CHECK_STRING(-1,"run_guarded")
  string_ptr fname(new string_wrap(lua_tostring(L,-1)));
  guard_type g;
  if(n == 1) {
    g = global_guarded;
  } else if(n == 2) {
    g = *(guard_type *)lua_touserdata(L,-2);
  } else if(n > 2) {
    boost::shared_ptr<hpx::lcos::local::guard_set> gs{new hpx::lcos::local::guard_set()};
    ptr_type all_data{new std::vector<Holder>()};

    guard_type *gv = new guard_type[n];
    for(int i=1;i<n;i++) {
      guard_type g2 = *(guard_type *)lua_touserdata(L,i);
      gv[i-1]=g2;
      gs->add(g2->g);
      Holder h;
      h.var = g2->g_data;
      all_data->push_back(h);
    }
    boost::function<void()> func = boost::bind(hpx_srun,fname,all_data,gv,n);
    run_guarded(*gs,func);
    return 1;
  }
  lua_pop(L,n);
  guard_type *gv = new guard_type[1];
  gv[0] = g;
  boost::function<void()> func = boost::bind(hpx_srun,fname,g->g_data,gv,1);
  run_guarded(*g->g,func);
  return 1;
}

int isfuture(lua_State *L) {
    if(lua_isuserdata(L,-1) && luaL_checkudata(L,-1,future_metatable_name) != nullptr) {
      lua_pop(L,1);
      lua_pushnumber(L,1);
    } else {
      lua_pop(L,1);
      lua_pushnumber(L,0);
    }
    return 1;
}

int dataflow(lua_State *L) {

    locality_type *loc = nullptr;
    if(lua_isuserdata(L,1) && luaL_checkudata(L,1,locality_metatable_name) != nullptr) {
      loc = (locality_type *)lua_touserdata(L,1);
      lua_remove(L,1);
    }

    // Package up the arguments
    ptr_type args(new std::vector<Holder>());
    int nargs = lua_gettop(L);
    for(int i=2;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(args);
    }
    
    CHECK_STRING(1,"dataflow")
    string_ptr fname(new string_wrap(lua_tostring(L,1)));

    // Launch the thread
    future_type f =
      (loc == nullptr) ?
        hpx::async(luax_dataflow,fname,args) :
        hpx::async<luax_dataflow_action>(*loc,fname,args);

    new_future(L);
    future_type *fc =
      (future_type *)lua_touserdata(L,-1);
    *fc = f;
    return 1;
}

int async(lua_State *L) {

    locality_type *loc = nullptr;
    if(lua_isuserdata(L,1) && luaL_checkudata(L,1,locality_metatable_name) != nullptr) {
      loc = (locality_type *)lua_touserdata(L,1);
      lua_remove(L,1);
    }

    // Package up the arguments
    ptr_type args(new std::vector<Holder>());
    int nargs = lua_gettop(L);
    for(int i=2;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(args);
    }
    
    CHECK_STRING(1,"async")
    string_ptr fname(new string_wrap(lua_tostring(L,1)));

    // Launch the thread
    future_type f =
      (loc == nullptr) ?
        hpx::async(luax_async2,fname,args) :
        hpx::async<luax_async_action>(*loc,fname,args);

    new_future(L);
    future_type *fc =
      (future_type *)lua_touserdata(L,-1);
    *fc = f;
    return 1;
}

int unwrap(lua_State *L) {

    // Package up the arguments
    ptr_type args(new std::vector<Holder>());
    int nargs = lua_gettop(L);
    for(int i=2;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(args);
    }
    
    CHECK_STRING(1,"unwrap")
    string_ptr fname(new string_wrap(lua_tostring(L,1)));

    future_type f =
      luax_dataflow(fname,args);

    new_future(L);
    future_type *fc =
      (future_type *)lua_touserdata(L,-1);
    *fc = f;
    return 1;
}

int remote_reg(std::map<std::string,std::string> registry) {
	LuaEnv lenv;
    lua_State *L = lenv.get_state();
	function_registry = registry;
	for(auto i = registry.begin();i != registry.end();++i) {
		std::string& bytecode = i->second;
		if(lua_load(L,(lua_Reader)lua_read,(void *)&bytecode,i->first.c_str(),"b") != 0) {
			std::cout << "Error in function: " << i->first << " size=" << bytecode.size() << std::endl;
			SHOW_ERROR(L);
			return -1;
		}

		lua_setglobal(L,i->first.c_str());
	}
	return 0;
}

// TODO: add wrappers to conveniently get and use tables?

// TODO: Extend to include loading of libraries from .so files and running scripts.
// Example: hpx_reg('init.lua','power','pr()')
// The first is a script, the second a lib (named either power.so or libpower.so),
// the third a function. 
int hpx_reg(lua_State *L) {
	while(lua_gettop(L)>0) {
    CHECK_STRING(-1,"HPX_PLAIN_ACTION")
		if(lua_isstring(L,-1)) {
			const int n = lua_gettop(L);
			std::string fname = lua_tostring(L,-1);
			lua_getglobal(L,fname.c_str());
			luaL_Buffer buf;
			luaL_buffinit(L,&buf);
			lua_dump(L,(lua_Writer)lua_write,&buf);
			luaL_pushresult(&buf);
			const char *data = lua_tostring(L,-1);
			int len = lua_rawlen(L,-1);
			std::string bytecode(data,len);
			function_registry[fname]=bytecode;
			//std::cout << "register(" << fname << "):size=" << len << std::endl;
			const int nf = lua_gettop(L);
			if(nf > n) {
				lua_pop(L,nf-n);
			}
		}
		lua_pop(L,1);
	}

	std::vector<hpx::naming::id_type> remote_localities = hpx::find_remote_localities();
  if(remote_localities.size() > 0) {
    auto f = hpx::lcos::broadcast<remote_reg_action>(remote_localities,function_registry);
    f.get(); // in case there are exceptions
  }
  
	return 1;
}

// TODO: Get rid of this constant.
const char *alt_name = "xrun";

void hpx_srun(string_ptr fname,ptr_type gdata,guard_type *gv,int ng) {
    LuaEnv lenv;
    lua_State *L = lenv.get_state();
    int n = lua_gettop(L);
    lua_pop(L,n);
    for(auto i=gdata->begin();i!=gdata->end();++i) {
      i->unpack(L);
    }
    hpx_srun(L,fname->str,gdata);
    gdata->clear();

    // Trim stack
    int nargs = lua_gettop(L);
    while(nargs > 0 && lua_isnil(L,-1)) {
      lua_pop(L,1);
      nargs--;
    }

    for(int i=1;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      guard_type g;
      int n = i <= ng ? i-1 : ng-1;
      g = gv[n];
      g->g_data->clear();
      h.push(g->g_data);
    }
    delete[] gv;
}

int hpx_run(lua_State *L) {
  CHECK_STRING(1,"hpx_run")
  std::string fname = lua_tostring(L,1);
  lua_remove(L,1);
  return hpx_srun(L,fname,global_guarded->g_data);
}

int hpx_srun(lua_State *L,std::string& fname,ptr_type gdata) {
  int n = lua_gettop(L);
  if(function_registry.find(fname) == function_registry.end()) {
    std::cout << "Function '" << fname << "' is not defined." << std::endl;
    return 0;
  }

  std::string bytecode = function_registry[fname];
  if(lua_load(L,(lua_Reader)lua_read,(void *)&bytecode,alt_name,"b") != 0) {
    std::cout << "Error in function: " << fname << " size=" << bytecode.size() << std::endl;
    SHOW_ERROR(L);
    return 0;
  }

  lua_setglobal(L,alt_name);
  lua_getglobal(L,alt_name);

  if(!lua_isfunction(L,-1)) {
    std::cout << "Failed to load byte code for " << fname << std::endl;
    return 0;
  }

  lua_insert(L,1);
  if(lua_pcall(L,n,10,0) != 0) {
    SHOW_ERROR(L);
    return 0;
  }
  return 1;
}

}
