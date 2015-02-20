What this builds:

libxlua.a - Use this to link your application for running Lua in your HPX program.
xlua - This is a command line interpreter, suitable for running the scripts in the example_scripts dir.
hello - This is an example that shows you how to call lua from inside a C++ program.

How it works:

XLua maintains a lua virtual machine (LVM) for each hardware thread. Whenever an HPX program makes
use of one of these threads, either by instantiating a LuaEnv object or by running a method call
on it through XLua's async() method, it takes control of that LVM.

Normally, the LuaEnv destructor releases possession of the LVM when the destructor is called.
If two HPX user threads try to use the same hardware thread for a Lua operation, a new LVM
will be allocated and the old one (when it's finished with its task) will be freed.

Because calls to future:get() block, there is an increased likelyhood that a new LVM will have
to be allocated whenever it is called. Therefore, futurized code is recommended.

In addition, because LVM's come and go, and because you never know which one you'll be running
on, you should avoid storing information in global variables as each of them will have their
own global data. The exception to this rule is the set of functions you supply to hpx_reg(). They
will be available on all LVM's.
