
// main.c

#include "handler.c"

int main(int argc, char * argv[])
{
    init_or_die(argc, argv);
    run_events_loop(0); // main thread_id=0
    return -1;
}
