/* -------------------------------------------------------------------------
 *
 * s_lock.cpp
 *	   Hardware-dependent implementation of spinlocks.
 *
 * When waiting for a contended spinlock we loop tightly for awhile, then
 * delay using pg_usleep() and try again.  Preferably, "awhile" should be a
 * small multiple of the maximum time we expect a spinlock to be held.  100
 * iterations seems about right as an initial guess.  However, on a
 * uniprocessor the loop is a waste of cycles, while in a multi-CPU scenario
 * it's usually better to spin a bit longer than to call the kernel, so we try
 * to adapt the spin loop count depending on whether we seem to be in a
 * uniprocessor or multiprocessor.
 *
 * Note: you might think MIN_SPINS_PER_DELAY should be just 1, but you'd
 * be wrong; there are platforms where that can result in a "stuck
 * spinlock" failure.  This has been seen particularly on Alphas; it seems
 * that the first TAS after returning from kernel space will always fail
 * on that hardware.
 *
 * Once we do decide to block, we use randomly increasing pg_usleep()
 * delays. The first delay is 1 msec, then the delay randomly increases to
 * about one second, after which we reset to 1 msec and start again.  The
 * idea here is that in the presence of heavy contention we need to
 * increase the delay, else the spinlock holder may never get to run and
 * release the lock.  (Consider situation where spinlock holder has been
 * nice'd down in priority by the scheduler --- it will not get scheduled
 * until all would-be acquirers are sleeping, so if we always use a 1-msec
 * sleep, there is a real possibility of starvation.)  But we can't just
 * clamp the delay to an upper bound, else it would take a long time to
 * make a reasonable number of tries.
 *
 * We time out and declare error after NUM_DELAYS delays (thus, exactly
 * that many tries).  With the given settings, this will usually take 2 or
 * so minutes.  It seems better to fix the total number of tries (and thus
 * the probability of unintended failure) than to fix the total time
 * spent.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/lmgr/s_lock.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <time.h>
#include <unistd.h>

#include "storage/lock/s_lock.h"
#include "utils/atomic.h"

#define MIN_SPINS_PER_DELAY 10
#define MAX_SPINS_PER_DELAY 1000
#define NUM_DELAYS 1000
#define MIN_DELAY_USEC 1000L
#define MAX_DELAY_USEC 1000000L

#ifdef __aarch64__
    int (*arm_tas_spin)(volatile slock_t *lock);
#endif

/*
 * s_lock_stuck() - complain about a stuck spinlock
 */
static void s_lock_stuck(void* p, const char* file, int line)
{
#if defined(S_LOCK_TEST)
    fprintf(stderr, "\nStuck spinlock detected at %s:%d.\n", file, line);
    gs_thread_exit(1);
#else
    ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("stuck spinlock detected at %s:%d", file, line)));
#endif
}

/*
 * s_lock(lock) - platform-independent portion of waiting for a spinlock.
 */
int s_lock(volatile slock_t* lock, const char* file, int line)
{
    SpinDelayStatus delayStatus = init_spin_delay((void*)lock);

    while (TAS_SPIN(lock)) {
        perform_spin_delay(&delayStatus);
    }

    finish_spin_delay(&delayStatus);

    return delayStatus.delays;
}

#ifdef USE_DEFAULT_S_UNLOCK
void s_unlock(volatile slock_t* lock)
{
#ifdef TAS_ACTIVE_WORD
    /* HP's PA-RISC */
    *TAS_ACTIVE_WORD(lock) = -1;
#else
    *lock = 0;
#endif
}
#endif

/*
 * Wait while spinning on a contended spinlock.
 */
void perform_spin_delay(SpinDelayStatus* status)
{
    /* CPU-specific delay each time through the loop */
    SPIN_DELAY();

    /* Block the process every spins_per_delay tries */
    if (++(status->spins) >= t_thrd.storage_cxt.spins_per_delay) {
        if (++(status->delays) > NUM_DELAYS)
            s_lock_stuck(status->ptr, status->file, status->line);

        if (status->cur_delay == 0) /* first time to delay? */
            status->cur_delay = MIN_DELAY_USEC;

        pg_usleep(status->cur_delay);

#if defined(S_LOCK_TEST)
        fprintf(stdout, "*");
        fflush(stdout);
#endif

        /* increase delay by a random fraction between 1X and 2X */
        status->cur_delay += (int)(status->cur_delay * ((double)random() / (double)MAX_RANDOM_VALUE) + 0.5);
        /* wrap back to minimum delay when max is exceeded */
        if (status->cur_delay > MAX_DELAY_USEC)
            status->cur_delay = MIN_DELAY_USEC;

        status->spins = 0;
    }
}

/*
 * After acquiring a spinlock, update estimates about how long to loop.
 *
 * If we were able to acquire the lock without delaying, it's a good
 * indication we are in a multiprocessor.  If we had to delay, it's a sign
 * (but not a sure thing) that we are in a uniprocessor. Hence, we
 * decrement spins_per_delay slowly when we had to delay, and increase it
 * rapidly when we didn't.  It's expected that spins_per_delay will
 * converge to the minimum value on a uniprocessor and to the maximum
 * value on a multiprocessor.
 *
 * Note: spins_per_delay is local within our current process. We want to
 * average these observations across multiple backends, since it's
 * relatively rare for this function to even get entered, and so a single
 * backend might not live long enough to converge on a good value.  That
 * is handled by the two routines below.
 */
void finish_spin_delay(SpinDelayStatus* status)
{
    if (status->cur_delay == 0) {
        /* we never had to delay */
        if (t_thrd.storage_cxt.spins_per_delay < MAX_SPINS_PER_DELAY)
            t_thrd.storage_cxt.spins_per_delay = Min(t_thrd.storage_cxt.spins_per_delay + 100, MAX_SPINS_PER_DELAY);
    } else {
        if (t_thrd.storage_cxt.spins_per_delay > MIN_SPINS_PER_DELAY)
            t_thrd.storage_cxt.spins_per_delay = Max(t_thrd.storage_cxt.spins_per_delay - 1, MIN_SPINS_PER_DELAY);
    }
}

/*
 * Set local copy of spins_per_delay during backend startup.
 *
 * NB: this has to be pretty fast as it is called while holding a spinlock
 */
void set_spins_per_delay(int shared_spins_per_delay)
{
    t_thrd.storage_cxt.spins_per_delay = shared_spins_per_delay;
}

/*
 * Update shared estimate of spins_per_delay during backend exit.
 *
 * NB: this has to be pretty fast as it is called while holding a spinlock
 */
int update_spins_per_delay(int shared_spins_per_delay)
{
    /*
     * We use an exponential moving average with a relatively slow adaption
     * rate, so that noise in any one backend's result won't affect the shared
     * value too much.	As long as both inputs are within the allowed range,
     * the result must be too, so we need not worry about clamping the result.
     *
     * We deliberately truncate rather than rounding; this is so that single
     * adjustments inside a backend can affect the shared estimate (see the
     * asymmetric adjustment rules above).
     */
    return (shared_spins_per_delay * 15 + t_thrd.storage_cxt.spins_per_delay) / 16;
}

/*
 * Various TAS implementations that cannot live in s_lock.h as no inline
 * definition exists (yet).
 * In the future, get rid of tas.[cso] and fold it into this file.
 *
 * If you change something here, you will likely need to modify s_lock.h too,
 * because the definitions for these are split between this file and s_lock.h.
 */
#ifdef HAVE_SPINLOCKS /* skip spinlocks if requested */

#if defined(__GNUC__)

/*
 * All the gcc flavors that are not inlined
 *
 *
 * Note: all the if-tests here probably ought to be testing gcc version
 * rather than platform, but I don't have adequate info to know what to
 * write.  Ideally we'd flush all this in favor of the inline version.
 */
#if defined(__m68k__) && !defined(__linux__)
static void tas_dummy()
{
    __asm__ __volatile__(
#if defined(__NetBSD__) && defined(__ELF__)
        /* no underscore for label and % for registers */
        "\
.global		tas 				\n\
tas:							\n\
			movel	%sp@(0x4),%a0	\n\
			tas 	%a0@		\n\
			beq 	_success	\n\
			moveq	#-128,%d0	\n\
			rts 				\n\
_success:						\n\
			moveq	#0,%d0		\n\
			rts 				\n"
#else
        "\
.global		_tas				\n\
_tas:							\n\
			movel	sp@(0x4),a0	\n\
			tas 	a0@			\n\
			beq 	_success	\n\
			moveq 	#-128,d0	\n\
			rts					\n\
_success:						\n\
			moveq 	#0,d0		\n\
			rts					\n"
#endif /* __NetBSD__ && __ELF__ */
    );
}
#endif /* __m68k__ && !__linux__ */
#else  /* not __GNUC__ */

/*
 * All non gcc
 */
#if defined(sun3)
static void tas_dummy() /* really means: extern int tas(slock_t *lock); */
{
    asm("LLA0:");
    asm("   .data");
    asm("   .text");
    asm("|#PROC# 04");
    asm("   .globl  _tas");
    asm("_tas:");
    asm("|#PROLOGUE# 1");
    asm("   movel   sp@(0x4),a0");
    asm("   tas a0@");
    asm("   beq LLA1");
    asm("   moveq   #-128,d0");
    asm("   rts");
    asm("LLA1:");
    asm("   moveq   #0,d0");
    asm("   rts");
    asm("   .data");
}
#endif /* sun3 */
#endif /* not __GNUC__ */
#endif /* HAVE_SPINLOCKS */

#if defined(S_LOCK_TEST)

/*
 * test program for verifying a port's spinlock support.
 */
struct test_lock_struct {
    char pad1;
    slock_t lock;
    char pad2;
};

volatile static struct test_lock_struct g_test_lock;

int main()
{
    srandom((unsigned int)time(NULL));

    g_test_lock.pad1 = g_test_lock.pad2 = 0x44;

    S_INIT_LOCK(&g_test_lock.lock);

    if (g_test_lock.pad1 != 0x44 || g_test_lock.pad2 != 0x44) {
        printf("S_LOCK_TEST: failed, declared datatype is wrong size\n");
        return 1;
    }

    if (!S_LOCK_FREE(&g_test_lock.lock)) {
        printf("S_LOCK_TEST: failed, lock not initialized\n");
        return 1;
    }

    S_LOCK(&g_test_lock.lock);

    if (g_test_lock.pad1 != 0x44 || g_test_lock.pad2 != 0x44) {
        printf("S_LOCK_TEST: failed, declared datatype is wrong size\n");
        return 1;
    }

    if (S_LOCK_FREE(&g_test_lock.lock)) {
        printf("S_LOCK_TEST: failed, lock not locked\n");
        return 1;
    }

    S_UNLOCK(&g_test_lock.lock);

    if (g_test_lock.pad1 != 0x44 || g_test_lock.pad2 != 0x44) {
        printf("S_LOCK_TEST: failed, declared datatype is wrong size\n");
        return 1;
    }

    if (!S_LOCK_FREE(&g_test_lock.lock)) {
        printf("S_LOCK_TEST: failed, lock not unlocked\n");
        return 1;
    }

    S_LOCK(&g_test_lock.lock);

    if (g_test_lock.pad1 != 0x44 || g_test_lock.pad2 != 0x44) {
        printf("S_LOCK_TEST: failed, declared datatype is wrong size\n");
        return 1;
    }

    if (S_LOCK_FREE(&g_test_lock.lock)) {
        printf("S_LOCK_TEST: failed, lock not re-locked\n");
        return 1;
    }

    printf("S_LOCK_TEST: this will print %d stars and then\n", NUM_DELAYS);
    printf("             exit with a 'stuck spinlock' message\n");
    printf("             if S_LOCK() and TAS() are working.\n");
    fflush(stdout);

    s_lock(&g_test_lock.lock, __FILE__, __LINE__);

    printf("S_LOCK_TEST: failed, lock not locked\n");
    return 1;
}

#endif /* S_LOCK_TEST */
