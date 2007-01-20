#!/usr/local/bin/ruby

$: << File.join(File.dirname($0), '..', 'lib')

require 'rubyunit'
require 'tank/thread'
require 'thwait'
require 'timeout'

module Tank::Test
  class LatchTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Timeout

    NUM_OF_THREADS = 10
    DELTA_T = 0.1

    def test_start_wait
      latch = Tank::Latch.new

      lock = Mutex.new
      count = 0
      th_grp = ThreadGroup.new
      NUM_OF_THREADS.times do
        th_grp.add Thread.new{
          latch.wait
          lock.synchronize{ count += 1 }
        }
      end

      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      latch.start
      timeout(10) { ThreadsWait.all_waits(*th_grp.list) }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
    end
  end

  class BarrierTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Timeout

    NUM_OF_THREADS = 10
    DELTA_T = 0.1

    def test_wait
      barrier = Tank::Barrier.new(NUM_OF_THREADS)

      lock = Mutex.new
      count = 0
      th_new = proc{
        Thread.new{
          barrier.wait
          lock.synchronize{ count += 1 }
        }
      }

      th_grp = ThreadGroup.new
      (NUM_OF_THREADS - 1).times do
        th_grp.add(th_new.call)
      end

      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      th_grp.add(th_new.call)
      timeout(10) { ThreadsWait.all_waits(*th_grp.list) }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
    end
  end

  class SharedWorkTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Timeout

    NUM_OF_THREADS = 10
    WORK_COUNT = 1000

    def calc
      @s = 0                    # @s's scope is over multi-threading
      for i in 1..WORK_COUNT
        @s += i
      end
      @s
    end

    def test_calc_single_thread
      a = calc
      b = calc
      assert_equal(a, b)
    end

    def test_calc_race_condition
      barrier = Tank::Barrier.new(3)

      a = nil
      th1 = Thread.new{
        barrier.wait
        a = calc
      }

      b = nil
      th2 = Thread.new{
        barrier.wait
        b = calc
      }

      barrier.wait
      ThreadsWait.all_waits(th1, th2)
      assert(a != b)
    end

    def test_result
      expected_result = calc

      latch = Tank::Latch.new
      work = Tank::SharedWork.new{
        latch.wait
        calc
      }

      barrier = Tank::Barrier.new(NUM_OF_THREADS + 1)
      lock = Mutex.new
      count = 0

      th_grp = ThreadGroup.new
      NUM_OF_THREADS.times do
        th_grp.add Thread.new{
          barrier.wait
          assert_equal(expected_result, work.result)
          lock.synchronize{ count += 1 }
        }
      end

      barrier.wait
      assert_equal(0, lock.synchronize{ count })

      latch.start
      timeout(10) { ThreadsWait.all_waits(*th_grp.list) }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
      assert_equal(expected_result, work.result)
    end
  end
end
