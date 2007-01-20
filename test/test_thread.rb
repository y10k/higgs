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
      timeout(DELTA_T) { ThreadsWait.all_waits(*th_grp.list) }
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
      timeout(DELTA_T) { ThreadsWait.all_waits(*th_grp.list) }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
    end
  end

  class SharedWorkTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Timeout

    NUM_OF_THREADS = 10
    DELTA_T = 0.1
    WORK_COUNT = 100

    def frac(n)
      (n == 0) ? 1 : n * frac(n - 1)
    end
    private :frac

    def test_frac
      assert_equal(1, frac(0))
      assert_equal(1, frac(1))
      assert_equal(1 * 2, frac(2))
      assert_equal(1 * 2 * 3, frac(3))
      assert_equal(1 * 2 * 3 * 4, frac(4))
      assert_equal(1 * 2 * 3 * 4 * 5, frac(5))
      assert_equal(1 * 2 * 3 * 4 * 5 * 6, frac(6))
      assert_equal(1 * 2 * 3 * 4 * 5 * 6 * 7, frac(7))
      assert_equal(1 * 2 * 3 * 4 * 5 * 6 * 7 * 8, frac(8))
      assert_equal(1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9, frac(9))
      assert_equal(1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10, frac(10))
    end

    def test_result
      latch = Tank::Latch.new
      expected_result = frac(WORK_COUNT)
      work = Tank::SharedWork.new{
        latch.wait
        frac(WORK_COUNT)
      }

      lock = Mutex.new
      count = 0
      th_grp = ThreadGroup.new
      NUM_OF_THREADS.times do
        th_grp.add Thread.new{
          assert_equal(expected_result, work.result)
          lock.synchronize{ count += 1 }
        }
      end

      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      latch.start
      timeout(DELTA_T) { ThreadsWait.all_waits(*th_grp.list) }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
      assert_equal(expected_result, work.result)
    end
  end
end
