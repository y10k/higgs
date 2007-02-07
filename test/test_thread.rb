#!/usr/local/bin/ruby

require 'higgs/thread'
require 'rubyunit'
require 'timeout'

module Higgs::ThreadTest
  # for ident(1)
  CVS_ID = '$Id$'

  class LatchTest < RUNIT::TestCase
    include Higgs::Thread
    include Timeout

    NUM_OF_THREADS = 10
    DELTA_T = 0.1

    def test_start_wait
      latch = Latch.new

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
      timeout(10) {
        for t in th_grp.list
          t.join
        end
      }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
    end
  end

  class BarrierTest < RUNIT::TestCase
    include Higgs::Thread
    include Timeout

    NUM_OF_THREADS = 10
    DELTA_T = 0.1

    def test_wait
      barrier = Barrier.new(NUM_OF_THREADS)

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
      timeout(10) {
        for t in th_grp.list
          t.join
        end
      }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
    end

    def test_not_recycle
      barrier = Barrier.new(1)
      barrier.wait
      assert_exception(RuntimeError) { barrier.wait }
    end
  end

  class SharedWorkTest < RUNIT::TestCase
    include Higgs::Thread
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
      a = nil
      b = nil
      begin
        barrier = Barrier.new(3)

        th1 = Thread.new{
          barrier.wait
          a = calc
        }

        th2 = Thread.new{
          barrier.wait
          b = calc
        }

        barrier.wait
        th1.join
        th2.join
      end until (a != b)        # race condition
    end

    def test_result
      expected_result = calc

      latch = Latch.new
      work = SharedWork.new{
        latch.wait
        calc
      }

      barrier = Barrier.new(NUM_OF_THREADS + 1)
      lock = Mutex.new
      count = 0

      th_grp = ThreadGroup.new
      NUM_OF_THREADS.times{|i|  # `i' should be local scope of thread block
        th_grp.add Thread.new{
          barrier.wait
          assert_equal(expected_result, work.result, "th#{i}")
          lock.synchronize{ count += 1 }
        }
      }

      barrier.wait
      assert_equal(0, lock.synchronize{ count })

      latch.start
      timeout(10) {
        for t in th_grp.list
          t.join
        end
      }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
      assert_equal(expected_result, work.result)
    end

    def test_no_work_block
      assert_exception(RuntimeError) { SharedWork.new }
    end
  end
end
