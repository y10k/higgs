#!/usr/local/bin/ruby

require 'higgs/thread'
require 'test/unit'
require 'timeout'

Thread.abort_on_exception = true if $DEBUG

module Higgs::Test
  class LatchTest < Test::Unit::TestCase
    include Higgs
    include Timeout

    # for ident(1)
    CVS_ID = '$Id$'

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

  class CountDownLatchTest < Test::Unit::TestCase
    include Higgs
    include Timeout

    # for ident(1)
    CVS_ID = '$Id$'

    NUM_OF_THREADS = 10
    DELTA_T = 0.1

    def test_count_down_wait
      latch = CountDownLatch.new(3)

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

      latch.count_down
      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      latch.count_down
      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      latch.count_down
      timeout(10) {
        for t in th_grp.list
          t.join
        end
      }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
    end
  end

  class BarrierTest < Test::Unit::TestCase
    include Higgs
    include Timeout

    # for ident(1)
    CVS_ID = '$Id$'

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
      assert_raise(RuntimeError) { barrier.wait }
    end
  end

  class SharedWorkTest < Test::Unit::TestCase
    include Higgs
    include Timeout

    # for ident(1)
    CVS_ID = '$Id$'

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
      assert_raise(RuntimeError) { SharedWork.new }
    end
  end

  class ReadWriteLockTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    WORK_COUNT = 100
    THREAD_COUNT = 10

    def setup
      @rw_lock = ReadWriteLock.new
    end

    def test_read_lock_single_thread
      v = "foo"
      r_lock = @rw_lock.read_lock
      WORK_COUNT.times do
        assert_equal("foo", r_lock.synchronize{ v })
      end
    end

    def test_write_lock_single_thread
      count = 0
      w_lock = @rw_lock.write_lock
      WORK_COUNT.times do
        w_lock.synchronize{
          count += 1
        }
      end
      assert_equal(WORK_COUNT, count)
    end

    def test_read_lock_multithread
      v = "foo"
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times{|i| # `i' should be local scope of thread block
        th_grp.add Thread.new{
          r_lock = @rw_lock.read_lock
          r_lock.synchronize{
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal("foo", v, "read_lock: #{i}.#{j}")
            end
          }
        }
      }

      barrier.wait
      for t in th_grp.list
        t.join
      end
    end

    def test_write_lock_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times do
        th_grp.add Thread.new{
          w_lock = @rw_lock.write_lock
          barrier.wait
          WORK_COUNT.times do
            w_lock.synchronize{
              count += 1
            }
          end
        }
      end

      barrier.wait
      for t in th_grp.list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end

    def test_read_write_lock_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT * 2 + 1)

      THREAD_COUNT.times{|i| # `i' should be local scope of thread block
        th_grp.add Thread.new{
          r_lock = @rw_lock.read_lock
          r_lock.synchronize{
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal(0, count, "read_lock: #{i}.#{j}")
            end
          }
        }
      }

      THREAD_COUNT.times do
        th_grp.add Thread.new{
          w_lock = @rw_lock.write_lock
          barrier.wait
          WORK_COUNT.times do
            w_lock.synchronize{
              count += 1
            }
          end
        }
      end

      barrier.wait
      for t in th_grp.list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end

    def test_write_read_lock_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT + 2)

      th_grp.add Thread.new{
        w_lock = @rw_lock.write_lock
        w_lock.synchronize{
          barrier.wait
          THREAD_COUNT.times do
            WORK_COUNT.times do
              count += 1
            end
          end
        }
      }

      THREAD_COUNT.times{|i| # `i' should be local scope of thread block
        th_grp.add Thread.new{
          r_lock = @rw_lock.read_lock
          barrier.wait
          r_lock.synchronize{
            assert_equal(THREAD_COUNT * WORK_COUNT, count, "read_lock: #{i}")
          }
        }
      }

      barrier.wait
      for t in th_grp.list
        t.join
      end
    end
  end

  class PoolTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    class Counter
      def initialize
        @value = 0
      end

      attr_reader :value

      def count
        @value += 1
      end
    end

    WORK_COUNT = 100
    THREAD_COUNT = 10

    def setup
      @pool = Pool.new(2) { Counter.new }
    end

    def test_transaction
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times{|i| # `i' should be local scope of thread block
        th_grp.add Thread.new{
          barrier.wait
          WORK_COUNT.times do |j|
            @pool.transaction{|c|
              v = c.value
              c.count
              assert_equal(v + 1, c.value, "thread: #{i}.#{j}")
            }
          end
        }
      }

      barrier.wait
      for t in th_grp.list
        t.join
      end

      n = 0
      s = 0
      @pool.shutdown{|c|
        n += 1
        s += c.value
      }
      assert_equal(@pool.size, n)
      assert_equal(WORK_COUNT * THREAD_COUNT, s)
    end

    def test_shutdown
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT + 1)
      latch = CountDownLatch.new(THREAD_COUNT)

      THREAD_COUNT.times{|i| # `i' should be local scope of thread block
        th_grp.add Thread.new{
          barrier.wait
          assert_raise(Pool::ShutdownException) {
            j = 0
            loop do
              @pool.transaction{|c|
                v = c.value
                c.count
                assert_equal(v + 1, c.value, "thread: #{i}.#{j}")
                latch.count_down if (j > WORK_COUNT)
              }
              j += 1
            end
          }
        }
      }

      barrier.wait
      latch.wait
      @pool.shutdown
      for t in th_grp.list
        t.join
      end
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
