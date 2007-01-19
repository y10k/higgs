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
      th_grp = ThreadGroup.new
      th_new = proc{
        Thread.new{
          barrier.wait
          lock.synchronize{ count += 1 }
        }
      }
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

    def test_result
      latch = Tank::Latch.new
      work = Tank::SharedWork.new{
        latch.wait
        (1..100).inject(0) {|s, i| s + i }
      }

      lock = Mutex.new
      count = 0
      th_num = 10
      th_grp = ThreadGroup.new
      NUM_OF_THREADS.times do
        th_grp.add Thread.new{
          assert_equal(5050, work.result)
          lock.synchronize{ count += 1 }
        }
      end

      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })
      latch.start
      timeout(DELTA_T) { ThreadsWait.all_waits(*th_grp.list) }
      assert_equal(NUM_OF_THREADS, lock.synchronize{ count })
      assert_equal(5050, work.result)
    end
  end
end
